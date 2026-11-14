package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file observe acknowledgements the way PostgreSQL sees them:
// through the slot's confirmed_flush_lsn. Each one uses its own slot,
// publication, tables and indices and drops them afterwards, so no slot keeps
// retaining WAL once its test is over.

const (
	// ackWaitTimeout bounds every wait on the slot or on Elasticsearch.
	ackWaitTimeout  = 20 * time.Second
	ackPollInterval = 100 * time.Millisecond
)

func TestConnector_AckMultiMessageFlush(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_flush"
		publicationName = "cdc_publication_ack_flush"
		table           = "ack_flush_items"
		index           = "ack.flush.items"
		rows            = 6
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index}, table)

	// With a one hour ticker and a batch limit equal to the row count, the
	// single-row transactions share exactly one flush, triggered by the last row.
	cfg := ackTestConfig(slotName, publicationName, []string{table}, map[string]string{"public." + table: index}, time.Hour, rows)
	startConnector(t, cfg, handler)

	for i := 1; i < rows; i++ {
		insertRow(t, db, table, fmt.Sprintf("row-%d", i))
	}
	lastCommit := commitWithLSN(t, db, func(tx *sql.Tx) {
		_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, table), fmt.Sprintf("row-%d", rows))
		require.NoError(t, err)
	})

	requireDocumentCount(t, esClient, index, rows)
	requireConfirmedReaches(t, db, slotName, lastCommit)
}

// TestConnector_AckSlotAdvancesWhenIdle reproduces the incident shape: once the
// published table goes quiet, WAL written elsewhere must not stay pinned by the
// slot. The connector receives nothing to ack, so only the core's keepalive
// confirmation can move the slot, and it does so only when every delivered
// message has been acked.
func TestConnector_AckSlotAdvancesWhenIdle(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_idle"
		publicationName = "cdc_publication_ack_idle"
		table           = "ack_idle_items"
		noiseTable      = "ack_idle_noise"
		index           = "ack.idle.items"
	)
	db := openTestDB(t)
	// PostgreSQL 15 skips empty transactions, so the walsender sends keepalives
	// for WAL that has nothing to decode.
	skipBelowServerVersion(t, db, 150000, "the walsender only reports skipped WAL through keepalives from PostgreSQL 15")
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index}, table, noiseTable)

	// noiseTable is not in the publication.
	cfg := ackTestConfig(slotName, publicationName, []string{table}, map[string]string{"public." + table: index}, 100*time.Millisecond, 100)
	startConnector(t, cfg, handler)

	dmlCommit := commitWithLSN(t, db, func(tx *sql.Tx) {
		_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('last published change')`, table))
		require.NoError(t, err)
	})
	requireDocumentCount(t, esClient, index, 1)
	requireConfirmedReaches(t, db, slotName, dmlCommit)

	afterDML := currentWALLSN(t, db)
	generateNoise(t, db, noiseTable)

	requireConfirmedPasses(t, db, slotName, afterDML)
	// And it keeps following the WAL, not just once.
	requireConfirmedPasses(t, db, slotName, currentWALLSN(t, db))
}

// TestConnector_AckNonDMLAndSkippedMessages checks that messages the connector
// has nothing to index for are still acked: after each one the slot must pass
// it, and a mapped row written afterwards must still be indexed.
func TestConnector_AckNonDMLAndSkippedMessages(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_nondml"
		publicationName = "cdc_publication_ack_nondml"
		table           = "ack_nondml_items"
		unmappedTable   = "ack_nondml_audit"
		index           = "ack.nondml.items"
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index}, table, unmappedTable)

	// Both tables are published but only one is mapped; rows named "skip" make
	// the handler return no actions.
	cfg := ackTestConfig(slotName, publicationName, []string{table, unmappedTable}, map[string]string{"public." + table: index}, 100*time.Millisecond, 100)
	startConnector(t, cfg, func(msg cdc.Message) []elasticsearch.Action {
		if name, _ := msg.NewData["name"].(string); name == "skip" {
			return nil
		}
		return handler(msg)
	})

	requireDocument(t, esClient, index, insertRow(t, db, table, "baseline"))

	var skippedID string
	steps := []struct {
		run  func(t *testing.T) string // performs the step and returns the LSN the slot must reach
		name string
	}{
		{
			name: "truncate",
			run: func(t *testing.T) string {
				return commitWithLSN(t, db, func(tx *sql.Tx) {
					_, err := tx.Exec(fmt.Sprintf(`TRUNCATE %s`, table))
					require.NoError(t, err)
				})
			},
		},
		{
			name: "ddl then insert with a new relation",
			run: func(t *testing.T) string {
				_, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra TEXT`, table))
				require.NoError(t, err)
				var id int
				lsn := commitWithLSN(t, db, func(tx *sql.Tx) {
					require.NoError(t, tx.QueryRow(fmt.Sprintf(`INSERT INTO %s (name, extra) VALUES ('ddl', 'new column') RETURNING id`, table)).Scan(&id))
				})
				source := requireDocument(t, esClient, index, strconv.Itoa(id))
				assert.Equal(t, "new column", source["extra"], "the row must be decoded with the new relation")
				return lsn
			},
		},
		{
			name: "transactional logical message",
			run: func(t *testing.T) string {
				skipBelowServerVersion(t, db, 140000, "logical decoding messages are streamed from PostgreSQL 14")
				return commitWithLSN(t, db, func(tx *sql.Tx) {
					_, err := tx.Exec(`SELECT pg_logical_emit_message(true, 'ack-it', 'transactional')`)
					require.NoError(t, err)
				})
			},
		},
		{
			name: "non-transactional logical message",
			run: func(t *testing.T) string {
				skipBelowServerVersion(t, db, 140000, "logical decoding messages are streamed from PostgreSQL 14")
				// The function returns the end of the message record, which is the
				// position the message is delivered with.
				var lsn string
				require.NoError(t, db.QueryRow(`SELECT pg_logical_emit_message(false, 'ack-it', 'non-transactional')::text`).Scan(&lsn))
				return lsn
			},
		},
		{
			name: "row of a published but unmapped table",
			run: func(t *testing.T) string {
				return commitWithLSN(t, db, func(tx *sql.Tx) {
					_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('audit')`, unmappedTable))
					require.NoError(t, err)
				})
			},
		},
		{
			name: "handler returns no actions",
			run: func(t *testing.T) string {
				var id int
				lsn := commitWithLSN(t, db, func(tx *sql.Tx) {
					require.NoError(t, tx.QueryRow(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('skip') RETURNING id`, table)).Scan(&id))
				})
				skippedID = strconv.Itoa(id)
				return lsn
			},
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			requireConfirmedReaches(t, db, slotName, step.run(t))
			requireDocument(t, esClient, index, insertRow(t, db, table, "after "+step.name))
		})
	}

	if skippedID != "" {
		_, found, err := getDocument(esClient, index, skippedID, "")
		require.NoError(t, err)
		assert.False(t, found, "a row the handler skipped must not be indexed")
	}
}

// TestConnector_AckWaitsForUnflushedRows covers the old data-loss case: a message
// without actions must not be acked ahead of rows still waiting in the batch.
// The first connector never flushes on its own and then crashes; a second
// connector on the same slot must receive the buffered rows again. It indexes
// into a different index, so documents there can only come from redelivery.
//
// The crash is simulated with a proxy between the first connector and
// PostgreSQL: frozen, it drops all traffic but keeps the connection open, so the
// connector neither reaches the server nor notices that its walsender was
// terminated (which would make the core panic). Nothing the connector does
// afterwards, including whatever its shutdown flushes and acks, can move the
// slot, so the test does not depend on the order of steps in Close.
func TestConnector_AckWaitsForUnflushedRows(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_ahead"
		publicationName = "cdc_publication_ack_ahead"
		table           = "ack_ahead_items"
		unmappedTable   = "ack_ahead_audit"
		firstIndex      = "ack.ahead.first"
		secondIndex     = "ack.ahead.second"
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{firstIndex, secondIndex}, table, unmappedTable)
	tables := []string{table, unmappedTable}

	markerSeen := make(chan struct{})
	var markerOnce sync.Once
	firstHandler := func(msg cdc.Message) []elasticsearch.Action {
		if name, _ := msg.NewData["name"].(string); name == "marker" {
			markerOnce.Do(func() { close(markerSeen) })
		}
		return handler(msg)
	}
	// A one hour ticker and a batch far larger than the test keep every row
	// buffered. The connector reaches PostgreSQL through the proxy; with a one
	// hour slot activity check it sends no query that could hang once the proxy
	// is frozen.
	proxy := newPGProxy(t)
	firstCfg := ackTestConfig(slotName, publicationName, tables, map[string]string{"public." + table: firstIndex}, time.Hour, 1000)
	firstCfg.CDC.Host = "127.0.0.1"
	firstCfg.CDC.Port = proxy.port()
	firstCfg.CDC.Slot.SlotActivityCheckerInterval = 3_600_000 // milliseconds
	stopFirst := startConnector(t, firstCfg, firstHandler)

	var firstID int
	firstCommit := commitWithLSN(t, db, func(tx *sql.Tx) {
		require.NoError(t, tx.QueryRow(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('buffered-1') RETURNING id`, table)).Scan(&firstID))
	})
	ids := []string{strconv.Itoa(firstID), insertRow(t, db, table, "buffered-2"), insertRow(t, db, table, "buffered-3")}
	insertRow(t, db, unmappedTable, "nothing to index")
	ids = append(ids, insertRow(t, db, table, "marker"))

	// The listener handles messages in order, so once it reaches the marker it has
	// handled the unmapped row. Every standby reply sent after that reports what
	// was acked for it; waiting for two makes sure PostgreSQL has applied one.
	select {
	case <-markerSeen:
	case <-time.After(ackWaitTimeout):
		require.FailNow(t, "the connector did not receive the marker row")
	}
	requireStandbyReplies(t, db, slotName, time.Now(), 2)

	confirmed, diff, err := confirmedSince(db, slotName, firstCommit)
	require.NoError(t, err)
	assert.Negative(t, diff, "confirmed_flush_lsn %s passed rows that were never flushed (first commit %s)", confirmed, firstCommit)

	// Crash: cut the connector off, end its walsender, and only then stop it.
	proxy.freeze()
	terminateWalSender(t, db, slotName)
	crashPosition := confirmedFlushLSN(t, db, slotName)
	stopFirst()
	require.Equal(t, crashPosition, confirmedFlushLSN(t, db, slotName), "the crashed connector must not have moved the slot")

	// The second connector talks to PostgreSQL directly and starts from crashPosition.
	startConnector(t, ackTestConfig(slotName, publicationName, tables, map[string]string{"public." + table: secondIndex}, 100*time.Millisecond, 100), handler)
	for _, id := range ids {
		requireDocument(t, esClient, secondIndex, id)
	}
}

// TestConnector_AckStreamedTransaction follows the path most likely behind the
// incident, a large transaction that PostgreSQL streams while it is still open.
// It checks that streaming really happens; that small transactions committed
// meanwhile are indexed and confirmed while the streamed one is open; that none
// of the streamed rows are indexed before its COMMIT; and that afterwards every
// row is indexed and the slot reaches the transaction's end LSN.
// It does not catch acking only the last message of each flush: that also acks
// the final row, which carries the end LSN.
func TestConnector_AckStreamedTransaction(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_stream"
		publicationName = "cdc_publication_ack_stream"
		bigTable        = "ack_stream_big"
		smallTable      = "ack_stream_small"
		bigIndex        = "ack.stream.big"
		smallIndex      = "ack.stream.small"
		bigRows         = 2000 // several times the change data logical_decoding_work_mem allows
		smallRows       = 3
	)
	db := openTestDB(t)
	skipBelowServerVersion(t, db, 140000, "streaming in-progress transactions and pg_stat_replication_slots need PostgreSQL 14")
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{bigIndex, smallIndex}, bigTable, smallTable)
	// 64kB is the minimum. The walsender applies role settings when it starts,
	// so this has to happen before the connector connects.
	setRoleSetting(t, db, "logical_decoding_work_mem", "64kB")

	mapping := map[string]string{"public." + bigTable: bigIndex, "public." + smallTable: smallIndex}
	startConnector(t, ackTestConfig(slotName, publicationName, []string{bigTable, smallTable}, mapping, 100*time.Millisecond, 500), handler)

	big, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = big.Rollback() }) // runs before the tables are dropped
	_, err = big.Exec(fmt.Sprintf(`INSERT INTO %s (name) SELECT repeat('x', 200) || g FROM generate_series(1, %d) AS g`, bigTable, bigRows))
	require.NoError(t, err)

	// Only the open transaction is large enough to be streamed.
	requireStreamedTransaction(t, db, slotName)

	var smallCommit string
	for i := range smallRows {
		smallCommit = commitWithLSN(t, db, func(tx *sql.Tx) {
			_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, smallTable), fmt.Sprintf("small-%d", i))
			require.NoError(t, err)
		})
	}
	// While the big transaction is open, the small ones are indexed and acked on their own.
	requireDocumentCount(t, esClient, smallIndex, smallRows)
	requireConfirmedReaches(t, db, slotName, smallCommit)
	count, err := documentCount(esClient, bigIndex)
	require.NoError(t, err)
	require.Zero(t, count, "rows of an open transaction must not be indexed")

	var bigCommit string
	require.NoError(t, big.QueryRow(`SELECT pg_current_wal_insert_lsn()::text`).Scan(&bigCommit))
	require.NoError(t, big.Commit())

	requireDocumentCount(t, esClient, bigIndex, bigRows)
	requireConfirmedReaches(t, db, slotName, bigCommit)
}

func TestConnector_AckLargeTransactionAndFanout(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_fanout"
		publicationName = "cdc_publication_ack_fanout"
		table           = "ack_fanout_items"
		fanoutTable     = "ack_fanout_events"
		index           = "ack.fanout.items"
		fanoutIndex     = "ack.fanout.events"
		batchSizeLimit  = 10
		rows            = 35 // rows in one transaction, more than batchSizeLimit
		fanout          = 25 // actions for one message, more than batchSizeLimit
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index, fanoutIndex}, table, fanoutTable)

	fanoutHandler := func(msg cdc.Message) []elasticsearch.Action {
		if msg.TableName != fanoutTable {
			return handler(msg)
		}
		actions := make([]elasticsearch.Action, 0, fanout)
		for n := range fanout {
			id := fmt.Sprintf("%v-%d", msg.NewData["id"], n)
			actions = append(actions, elasticsearch.NewIndexAction([]byte(id), []byte(`{"fanout":true}`), nil))
		}
		return actions
	}
	mapping := map[string]string{"public." + table: index, "public." + fanoutTable: fanoutIndex}
	startConnector(t, ackTestConfig(slotName, publicationName, []string{table, fanoutTable}, mapping, 100*time.Millisecond, batchSizeLimit), fanoutHandler)

	txEnd := commitWithLSN(t, db, func(tx *sql.Tx) {
		_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) SELECT 'row-' || g FROM generate_series(1, %d) AS g`, table, rows))
		require.NoError(t, err)
	})
	requireDocumentCount(t, esClient, index, rows)
	requireConfirmedReaches(t, db, slotName, txEnd)

	fanoutEnd := commitWithLSN(t, db, func(tx *sql.Tx) {
		_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('fanout')`, fanoutTable))
		require.NoError(t, err)
	})
	requireDocumentCount(t, esClient, fanoutIndex, fanout)
	requireConfirmedReaches(t, db, slotName, fanoutEnd)
}

func TestConnector_ResponseHandlerReportsItemErrorsPerAction(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_items"
		publicationName = "cdc_publication_ack_items"
		table           = "ack_strict_items"
		index           = "ack.strict.items"
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index}, table)
	// A strict mapping rejects documents with unknown fields at item level.
	createIndex(t, esClient, index,
		`{"settings":{"number_of_shards":2,"number_of_replicas":0},`+
			`"mappings":{"dynamic":"strict","properties":{"name":{"type":"keyword"}}}}`)

	recorder := newResponseRecorder()
	cfg := ackTestConfig(slotName, publicationName, []string{table}, map[string]string{"public." + table: index}, 100*time.Millisecond, 100)
	startConnector(t, cfg, strictHandler, cdc.WithResponseHandler(recorder))

	// "routed" yields both of its actions from one message, so they share a bulk request.
	commit := commitWithLSN(t, db, func(tx *sql.Tx) {
		for _, name := range []string{"ok-1", "bad-1", "routed", "ok-2", "bad-2"} {
			_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, table), name)
			require.NoError(t, err)
		}
	})

	requireOutcomes(t, recorder, 6)
	successes, failures := recorder.snapshot()
	assert.Equal(t, map[string]int{"ok-1": 1, "ok-2": 1, "routed@ra": 1}, successes)
	assert.Equal(t, map[string]int{"bad-1": 1, "bad-2": 1, "routed@rb": 1}, failures)

	// Elasticsearch agrees with what the handler was told.
	for _, doc := range []struct{ id, routing string }{{"ok-1", ""}, {"ok-2", ""}, {"routed", "ra"}} {
		_, found, err := getDocument(esClient, index, doc.id, doc.routing)
		require.NoError(t, err)
		assert.True(t, found, "document %s (routing %q) must be indexed", doc.id, doc.routing)
	}
	for _, id := range []string{"bad-1", "bad-2"} {
		_, found, err := getDocument(esClient, index, id, "")
		require.NoError(t, err)
		assert.False(t, found, "rejected document %s must not be indexed", id)
	}
	// With a response handler, failed actions are reported and the batch is acked.
	requireConfirmedReaches(t, db, slotName, commit)
}

func TestConnector_ResponseHandlerReportsRequestFailureForEveryAction(t *testing.T) {
	const (
		slotName        = "cdc_slot_ack_request"
		publicationName = "cdc_publication_ack_request"
		table           = "ack_request_items"
		index           = "ack.request.items"
	)
	db := openTestDB(t)
	esClient := newESClient(t)
	setupAckScenario(t, db, esClient, slotName, publicationName, []string{index}, table)

	proxy, bulkRequests := newBulkFailingProxy(t)
	recorder := newResponseRecorder()
	cfg := ackTestConfig(slotName, publicationName, []string{table}, map[string]string{"public." + table: index}, 100*time.Millisecond, 100)
	cfg.Elasticsearch.URLs = []string{proxy.URL}
	startConnector(t, cfg, handler, cdc.WithResponseHandler(recorder))

	ids := make([]string, 0, 3)
	commit := commitWithLSN(t, db, func(tx *sql.Tx) {
		for _, name := range []string{"a", "b", "c"} {
			var id int
			require.NoError(t, tx.QueryRow(fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1) RETURNING id`, table), name).Scan(&id))
			ids = append(ids, strconv.Itoa(id))
		}
	})

	requireOutcomes(t, recorder, len(ids))
	successes, failures := recorder.snapshot()
	assert.Empty(t, successes, "no action of a failed bulk request may be reported as indexed")
	assert.Equal(t, map[string]int{ids[0]: 1, ids[1]: 1, ids[2]: 1}, failures)
	assert.Positive(t, bulkRequests.Load(), "the bulk requests must have gone through the proxy")

	count, err := documentCount(esClient, index)
	require.NoError(t, err)
	assert.Zero(t, count, "the failing proxy must have kept every document out of Elasticsearch")
	// With a response handler, the failed batch is acked once every action has been reported.
	requireConfirmedReaches(t, db, slotName, commit)
}

// strictHandler uses the row name as the document ID. Names starting with "bad"
// carry a field the strict mapping rejects; "routed" yields two actions with
// the same ID and different routing, of which only the second is rejected.
func strictHandler(msg cdc.Message) []elasticsearch.Action {
	name, _ := msg.NewData["name"].(string)
	accepted := []byte(fmt.Sprintf(`{"name":%q}`, name))
	rejected := []byte(fmt.Sprintf(`{"name":%q,"unexpected":true}`, name))
	switch {
	case name == "routed":
		first, second := "ra", "rb"
		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(name), accepted, &first),
			elasticsearch.NewIndexAction([]byte(name), rejected, &second),
		}
	case strings.HasPrefix(name, "bad"):
		return []elasticsearch.Action{elasticsearch.NewIndexAction([]byte(name), rejected, nil)}
	default:
		return []elasticsearch.Action{elasticsearch.NewIndexAction([]byte(name), accepted, nil)}
	}
}

// ackTestConfig builds a connector config in the style of the other tests in
// this package, for the given slot, publication, published tables and mapping.
func ackTestConfig(slotName, publicationName string, tables []string, mapping map[string]string, ticker time.Duration, batchSizeLimit int) config.Config {
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	publicationTables := make(publication.Tables, 0, len(tables))
	for _, table := range tables {
		publicationTables = append(publicationTables, publication.Table{
			Name:            table,
			ReplicaIdentity: publication.ReplicaIdentityFull,
		})
	}
	return config.Config{
		CDC: cdcconfig.Config{
			Host:     Infra.PostgresHost,
			Port:     postgresPort,
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              publicationName,
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
					publication.OperationTruncate,
				},
				Tables: publicationTables,
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        slotName,
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Elasticsearch: config.Elasticsearch{
			TableIndexMapping:           mapping,
			BatchTickerDuration:         ticker,
			BatchSizeLimit:              batchSizeLimit,
			URLs:                        []string{Infra.ElasticsearchURL},
			DisableDiscoverNodesOnStart: true,
		},
	}
}

// startConnector starts a connector in the background and waits until it
// streams. The returned function closes it once; it also runs on cleanup.
func startConnector(t *testing.T, cfg config.Config, h cdc.Handler, options ...cdc.Option) func() {
	t.Helper()
	ctx := context.Background()
	connector, err := cdc.NewConnector(ctx, cfg, h, options...)
	require.NoError(t, err)

	var once sync.Once
	stop := func() { once.Do(connector.Close) }
	t.Cleanup(stop)

	go connector.Start(ctx)

	readyCtx, cancel := context.WithTimeout(ctx, ackWaitTimeout)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))
	return stop
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func newESClient(t *testing.T) *es.Client {
	t.Helper()
	esClient, err := es.NewClient(es.Config{
		Addresses:            []string{Infra.ElasticsearchURL},
		DiscoverNodesOnStart: false,
	})
	require.NoError(t, err)
	return esClient
}

// setupAckScenario creates the scenario's tables and drops its slot,
// publication, tables and indices both now (leftovers of an interrupted run)
// and on cleanup. Call it before startConnector: cleanups run in reverse
// order, so the connector is closed before its slot is dropped.
func setupAckScenario(t *testing.T, db *sql.DB, esClient *es.Client, slotName, publicationName string, indices []string, tables ...string) {
	t.Helper()
	cleanup := func() {
		dropSlot(t, db, slotName)
		cleanupExec(t, db, fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`, publicationName))
		cleanupExec(t, db, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, strings.Join(tables, ", ")))
		deleteIndices(t, esClient, indices...)
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, table := range tables {
		_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`, table))
		require.NoError(t, err)
	}
}

// dropSlot drops a replication slot once the connector's walsender has released
// it, terminating the walsender if it does not.
func dropSlot(t *testing.T, db *sql.DB, slotName string) {
	t.Helper()
	terminated := false
	deadline := time.Now().Add(ackWaitTimeout)
	for {
		var active bool
		var activePID sql.NullInt64
		err := db.QueryRow(`SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1`, slotName).
			Scan(&active, &activePID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return
		case err != nil:
			t.Errorf("read replication slot %s: %v", slotName, err)
			return
		case !active:
			if _, err = db.Exec(`SELECT pg_drop_replication_slot($1)`, slotName); err != nil {
				t.Errorf("drop replication slot %s: %v", slotName, err)
			}
			return
		case time.Now().After(deadline) && !terminated:
			_, _ = db.Exec(`SELECT pg_terminate_backend($1)`, activePID.Int64)
			terminated = true
			deadline = time.Now().Add(ackWaitTimeout)
		case time.Now().After(deadline):
			t.Errorf("replication slot %s is still active", slotName)
			return
		}
		time.Sleep(ackPollInterval)
	}
}

func cleanupExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Errorf("%s: %v", query, err)
	}
}

func deleteIndices(t *testing.T, esClient *es.Client, indices ...string) {
	t.Helper()
	res, err := esClient.Indices.Delete(indices, esClient.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil {
		t.Errorf("delete indices %v: %v", indices, err)
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		t.Errorf("delete indices %v: %s", indices, res.String())
	}
}

func createIndex(t *testing.T, esClient *es.Client, index, body string) {
	t.Helper()
	res, err := esClient.Indices.Create(index, esClient.Indices.Create.WithBody(strings.NewReader(body)))
	require.NoError(t, err)
	defer res.Body.Close()
	require.False(t, res.IsError(), res.String())
}

// insertRow inserts a row in its own transaction and returns its id, which is
// the document ID handler uses.
func insertRow(t *testing.T, db *sql.DB, table, name string) string {
	t.Helper()
	var id int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1) RETURNING id`, table), name).Scan(&id))
	return strconv.Itoa(id)
}

// commitWithLSN runs fn in one transaction and returns pg_current_wal_insert_lsn()
// read just before COMMIT. Every change of the transaction lies below that LSN
// and its commit record at or above it, so confirmed_flush_lsn reaches it only
// once the connector has acked the end of the transaction.
func commitWithLSN(t *testing.T, db *sql.DB, fn func(tx *sql.Tx)) string {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	fn(tx)
	var lsn string
	require.NoError(t, tx.QueryRow(`SELECT pg_current_wal_insert_lsn()::text`).Scan(&lsn))
	require.NoError(t, tx.Commit())
	return lsn
}

func currentWALLSN(t *testing.T, db *sql.DB) string {
	t.Helper()
	var lsn string
	require.NoError(t, db.QueryRow(`SELECT pg_current_wal_lsn()::text`).Scan(&lsn))
	return lsn
}

// confirmedSince returns the slot's confirmed_flush_lsn and its distance in
// bytes from lsn, negative while the slot is still below lsn.
func confirmedSince(db *sql.DB, slotName, lsn string) (string, float64, error) {
	var confirmed string
	var diff float64
	err := db.QueryRow(
		`SELECT confirmed_flush_lsn::text, pg_wal_lsn_diff(confirmed_flush_lsn, $2::pg_lsn) FROM pg_replication_slots WHERE slot_name = $1`,
		slotName, lsn,
	).Scan(&confirmed, &diff)
	return confirmed, diff, err
}

// requireConfirmedReaches waits until the slot's confirmed_flush_lsn is at or past lsn.
func requireConfirmedReaches(t *testing.T, db *sql.DB, slotName, lsn string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		confirmed, diff, err := confirmedSince(db, slotName, lsn)
		if assert.NoError(c, err) {
			assert.GreaterOrEqual(c, diff, 0.0, "confirmed_flush_lsn %s has not reached %s", confirmed, lsn)
		}
	}, ackWaitTimeout, ackPollInterval)
}

// requireConfirmedPasses waits until the slot's confirmed_flush_lsn is past lsn.
func requireConfirmedPasses(t *testing.T, db *sql.DB, slotName, lsn string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		confirmed, diff, err := confirmedSince(db, slotName, lsn)
		if assert.NoError(c, err) {
			assert.Positive(c, diff, "confirmed_flush_lsn %s has not passed %s", confirmed, lsn)
		}
	}, ackWaitTimeout, ackPollInterval)
}

// requireStandbyReplies waits until PostgreSQL has processed n standby status
// updates for the slot that the connector sent after since.
func requireStandbyReplies(t *testing.T, db *sql.DB, slotName string, since time.Time, n int) {
	t.Helper()
	last := since
	for range n {
		var replyTime time.Time
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var reply time.Time
			err := db.QueryRow(
				`SELECT r.reply_time FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1`,
				slotName,
			).Scan(&reply)
			if assert.NoError(c, err) && assert.True(c, reply.After(last), "no standby reply after %s", last) {
				replyTime = reply
			}
		}, ackWaitTimeout, ackPollInterval)
		last = replyTime
	}
}

func confirmedFlushLSN(t *testing.T, db *sql.DB, slotName string) string {
	t.Helper()
	var lsn string
	require.NoError(t, db.QueryRow(`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&lsn))
	return lsn
}

// terminateWalSender ends the walsender holding the slot and waits until the
// slot is released.
func terminateWalSender(t *testing.T, db *sql.DB, slotName string) {
	t.Helper()
	var terminated bool
	require.NoError(t, db.QueryRow(
		`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active`,
		slotName,
	).Scan(&terminated))
	require.True(t, terminated, "the walsender of %s was not terminated", slotName)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var active bool
		if assert.NoError(c, db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&active)) {
			assert.False(c, active, "slot %s is still held", slotName)
		}
	}, ackWaitTimeout, ackPollInterval)
}

// setRoleSetting sets a parameter for new sessions of the test role and resets
// it on cleanup.
func setRoleSetting(t *testing.T, db *sql.DB, name, value string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf(`ALTER ROLE cdc_user SET %s = '%s'`, name, value))
	require.NoError(t, err)
	t.Cleanup(func() { cleanupExec(t, db, fmt.Sprintf(`ALTER ROLE cdc_user RESET %s`, name)) })
}

// requireStreamedTransaction waits until PostgreSQL has streamed an in-progress
// transaction through the slot.
func requireStreamedTransaction(t *testing.T, db *sql.DB, slotName string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var streamed int64
		err := db.QueryRow(`SELECT stream_txns FROM pg_stat_replication_slots WHERE slot_name = $1`, slotName).Scan(&streamed)
		if assert.NoError(c, err) {
			assert.Positive(c, streamed, "no transaction was streamed through %s", slotName)
		}
	}, ackWaitTimeout, ackPollInterval)
}

func skipBelowServerVersion(t *testing.T, db *sql.DB, minimum int, reason string) {
	t.Helper()
	var version int
	require.NoError(t, db.QueryRow(`SHOW server_version_num`).Scan(&version))
	if version < minimum {
		t.Skipf("PostgreSQL %d: %s", version, reason)
	}
}

// generateNoise keeps writing to a table outside the publication until the test
// ends, so the walsender keeps decoding WAL without anything to send. Each write
// makes the walsender send a keepalive; spacing them beyond the core's 300 ms
// idle read timeout lets every idle status update report the latest confirmed
// position instead of waiting for the periodic one.
func generateNoise(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ('noise')`, table))
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// documentCount refreshes index and counts its documents; a missing index is empty.
func documentCount(esClient *es.Client, index string) (int, error) {
	refresh, err := esClient.Indices.Refresh(
		esClient.Indices.Refresh.WithIndex(index),
		esClient.Indices.Refresh.WithIgnoreUnavailable(true),
	)
	if err != nil {
		return 0, err
	}
	refresh.Body.Close()

	res, err := esClient.Count(esClient.Count.WithIndex(index), esClient.Count.WithIgnoreUnavailable(true))
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return 0, fmt.Errorf("count %s: %s", index, res.String())
	}
	var body struct {
		Count int `json:"count"`
	}
	if err = json.NewDecoder(res.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.Count, nil
}

func requireDocumentCount(t *testing.T, esClient *es.Client, index string, want int) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		count, err := documentCount(esClient, index)
		if assert.NoError(c, err) {
			assert.Equal(c, want, count, "documents in %s", index)
		}
	}, ackWaitTimeout, ackPollInterval)
}

// getDocument fetches a document by ID, with routing when it is not empty.
// found is false when the index or the document does not exist.
func getDocument(esClient *es.Client, index, id, routing string) (map[string]any, bool, error) {
	var options []func(*esapi.GetRequest)
	if routing != "" {
		options = append(options, esClient.Get.WithRouting(routing))
	}
	res, err := esClient.Get(index, id, options...)
	if err != nil {
		return nil, false, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if res.IsError() {
		return nil, false, fmt.Errorf("get %s/%s: %s", index, id, res.String())
	}
	var body struct {
		Source map[string]any `json:"_source"`
	}
	if err = json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, false, err
	}
	return body.Source, true, nil
}

// requireDocument waits until index holds the document and returns its source.
func requireDocument(t *testing.T, esClient *es.Client, index, id string) map[string]any {
	t.Helper()
	var source map[string]any
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		doc, found, err := getDocument(esClient, index, id, "")
		if assert.NoError(c, err) && assert.True(c, found, "document %s/%s is not indexed", index, id) {
			source = doc
		}
	}, ackWaitTimeout, ackPollInterval)
	return source
}

// newBulkFailingProxy puts a reverse proxy in front of Elasticsearch that
// answers every bulk request with HTTP 500 and passes everything else through.
func newBulkFailingProxy(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	target, err := url.Parse(Infra.ElasticsearchURL)
	require.NoError(t, err)
	upstream := httputil.NewSingleHostReverseProxy(target)

	var bulkRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_bulk") {
			upstream.ServeHTTP(w, r)
			return
		}
		bulkRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"proxy_error","reason":"injected bulk failure"},"status":500}`))
	}))
	t.Cleanup(server.Close)
	return server, &bulkRequests
}

// pgProxy forwards TCP connections to PostgreSQL. Frozen, it drops all traffic
// in both directions but keeps every connection open, so a client can neither
// reach the server nor notice that its server side went away: to PostgreSQL
// the client has vanished, as in a crash.
type pgProxy struct {
	listener net.Listener
	conns    []net.Conn
	wg       sync.WaitGroup
	mu       sync.Mutex
	frozen   atomic.Bool
	closed   bool
}

func newPGProxy(t *testing.T) *pgProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &pgProxy{listener: listener}
	target := net.JoinHostPort(Infra.PostgresHost, Infra.PostgresPort)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			if !p.track(client, server) {
				return
			}
			p.wg.Add(2)
			go p.pipe(server, client)
			go p.pipe(client, server)
		}
	}()
	t.Cleanup(p.close)
	return p
}

// track registers a proxied connection pair, closing it when the proxy is closed.
func (p *pgProxy) track(client, server net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = client.Close()
		_ = server.Close()
		return false
	}
	p.conns = append(p.conns, client, server)
	return true
}

// pipe copies src to dst. Frozen, it discards what it reads and leaves dst open
// when src goes away.
func (p *pgProxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.frozen.Load() {
			_, _ = dst.Write(buf[:n])
		}
		if err != nil {
			if !p.frozen.Load() {
				_ = dst.Close()
			}
			return
		}
	}
}

func (p *pgProxy) port() int {
	return p.listener.Addr().(*net.TCPAddr).Port
}

func (p *pgProxy) freeze() {
	p.frozen.Store(true)
}

func (p *pgProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// responseRecorder is a response handler that counts outcomes per action,
// keyed by "<id>" or "<id>@<routing>".
type responseRecorder struct {
	successes map[string]int
	failures  map[string]int
	mu        sync.Mutex
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{successes: map[string]int{}, failures: map[string]int{}}
}

func (r *responseRecorder) OnInit(*elasticsearch.ResponseHandlerInitContext) {}

func (r *responseRecorder) OnSuccess(ctx *elasticsearch.ResponseHandlerContext) {
	r.record(r.successes, ctx.Action)
}

func (r *responseRecorder) OnError(ctx *elasticsearch.ResponseHandlerContext) {
	r.record(r.failures, ctx.Action)
}

func (r *responseRecorder) record(outcomes map[string]int, action *elasticsearch.Action) {
	label := string(action.ID)
	if action.Routing != nil {
		label += "@" + *action.Routing
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	outcomes[label]++
}

func (r *responseRecorder) snapshot() (successes, failures map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.successes), maps.Clone(r.failures)
}

func (r *responseRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, n := range r.successes {
		total += n
	}
	for _, n := range r.failures {
		total += n
	}
	return total
}

// requireOutcomes waits until the recorder has received n callbacks.
func requireOutcomes(t *testing.T, recorder *responseRecorder, n int) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, n, recorder.total(), "response handler callbacks")
	}, ackWaitTimeout, ackPollInterval)
}
