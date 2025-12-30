package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	es "github.com/elastic/go-elasticsearch/v7"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_InitialMode(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS books (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert existing data before snapshot
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`INSERT INTO books (title, author) VALUES ($1, $2)`,
			fmt.Sprintf("Book %d", i),
			fmt.Sprintf("Author %d", i))
		require.NoError(t, err)
	}

	// Setup connector with snapshot enabled
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Config{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication_snapshot",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "books",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot_snapshot",
				SlotActivityCheckerInterval: 3000,
			},
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled:   true,
				Mode:      "initial",
				ChunkSize: 1000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Elasticsearch: config.Elasticsearch{
			TableIndexMapping: map[string]string{
				"public.books": "books.snapshot",
			},
			BatchTickerDuration:         time.Millisecond * 100,
			BatchSizeLimit:              10,
			URLs:                        []string{Infra.ElasticsearchURL},
			DisableDiscoverNodesOnStart: true,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, snapshotHandler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector in background
	go connector.Start(ctx)

	// Wait for connector to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Wait for snapshot to complete
	time.Sleep(time.Second * 5)

	// Setup Elasticsearch client to verify documents
	esClient, err := es.NewClient(es.Config{
		Addresses:            []string{Infra.ElasticsearchURL},
		DiscoverNodesOnStart: false,
	})
	require.NoError(t, err)

	// Refresh index
	_, err = esClient.Indices.Refresh(esClient.Indices.Refresh.WithIndex("books.snapshot"))
	require.NoError(t, err)

	// Search for documents
	res, err := esClient.Search(
		esClient.Search.WithIndex("books.snapshot"),
		esClient.Search.WithSize(20),
	)
	require.NoError(t, err)
	defer res.Body.Close()

	var searchResult map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&searchResult)
	require.NoError(t, err)

	hits := searchResult["hits"].(map[string]interface{})["hits"].([]interface{})
	assert.GreaterOrEqual(t, len(hits), 10, "Should have at least 10 documents from snapshot")

	// Verify snapshot documents
	snapshotCount := 0
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if source["operation"] == "SNAPSHOT" {
			snapshotCount++
			assert.Contains(t, source["title"], "Book")
			assert.Contains(t, source["author"], "Author")
		}
	}
	assert.GreaterOrEqual(t, snapshotCount, 10, "Should have at least 10 SNAPSHOT operations")

	// Insert new data after snapshot - should be captured via CDC
	for i := 11; i <= 15; i++ {
		_, err = db.Exec(`INSERT INTO books (title, author) VALUES ($1, $2)`,
			fmt.Sprintf("Book %d", i),
			fmt.Sprintf("Author %d", i))
		require.NoError(t, err)
	}

	// Wait for CDC to process new inserts
	time.Sleep(time.Second * 2)

	// Refresh and search again
	_, err = esClient.Indices.Refresh(esClient.Indices.Refresh.WithIndex("books.snapshot"))
	require.NoError(t, err)

	res2, err := esClient.Search(
		esClient.Search.WithIndex("books.snapshot"),
		esClient.Search.WithSize(20),
	)
	require.NoError(t, err)
	defer res2.Body.Close()

	var searchResult2 map[string]interface{}
	err = json.NewDecoder(res2.Body).Decode(&searchResult2)
	require.NoError(t, err)

	hits2 := searchResult2["hits"].(map[string]interface{})["hits"].([]interface{})
	assert.GreaterOrEqual(t, len(hits2), 15, "Should have at least 15 documents (10 snapshot + 5 CDC)")

	// Verify we have both snapshot and INSERT operations
	insertCount := 0
	for _, hit := range hits2 {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if source["operation"] == "INSERT" {
			insertCount++
		}
	}
	assert.GreaterOrEqual(t, insertCount, 5, "Should have at least 5 INSERT operations from CDC")
}

func TestSnapshot_SnapshotOnlyMode(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id SERIAL PRIMARY KEY,
			order_number TEXT NOT NULL,
			amount DECIMAL(10,2),
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert existing data
	for i := 1; i <= 20; i++ {
		_, err = db.Exec(`INSERT INTO orders (order_number, amount) VALUES ($1, $2)`,
			fmt.Sprintf("ORDER-%05d", i),
			float64(i)*100.0)
		require.NoError(t, err)
	}

	// Setup connector with snapshot_only mode
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Config{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled:   true,
				Mode:      "snapshot_only",
				ChunkSize: 1000,
				Tables: []publication.Table{
					{
						Name:            "orders",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Elasticsearch: config.Elasticsearch{
			TableIndexMapping: map[string]string{
				"public.orders": "orders.snapshot_only",
			},
			BatchTickerDuration:         time.Millisecond * 100,
			BatchSizeLimit:              10,
			URLs:                        []string{Infra.ElasticsearchURL},
			DisableDiscoverNodesOnStart: true,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, snapshotHandler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector - in snapshot_only mode, it will complete and exit
	go connector.Start(ctx)

	// Wait for connector to be ready (snapshot_only mode signals ready immediately)
	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Wait for snapshot to complete
	time.Sleep(time.Second * 5)

	// Setup Elasticsearch client to verify documents
	esClient, err := es.NewClient(es.Config{
		Addresses:            []string{Infra.ElasticsearchURL},
		DiscoverNodesOnStart: false,
	})
	require.NoError(t, err)

	// Refresh index
	_, err = esClient.Indices.Refresh(esClient.Indices.Refresh.WithIndex("orders.snapshot_only"))
	require.NoError(t, err)

	// Search for documents
	res, err := esClient.Search(
		esClient.Search.WithIndex("orders.snapshot_only"),
		esClient.Search.WithSize(30),
	)
	require.NoError(t, err)
	defer res.Body.Close()

	var searchResult map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&searchResult)
	require.NoError(t, err)

	hits := searchResult["hits"].(map[string]interface{})["hits"].([]interface{})
	assert.GreaterOrEqual(t, len(hits), 20, "Should have at least 20 documents from snapshot_only")

	// Verify all are snapshot documents
	snapshotCount := 0
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if source["operation"] == "SNAPSHOT" {
			snapshotCount++
			assert.Contains(t, source["order_number"], "ORDER-")
			assert.NotNil(t, source["amount"])
		}
	}
	assert.GreaterOrEqual(t, snapshotCount, 20, "Should have at least 20 SNAPSHOT operations")
}

func snapshotHandler(msg cdc.Message) []elasticsearch.Action {
	if msg.Type.IsSnapshot() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		key := ""
		if id, ok := msg.NewData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(key), newData, nil),
		}
	}

	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		key := ""
		if id, ok := msg.NewData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(key), newData, nil),
		}
	}

	if msg.Type.IsDelete() {
		msg.OldData["operation"] = msg.Type

		key := ""
		if id, ok := msg.OldData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []elasticsearch.Action{
			elasticsearch.NewDeleteAction([]byte(key), nil),
		}
	}

	return []elasticsearch.Action{}
}
