package bulk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	elasticsearch2 "github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
)

const testIndex = "users"

func TestBulkAcksEveryMessageOnceAfterItsFlushInDeliveryOrder(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 3, 1, nil)

	b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0"), testIndex, true)
	b.AddActions(newAckContext(events, "m1"), time.Now(), indexActions("d1"), testIndex, true)
	// The third action reaches batchSizeLimit and flushes inline.
	b.AddActions(newAckContext(events, "m2"), time.Now(), indexActions("d2"), testIndex, true)
	// Nothing is waiting to be flushed, so a message without actions is acked at once.
	b.AckWhenFlushed(newAckContext(events, "m3"))

	b.AddActions(newAckContext(events, "m4"), time.Now(), indexActions("d4"), testIndex, true)
	// d4 is not flushed yet, so m5 must wait for the next flush.
	b.AckWhenFlushed(newAckContext(events, "m5"))
	assert.False(t, events.has("ack:m5"), "m5 acked before d4 was flushed")

	// m6 arrives in two chunks; the second one triggers the inline flush.
	m6 := newAckContext(events, "m6")
	b.AddActions(m6, time.Now(), indexActions("d6a"), testIndex, false)
	b.AddActions(m6, time.Now(), indexActions("d6b", "d6c"), testIndex, true)

	// A ticker flush between two chunks of m7 sends the first chunk but has nothing to ack.
	m7 := newAckContext(events, "m7")
	b.AddActions(m7, time.Now(), indexActions("d7a"), testIndex, false)
	b.flushMessages()
	b.AddActions(m7, time.Now(), indexActions("d7b"), testIndex, true)
	b.flushMessages()

	assert.Equal(t, []string{
		"request:d0,d1,d2", "ack:m0", "ack:m1", "ack:m2",
		"ack:m3",
		"request:d4,d6a,d6b,d6c", "ack:m4", "ack:m5", "ack:m6",
		"request:d7a",
		"request:d7b", "ack:m7",
	}, events.list())
	assert.Empty(t, b.pendingAcks)
}

func TestBulkAcksManyMessagesExactlyOnceInDeliveryOrder(t *testing.T) {
	const messages = 60
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 7, 4, nil)

	// barrier is, for every message, the last action delivered up to and
	// including it: the message must not be acked before that action is sent.
	barrier := make(map[string]string, messages)
	lastAction := ""
	for i := range messages {
		id := fmt.Sprintf("m%02d", i)
		ctx := newAckContext(events, id)
		switch {
		case i%5 == 4:
			b.AckWhenFlushed(ctx)
		case i%3 == 0:
			b.AddActions(ctx, time.Now(), indexActions(id+"a"), testIndex, false)
			b.AddActions(ctx, time.Now(), indexActions(id+"b", id+"c"), testIndex, true)
			lastAction = id + "c"
		default:
			b.AddActions(ctx, time.Now(), indexActions(id+"a"), testIndex, true)
			lastAction = id + "a"
		}
		barrier[id] = lastAction
		if i%9 == 8 {
			b.flushMessages() // what the ticker does
		}
	}
	b.Close()

	sentAt := make(map[string]int)
	ackedAt := make(map[string]int)
	var acked []string
	for i, event := range events.list() {
		if docs, ok := strings.CutPrefix(event, "request:"); ok {
			for _, doc := range strings.Split(docs, ",") {
				require.NotContains(t, sentAt, doc, "action %s sent twice", doc)
				sentAt[doc] = i
			}
			continue
		}
		id := strings.TrimPrefix(event, "ack:")
		require.NotContains(t, ackedAt, id, "message %s acked twice", id)
		ackedAt[id] = i
		acked = append(acked, id)
	}

	require.Len(t, acked, messages)
	assert.IsIncreasing(t, acked, "acks must follow delivery order")
	for id, action := range barrier {
		require.Contains(t, sentAt, action)
		assert.Greater(t, ackedAt[id], sentAt[action], "message %s acked before action %s was sent", id, action)
	}
}

func TestBulkAckWhenFlushedWaitsForInFlightFlush(t *testing.T) {
	events := &eventLog{}
	transport := &fakeTransport{log: events, entered: make(chan struct{}, 1), release: make(chan struct{})}
	b := newTestBulk(t, transport, 10, 1, nil)
	b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0"), testIndex, true)

	flushed := make(chan struct{})
	go func() {
		b.flushMessages() // the ticker flush
		close(flushed)
	}()
	<-transport.entered // d0 is in flight and the flush holds flushLock

	acked := make(chan struct{})
	go func() {
		b.AckWhenFlushed(newAckContext(events, "m1"))
		close(acked)
	}()
	assert.Never(t, func() bool { return events.has("ack:m1") }, 100*time.Millisecond, 10*time.Millisecond)

	close(transport.release)
	<-flushed
	<-acked

	assert.Equal(t, []string{"request:d0", "ack:m0", "ack:m1"}, events.list())
}

func TestBulkTickerFlushAcksPendingMessages(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 10, 2, nil)
	b.batchTickerDuration = 10 * time.Millisecond
	b.batchTicker.Reset(b.batchTickerDuration)

	b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0"), testIndex, true)
	b.AckWhenFlushed(newAckContext(events, "m1"))
	b.StartBulk()

	assert.Eventually(t, func() bool { return events.has("ack:m1") }, time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{"request:d0", "ack:m0", "ack:m1"}, events.list())
}

func TestBulkQueuedAcksForOneHotDocumentTriggerFlush(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 3, 1, nil)

	// Four updates to one document collapse into a single batched action, but the
	// third queued ack reaches batchSizeLimit and flushes without the ticker.
	for i := range 4 {
		b.AddActions(newAckContext(events, fmt.Sprintf("m%d", i)), time.Now(), indexActions("d0"), testIndex, true)
	}
	assert.Equal(t, []string{"request:d0", "ack:m0", "ack:m1", "ack:m2"}, events.list())

	b.flushMessages() // the next ticker flush

	assert.Equal(t, []string{"request:d0", "ack:m0", "ack:m1", "ack:m2", "request:d0", "ack:m3"}, events.list())
}

func TestBulkQueuedAcksWithoutActionsTriggerFlush(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 3, 2, nil)

	b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0"), testIndex, true)
	b.AckWhenFlushed(newAckContext(events, "m1"))
	assert.Empty(t, events.list())
	// The third queued ack reaches batchSizeLimit: d0 is flushed, then all three are acked.
	b.AckWhenFlushed(newAckContext(events, "m2"))

	assert.Equal(t, []string{"request:d0", "ack:m0", "ack:m1", "ack:m2"}, events.list())
}

func TestBulkAcksEmptyLastChunkOnNextFlush(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{log: events}, 10, 1, nil)

	// An empty last chunk leaves nothing to send, yet the next flush must still ack
	// it, and a later message without actions must queue behind it.
	b.AddActions(newAckContext(events, "m0"), time.Now(), nil, testIndex, true)
	b.AckWhenFlushed(newAckContext(events, "m1"))
	assert.Empty(t, events.list())

	b.flushMessages()

	assert.Equal(t, []string{"ack:m0", "ack:m1"}, events.list())
}

func TestBulkSkipsMissingAckContexts(t *testing.T) {
	b := newTestBulk(t, &fakeTransport{}, 10, 1, nil)

	assert.NotPanics(t, func() {
		b.AckWhenFlushed(nil)
		b.AckWhenFlushed(&replication.ListenerContext{})
		b.AddActions(nil, time.Now(), indexActions("d0"), testIndex, true)
		b.AckWhenFlushed(nil)
		b.AckWhenFlushed(&replication.ListenerContext{})
		b.flushMessages()
	})
	assert.Empty(t, b.pendingAcks)
}

func TestBulkRequestFailureReportsEveryActionAsError(t *testing.T) {
	tests := []struct {
		transport *fakeTransport
		name      string
		wantErr   string
	}{
		{
			name:      "transport error",
			transport: &fakeTransport{err: errors.New("connection reset by peer")},
			wantErr:   "connection reset by peer",
		},
		{
			name:      "HTTP 500",
			transport: &fakeTransport{status: http.StatusInternalServerError},
			wantErr:   "500 Internal Server Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &eventLog{}
			handler := newRecordingHandler()
			b := newTestBulk(t, tt.transport, 10, 2, handler)

			b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0", "d1", "d2"), testIndex, true)
			b.flushMessages()

			successes, failures, errs := handler.snapshot()
			assert.Empty(t, successes)
			assert.Equal(t, map[string]int{"d0": 1, "d1": 1, "d2": 1}, failures)
			for _, id := range []string{"d0", "d1", "d2"} {
				require.ErrorContains(t, errs[id], tt.wantErr)
			}
			// With a response handler the failed batch is dropped and acked, as before.
			assert.Equal(t, []string{"ack:m0"}, events.list())
		})
	}
}

func TestBulkRequestItemErrorsAreRoutedPerAction(t *testing.T) {
	handler := newRecordingHandler()
	failed := func(_ int, doc bulkDoc) bool { return doc.id == "d1" || doc.id == "d3" }
	b := newTestBulk(t, &fakeTransport{failItem: failed}, 10, 1, handler)
	routing := "tenant-1"
	actions := append(indexActions("d0", "d1", "d2"),
		elasticsearch2.NewIndexAction([]byte("d3"), []byte(`{"id":"d3"}`), &routing),
		elasticsearch2.NewDeleteAction([]byte("d4"), nil),
	)

	b.AddActions(newAckContext(&eventLog{}, "m0"), time.Now(), actions, testIndex, true)
	b.flushMessages()

	successes, failures, errs := handler.snapshot()
	assert.Equal(t, map[string]int{"d0": 1, "d2": 1, "d4": 1}, successes)
	assert.Equal(t, map[string]int{"d1": 1, "d3@tenant-1": 1}, failures)
	require.ErrorContains(t, errs["d1"], "mapper_parsing_exception")
}

func TestBulkRequestItemErrorsAreMatchedByPosition(t *testing.T) {
	handler := newRecordingHandler()
	// Both items carry the same _id and _index; Elasticsearch fails only the second.
	failSecond := func(position int, _ bulkDoc) bool { return position == 1 }
	b := newTestBulk(t, &fakeTransport{failItem: failSecond}, 10, 1, handler)
	first, second := "r1", "r2"
	actions := []elasticsearch2.Action{
		elasticsearch2.NewIndexAction([]byte("d1"), []byte(`{"id":"d1"}`), &first),
		elasticsearch2.NewIndexAction([]byte("d1"), []byte(`{"id":"d1"}`), &second),
	}

	b.AddActions(newAckContext(&eventLog{}, "m0"), time.Now(), actions, testIndex, true)
	b.flushMessages()

	successes, failures, _ := handler.snapshot()
	assert.Equal(t, map[string]int{"d1@r1": 1}, successes)
	assert.Equal(t, map[string]int{"d1@r2": 1}, failures)
}

func TestBulkRequestUnmatchedResponseReportsEveryActionAsError(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "invalid JSON", body: `{"took":1,"errors":`},
		{name: "empty object", body: `{}`, wantErr: "errors field is <nil>, not a bool"},
		{name: "errors is not a bool", body: `{"errors":"false"}`, wantErr: "errors field is string, not a bool"},
		{
			name:    "errors without items",
			body:    `{"took":1,"errors":true}`,
			wantErr: "item count 0 does not match action count 3",
		},
		{
			name:    "fewer items than actions",
			body:    `{"took":1,"errors":true,"items":[{"index":{"_index":"users","_id":"d0","status":201}}]}`,
			wantErr: "item count 1 does not match action count 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newRecordingHandler()
			b := newTestBulk(t, &fakeTransport{body: tt.body}, 10, 1, handler)

			b.AddActions(newAckContext(&eventLog{}, "m0"), time.Now(), indexActions("d0", "d1", "d2"), testIndex, true)
			b.flushMessages()

			successes, failures, errs := handler.snapshot()
			assert.Empty(t, successes)
			assert.Equal(t, map[string]int{"d0": 1, "d1": 1, "d2": 1}, failures)
			for _, id := range []string{"d0", "d1", "d2"} {
				require.Error(t, errs[id])
				assert.Contains(t, errs[id].Error(), tt.wantErr)
			}
		})
	}
}

func TestBulkRequestMalformedItemIsReportedAsError(t *testing.T) {
	handler := newRecordingHandler()
	body := `{"took":1,"errors":true,"items":[{"index":{"_index":"users","_id":"d0","status":201}},"unexpected"]}`
	b := newTestBulk(t, &fakeTransport{body: body}, 10, 1, handler)

	b.AddActions(newAckContext(&eventLog{}, "m0"), time.Now(), indexActions("d0", "d1"), testIndex, true)
	b.flushMessages()

	successes, failures, _ := handler.snapshot()
	assert.Equal(t, map[string]int{"d0": 1}, successes)
	assert.Equal(t, map[string]int{"d1": 1}, failures)
}

func TestBulkRequestFailureWithoutResponseHandlerPanics(t *testing.T) {
	events := &eventLog{}
	b := newTestBulk(t, &fakeTransport{status: http.StatusInternalServerError}, 3, 1, nil)
	b.AddActions(newAckContext(events, "m0"), time.Now(), indexActions("d0"), testIndex, true)

	assert.Panics(t, b.flushMessages)
	assert.Empty(t, events.list(), "a flush that panics must not ack")
}

func newTestBulk(
	t *testing.T,
	transport http.RoundTripper,
	batchSizeLimit, concurrentRequest int,
	handler elasticsearch2.ResponseHandler,
) *Bulk {
	t.Helper()

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:            []string{"http://elasticsearch.test:9200"},
		Transport:            transport,
		DisableRetry:         true,
		UseResponseCheckOnly: true,
	})
	require.NoError(t, err)

	cfg := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      batchSizeLimit,
			BatchTickerDuration: time.Hour,
			BatchByteSizeLimit:  "10mb",
			ConcurrentRequest:   concurrentRequest,
			Version:             "7.17.0",
		},
	}
	cfg.CDC.Slot.Name = "test_slot"

	var options []Option
	if handler != nil {
		options = append(options, WithResponseHandler(handler))
	}
	b, err := NewBulk(cfg, esClient, fakeCDC{}, options...)
	require.NoError(t, err)
	t.Cleanup(b.batchTicker.Stop)
	return b
}

// newAckContext returns a listener context whose Ack records "ack:<id>".
func newAckContext(events *eventLog, id string) *replication.ListenerContext {
	return &replication.ListenerContext{
		Ack: func() error {
			events.record("ack:" + id)
			return nil
		},
	}
}

func indexActions(ids ...string) []elasticsearch2.Action {
	actions := make([]elasticsearch2.Action, 0, len(ids))
	for _, id := range ids {
		actions = append(actions, elasticsearch2.NewIndexAction([]byte(id), []byte(`{"id":"`+id+`"}`), nil))
	}
	return actions
}

// eventLog records bulk requests and acks in the order they happen.
type eventLog struct {
	entries []string
	mu      sync.Mutex
}

func (l *eventLog) record(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *eventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

func (l *eventLog) has(entry string) bool {
	for _, e := range l.list() {
		if e == entry {
			return true
		}
	}
	return false
}

// fakeCDC provides the cdc.Connector the bulk metrics register their counters with.
type fakeCDC struct{ cdc.Connector }

func (fakeCDC) SetMetricCollectors(...prometheus.Collector) {}

// recordingHandler records response handler callbacks per document ID.
type recordingHandler struct {
	successes map[string]int
	failures  map[string]int
	errs      map[string]error
	mu        sync.Mutex
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		successes: map[string]int{},
		failures:  map[string]int{},
		errs:      map[string]error{},
	}
}

func (h *recordingHandler) OnInit(*elasticsearch2.ResponseHandlerInitContext) {}

func (h *recordingHandler) OnSuccess(ctx *elasticsearch2.ResponseHandlerContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.successes[actionLabel(ctx.Action)]++
}

func (h *recordingHandler) OnError(ctx *elasticsearch2.ResponseHandlerContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failures[actionLabel(ctx.Action)]++
	h.errs[actionLabel(ctx.Action)] = ctx.Err
}

// actionLabel identifies an action in assertions: its ID, plus "@<routing>" when routed.
func actionLabel(a *elasticsearch2.Action) string {
	if a.Routing != nil {
		return string(a.ID) + "@" + *a.Routing
	}
	return string(a.ID)
}

// snapshot returns copies of the recorded callbacks.
func (h *recordingHandler) snapshot() (successes, failures map[string]int, errs map[string]error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.successes), maps.Clone(h.failures), maps.Clone(h.errs)
}

// fakeTransport answers Elasticsearch bulk requests in memory.
type fakeTransport struct {
	err      error                                // returned for every request when set
	failItem func(position int, doc bulkDoc) bool // answers matching items with an item-level error, when set
	log      *eventLog                            // records "request:<ids>" for every request when set
	entered  chan struct{}                        // signalled when a request arrives, when set
	release  chan struct{}                        // every request waits for it, when set
	body     string                               // raw 200 response body for every request, when set
	status   int                                  // response status for every request; zero answers per item
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	docs, err := parseBulkBody(req.Body)
	if err != nil {
		return nil, err
	}
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.release != nil {
		<-f.release
	}
	if f.log != nil {
		ids := make([]string, 0, len(docs))
		for _, doc := range docs {
			ids = append(ids, doc.id)
		}
		f.log.record("request:" + strings.Join(ids, ","))
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.status != 0 {
		return newJSONResponse(f.status, `{"error":{"type":"internal_server_error"}}`), nil
	}
	if f.body != "" {
		return newJSONResponse(http.StatusOK, f.body), nil
	}
	return newJSONResponse(http.StatusOK, f.itemsBody(docs)), nil
}

func (f *fakeTransport) itemsBody(docs []bulkDoc) string {
	items := make([]map[string]any, 0, len(docs))
	hasErrors := false
	for i, doc := range docs {
		// Like Elasticsearch, an item echoes _index and _id but not the routing.
		item := map[string]any{"_index": doc.index, "_id": doc.id, "status": http.StatusCreated}
		if f.failItem != nil && f.failItem(i, doc) {
			hasErrors = true
			item["status"] = http.StatusBadRequest
			item["error"] = map[string]any{"type": "mapper_parsing_exception", "reason": "failed to parse"}
		}
		items = append(items, map[string]any{doc.action: item})
	}
	body, _ := json.Marshal(map[string]any{"took": 1, "errors": hasErrors, "items": items})
	return string(body)
}

func newJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type":      []string{"application/json"},
			"X-Elastic-Product": []string{"Elasticsearch"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

// bulkDoc is the metadata of one action in a bulk request body.
type bulkDoc struct {
	action string
	index  string
	id     string
}

// parseBulkBody reads the action metadata lines of an NDJSON bulk body.
func parseBulkBody(body io.Reader) ([]bulkDoc, error) {
	if body == nil {
		return nil, nil
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	docs := make([]bulkDoc, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		var meta map[string]struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		}
		if err := json.Unmarshal([]byte(lines[i]), &meta); err != nil || len(meta) != 1 {
			return nil, fmt.Errorf("line %d is not an action line: %q", i, lines[i])
		}
		for action, m := range meta {
			docs = append(docs, bulkDoc{action: action, index: m.Index, id: m.ID})
			if action != "delete" {
				i++ // the source line follows
			}
		}
	}
	return docs, nil
}
