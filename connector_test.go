package cdc

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pqcdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch/bulk"
)

func TestListenerAcksMessagesWithoutActionsThroughBulk(t *testing.T) {
	tests := []struct {
		message any
		handler Handler
		name    string
	}{
		{name: "relation", message: &format.Relation{Namespace: "public", Name: "users"}, handler: indexUser},
		{name: "truncate", message: &format.Truncate{}, handler: indexUser},
		{name: "logical decoding message", message: &format.LogicalDecodingMessage{Prefix: "audit"}, handler: indexUser},
		{name: "unmapped table", message: &format.Insert{TableNamespace: "public", TableName: "orders"}, handler: indexUser},
		{name: "handler returns no actions", message: &format.Insert{TableNamespace: "public", TableName: "users"}, handler: noActions},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var acks []string
			indexer := &recordingIndexer{}
			ctx := countingContext(tt.message, tt.name, &acks)

			newTestConnector(indexer, tt.handler).listener(ctx)

			assert.Equal(t, []string{tt.name}, acks)
			assert.Equal(t, []*replication.ListenerContext{ctx}, indexer.acked)
			assert.Zero(t, indexer.addActions)
		})
	}
}

func TestListenerHandsActionsToBulkWithoutAcking(t *testing.T) {
	var acks []string
	indexer := &recordingIndexer{}
	ctx := countingContext(&format.Insert{TableNamespace: "public", TableName: "users"}, "insert", &acks)

	newTestConnector(indexer, indexUser).listener(ctx)

	assert.Equal(t, 1, indexer.addActions)
	assert.Empty(t, indexer.acked)
	assert.Empty(t, acks, "the bulk acks the message after flushing its actions")
}

func TestListenerAckWaitsForEarlierActionsInBulk(t *testing.T) {
	c := newTestConnector(nil, indexUser)
	esClient, err := es.NewClient(es.Config{
		Addresses:            []string{"http://elasticsearch.test:9200"},
		Transport:            acceptAllTransport{},
		DisableRetry:         true,
		UseResponseCheckOnly: true,
	})
	require.NoError(t, err)
	b, err := bulk.NewBulk(c.cfg, esClient, fakeCDC{})
	require.NoError(t, err)
	c.bulk = b

	var acks []string
	c.listener(countingContext(&format.Insert{TableNamespace: "public", TableName: "users"}, "insert", &acks))
	c.listener(countingContext(&format.Relation{Namespace: "public", Name: "orders"}, "relation", &acks))
	c.listener(countingContext(&format.Insert{TableNamespace: "public", TableName: "orders"}, "unmapped insert", &acks))

	assert.Empty(t, acks, "nothing may be acked before the pending insert is flushed")

	b.Close()

	assert.Equal(t, []string{"insert", "relation", "unmapped insert"}, acks)
}

func newTestConnector(indexer bulk.Indexer, handler Handler) *connector {
	return &connector{
		bulk:    indexer,
		handler: handler,
		cfg: &config.Config{Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      10,
			BatchTickerDuration: time.Hour,
			BatchByteSizeLimit:  "10mb",
			ConcurrentRequest:   1,
			Version:             "7.17.0",
			TableIndexMapping:   map[string]string{"public.users": "users"},
		}},
	}
}

func indexUser(Message) []elasticsearch.Action {
	return []elasticsearch.Action{elasticsearch.NewIndexAction([]byte("1"), []byte(`{"id":1}`), nil)}
}

func noActions(Message) []elasticsearch.Action { return nil }

// countingContext returns a listener context for message whose Ack appends name to acks.
func countingContext(message any, name string, acks *[]string) *replication.ListenerContext {
	return &replication.ListenerContext{
		Message: message,
		Ack: func() error {
			*acks = append(*acks, name)
			return nil
		},
	}
}

// recordingIndexer is a bulk.Indexer that records how the listener hands messages over.
type recordingIndexer struct {
	acked      []*replication.ListenerContext
	addActions int
}

func (r *recordingIndexer) StartBulk() {}

func (r *recordingIndexer) AddActions(*replication.ListenerContext, time.Time, []elasticsearch.Action, string, bool) {
	r.addActions++
}

// AckWhenFlushed acks at once, as the bulk does when nothing is waiting to be flushed.
func (r *recordingIndexer) AckWhenFlushed(ctx *replication.ListenerContext) {
	r.acked = append(r.acked, ctx)
	_ = ctx.Ack()
}

func (r *recordingIndexer) GetMetric() bulk.Metric { return nil }

func (r *recordingIndexer) Close() {}

// acceptAllTransport answers every bulk request as fully indexed.
type acceptAllTransport struct{}

func (acceptAllTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":      []string{"application/json"},
			"X-Elastic-Product": []string{"Elasticsearch"},
		},
		Body: io.NopCloser(strings.NewReader(`{"took":1,"errors":false,"items":[]}`)),
	}, nil
}

// fakeCDC provides the cdc.Connector the bulk metrics register their counters with.
type fakeCDC struct{ pqcdc.Connector }

func (fakeCDC) SetMetricCollectors(...prometheus.Collector) {}
