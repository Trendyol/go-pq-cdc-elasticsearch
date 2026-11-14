package bulk

import (
	gobytes "bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/elastic/go-elasticsearch/v7"

	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	elasticsearch2 "github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/internal/bytes"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/internal/slices"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/go-playground/errors"

	"golang.org/x/sync/errgroup"

	"github.com/elastic/go-elasticsearch/v7/esapi"

	jsoniter "github.com/json-iterator/go"
)

type Indexer interface {
	StartBulk()
	AddActions(
		ctx *replication.ListenerContext,
		eventTime time.Time,
		actions []elasticsearch2.Action,
		indexName string,
		isLastChunk bool,
	)
	// AckWhenFlushed acks a message that produced no actions once every
	// message delivered before it has been flushed to Elasticsearch.
	AckWhenFlushed(ctx *replication.ListenerContext)
	GetMetric() Metric
	Close()
}

type Bulk struct {
	metric              Metric
	responseHandler     elasticsearch2.ResponseHandler
	config              *config.Config
	batchKeys           map[string]int
	batchTicker         *time.Ticker
	isClosed            chan bool
	esClient            *elasticsearch.Client
	readers             []*bytes.MultiDimensionReader
	typeName            []byte
	batch               []BatchItem
	pendingAcks         []*replication.ListenerContext // acked in delivery order by the next flush
	batchIndex          int
	batchSize           int
	batchSizeLimit      int
	batchTickerDuration time.Duration
	batchByteSizeLimit  int
	batchByteSize       int
	concurrentRequest   int
	flushLock           sync.Mutex
}

type BatchItem struct {
	Action *elasticsearch2.Action
	Bytes  []byte
}

func NewBulk(
	config *config.Config,
	esClient *elasticsearch.Client,
	pqCDC cdc.Connector,
	options ...Option,
) (*Bulk, error) {
	readers := make([]*bytes.MultiDimensionReader, config.Elasticsearch.ConcurrentRequest)
	for i := 0; i < config.Elasticsearch.ConcurrentRequest; i++ {
		readers[i] = bytes.NewMultiDimReader(nil)
	}

	batchByteSizeLimit, err := bytes.ParseSize(config.Elasticsearch.BatchByteSizeLimit)
	if err != nil {
		return nil, errors.Wrap(err, "batch byte size limit")
	}

	bulk := &Bulk{
		batchTickerDuration: config.Elasticsearch.BatchTickerDuration,
		batchTicker:         time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		batchSizeLimit:      config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:  int(batchByteSizeLimit),
		isClosed:            make(chan bool, 1),
		esClient:            esClient,
		metric:              NewMetric(pqCDC, config.CDC.Slot.Name),
		config:              config,
		typeName:            []byte(config.Elasticsearch.TypeName),
		readers:             readers,
		concurrentRequest:   config.Elasticsearch.ConcurrentRequest,
		batchKeys:           make(map[string]int, config.Elasticsearch.BatchSizeLimit),
	}

	if config.Elasticsearch.TypeName == "" {
		bulk.typeName = nil
	}

	Options(options).Apply(bulk)

	return bulk, nil
}

func (b *Bulk) StartBulk() {
	go func() {
		for range b.batchTicker.C {
			b.flushMessages()
		}
	}()
}

func (b *Bulk) AddActions(
	ctx *replication.ListenerContext,
	eventTime time.Time,
	actions []elasticsearch2.Action,
	indexName string,
	isLastChunk bool,
) {
	b.flushLock.Lock()
	for i, action := range actions {
		indexName := b.getIndexName(indexName, action.IndexName)
		actions[i].IndexName = indexName
		value := getEsActionJSON(
			action.ID,
			action.Type,
			actions[i].IndexName,
			action.Routing,
			action.Source,
			b.typeName,
			b.config.Elasticsearch.Version,
		)

		b.metric.incrementOp(action.Type, indexName)

		key := getActionKey(actions[i])
		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batchByteSize += len(value) - len(b.batch[batchIndex].Bytes)
			b.batch[batchIndex] = BatchItem{
				Action: &actions[i],
				Bytes:  value,
			}
		} else {
			b.batch = append(b.batch, BatchItem{
				Action: &actions[i],
				Bytes:  value,
			})
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
			b.batchSize++
			b.batchByteSize += len(value)
		}
	}
	if isLastChunk {
		// A message is complete once its last chunk is in the batch.
		b.queueAck(ctx)
	}

	// Check if we need to flush while still holding the lock. Queued acks count
	// too: updates to one document collapse into a single action, but every
	// message stays referenced until the flush that acks it.
	shouldFlush := b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit ||
		len(b.pendingAcks) >= b.batchSizeLimit

	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.SetProcessLatency(time.Now().UTC().Sub(eventTime).Nanoseconds())
	}
	if shouldFlush {
		b.flushMessages()
	}
}

// AckWhenFlushed acks a message that produced no actions: a non-DML message,
// a table without an index mapping, or a handler that returned no actions.
// Acking it straight away could confirm a position past messages whose
// actions are still waiting in the batch, so it is queued behind them and
// acked by the flush that sends them. With nothing pending it is acked now.
// Queued acks count toward batchSizeLimit like actions do.
func (b *Bulk) AckWhenFlushed(ctx *replication.ListenerContext) {
	if b.ackOrQueue(ctx) {
		b.flushMessages()
	}
}

// ackOrQueue acks ctx now when nothing is waiting to be flushed and queues it
// otherwise. It reports whether the queue reached batchSizeLimit, in which case
// the caller flushes once the lock is released, as AddActions does.
func (b *Bulk) ackOrQueue(ctx *replication.ListenerContext) (shouldFlush bool) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.batch) == 0 && len(b.pendingAcks) == 0 {
		ack(ctx)
		return false
	}
	b.queueAck(ctx)
	return len(b.pendingAcks) >= b.batchSizeLimit
}

// queueAck appends ctx to the acks sent by the next flush, keeping delivery
// order. The caller must hold flushLock.
func (b *Bulk) queueAck(ctx *replication.ListenerContext) {
	if ctx == nil {
		return
	}
	b.pendingAcks = append(b.pendingAcks, ctx)
}

// ackPending acks every queued message in delivery order and releases them.
// The caller must hold flushLock.
func (b *Bulk) ackPending() {
	for i, ctx := range b.pendingAcks {
		ack(ctx)
		b.pendingAcks[i] = nil
	}
	b.pendingAcks = b.pendingAcks[:0]
}

func ack(ctx *replication.ListenerContext) {
	if ctx == nil || ctx.Ack == nil {
		return
	}
	if err := ctx.Ack(); err != nil {
		logger.Error("ack", "error", err)
	}
}

var (
	indexPrefix   = []byte(`{"index":{"_index":"`)
	deletePrefix  = []byte(`{"delete":{"_index":"`)
	updatePrefix  = []byte(`{"update":{"_index":"`)
	scriptPrefix  = []byte(`{"script":`)
	idPrefix      = []byte(`","_id":"`)
	typePrefix    = []byte(`","_type":"`)
	routingPrefix = []byte(`","routing":"`)
	postFix       = []byte(`"}}`)
	scriptPostfix = []byte(`,"scripted_upsert":true}`)
)

var metaPool = sync.Pool{
	New: func() interface{} {
		return []byte{}
	},
}

func isTypeSupported(version string) bool {
	if version == "" {
		return true
	}

	parts := strings.Split(version, ".")
	if len(parts) == 0 {
		return true
	}

	majorVersion := parts[0]
	return majorVersion < "8"
}

func getEsActionJSON(docID []byte, action elasticsearch2.ActionType, indexName string, routing *string, source []byte, typeName []byte, esVersion string) []byte {
	meta := metaPool.Get().([]byte)[:0]

	switch action {
	case elasticsearch2.Index:
		meta = append(meta, indexPrefix...)
	case elasticsearch2.Delete:
		meta = append(meta, deletePrefix...)
	case elasticsearch2.ScriptUpdate:
		meta = append(meta, updatePrefix...)
	}

	meta = append(meta, []byte(indexName)...)
	meta = append(meta, idPrefix...)
	meta = append(meta, bytes.EscapePredefinedBytes(docID)...)
	if routing != nil {
		meta = append(meta, routingPrefix...)
		meta = append(meta, []byte(*routing)...)
	}
	if typeName != nil && isTypeSupported(esVersion) {
		meta = append(meta, typePrefix...)
		meta = append(meta, typeName...)
	}
	meta = append(meta, postFix...)

	switch action {
	case elasticsearch2.Index:
		meta = append(meta, '\n')
		meta = append(meta, source...)
	case elasticsearch2.ScriptUpdate:
		meta = append(meta, '\n')
		meta = append(meta, scriptPrefix...)
		meta = append(meta, source...)
		meta = append(meta, scriptPostfix...)
	}
	meta = append(meta, '\n')
	return meta
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()
	b.flushMessages()
	close(b.isClosed)
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if len(b.batch) > 0 {
		err := b.bulkRequest()
		if err != nil && b.responseHandler == nil {
			panic(err)
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		for _, batch := range b.batch {
			//nolint:staticcheck
			metaPool.Put(batch.Bytes)
		}
		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
	}
	// Each queued message has its own actions, and those of every message
	// delivered before it, in the batch just sent (or had none), so its ack
	// can no longer overtake an unflushed message.
	b.ackPending()
}

func (b *Bulk) requestFunc(concurrentRequestIndex int, batchItems []BatchItem) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		reader.Reset(getBytes(batchItems))
		actions := getActions(batchItems)
		r, err := b.esClient.Bulk(reader)
		if err != nil {
			b.handleRequestError(actions, err)
			return err
		}
		errorData, err := hasResponseError(r, len(actions))
		if err != nil && errorData == nil {
			// The request failed as a whole (non-2xx status, a body that is not a
			// bulk response, or items that cannot be matched to the actions), so
			// no action in it can be reported as indexed.
			b.handleRequestError(actions, err)
			return err
		}
		b.handleResponse(actions, errorData)
		return err
	}
}

func (b *Bulk) bulkRequest() error {
	eg, _ := errgroup.WithContext(context.Background())

	chunks := slices.Chunk(b.batch, b.concurrentRequest)

	startedTime := time.Now()

	for i, chunk := range chunks {
		if len(chunk) > 0 {
			eg.Go(b.requestFunc(i, chunk))
		}
	}

	err := eg.Wait()

	b.metric.SetBulkRequestProcessLatency(time.Since(startedTime).Nanoseconds())

	return err
}

func (b *Bulk) GetMetric() Metric {
	return b.metric
}

// hasResponseError reads a bulk response. It returns the error text of every
// failed item, keyed by the position of its action in the request, or only an
// error when the request failed as a whole.
func hasResponseError(r *esapi.Response, actionCount int) (map[int]string, error) {
	if r == nil {
		return nil, fmt.Errorf("esapi response is nil")
	}
	if r.IsError() {
		return nil, fmt.Errorf("bulk request has error %v", r.String())
	}
	rb := new(gobytes.Buffer)

	defer r.Body.Close()
	_, err := rb.ReadFrom(r.Body)
	if err != nil {
		return nil, err
	}
	b := make(map[string]any)
	err = jsoniter.Unmarshal(rb.Bytes(), &b)
	if err != nil {
		return nil, err
	}
	hasError, ok := b["errors"].(bool)
	if !ok {
		// Not a bulk response (e.g. a proxy answering for Elasticsearch): no
		// action in the request can be reported as indexed.
		return nil, fmt.Errorf("bulk response errors field is %T, not a bool", b["errors"])
	}
	if !hasError {
		return nil, nil
	}
	return joinErrors(b, actionCount)
}

// joinErrors matches the items of a bulk response to the request's actions by
// position: Elasticsearch returns one item per action, in request order. When
// the items cannot be matched that way, the whole request is failed.
func joinErrors(body map[string]any, actionCount int) (map[int]string, error) {
	items, _ := body["items"].([]any)
	if len(items) != actionCount {
		return nil, fmt.Errorf("bulk response item count %d does not match action count %d", len(items), actionCount)
	}

	var sb strings.Builder
	itemErrors := make(map[int]string)
	sb.WriteString("bulk request has error. Errors will be listed below:\n")
	for i, item := range items {
		if !itemFailed(item) {
			continue
		}
		itemValue := fmt.Sprintf("%v\n", item)
		sb.WriteString(itemValue)
		itemErrors[i] = itemValue
	}
	return itemErrors, fmt.Errorf("%s", sb.String())
}

// itemFailed reports whether a bulk response item failed. An item that does not
// hold an operation result object cannot be read as a success.
func itemFailed(item any) bool {
	operations, ok := item.(map[string]any)
	if !ok || len(operations) == 0 {
		return true
	}
	for _, v := range operations {
		result, ok := v.(map[string]any)
		if !ok || result["error"] != nil {
			return true
		}
	}
	return false
}

func (b *Bulk) getIndexName(indexName, actionIndexName string) string {
	if actionIndexName != "" {
		return actionIndexName
	}

	if indexName == "" {
		panic(fmt.Sprintf("there is no index mapping for table: %s on your configuration", indexName))
	}

	return indexName
}

func (b *Bulk) handleResponse(batchActions []*elasticsearch2.Action, errs map[int]string) {
	if b.responseHandler == nil {
		return
	}

	for i, a := range batchActions {
		// errs is keyed by position: response items follow the request order.
		if itemErr, ok := errs[i]; ok {
			b.responseHandler.OnError(&elasticsearch2.ResponseHandlerContext{
				Action: a,
				Err:    fmt.Errorf("%s", itemErr),
			})
			continue
		}

		b.responseHandler.OnSuccess(&elasticsearch2.ResponseHandlerContext{
			Action: a,
		})
	}
}

// handleRequestError reports every action of a bulk request that failed as a
// whole (transport error, non-2xx response, or a response that cannot be read
// as a bulk result) to the response handler as an error: none of them can be
// assumed indexed.
func (b *Bulk) handleRequestError(batchActions []*elasticsearch2.Action, err error) {
	if b.responseHandler == nil {
		return
	}

	for _, a := range batchActions {
		b.responseHandler.OnError(&elasticsearch2.ResponseHandlerContext{
			Action: a,
			Err:    err,
		})
	}
}

func getActionKey(action elasticsearch2.Action) string {
	if action.Routing != nil {
		return fmt.Sprintf("%s:%s:%s", action.ID, action.IndexName, *action.Routing)
	}
	return fmt.Sprintf("%s:%s", action.ID, action.IndexName)
}

func getBytes(batchItems []BatchItem) [][]byte {
	batchBytes := make([][]byte, 0, len(batchItems))
	for _, batchItem := range batchItems {
		batchBytes = append(batchBytes, batchItem.Bytes)
	}
	return batchBytes
}

func getActions(batchItems []BatchItem) []*elasticsearch2.Action {
	batchActions := make([]*elasticsearch2.Action, 0, len(batchItems))
	for _, batchItem := range batchItems {
		batchActions = append(batchActions, batchItem.Action)
	}
	return batchActions
}
