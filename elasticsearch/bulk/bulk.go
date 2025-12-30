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
	lastAckCtx          *replication.ListenerContext
	readers             []*bytes.MultiDimensionReader
	typeName            []byte
	batch               []BatchItem
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
		b.lastAckCtx = ctx
	}

	// Check if we need to flush while still holding the lock
	shouldFlush := b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit

	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.SetProcessLatency(time.Now().UTC().Sub(eventTime).Nanoseconds())
	}
	if shouldFlush {
		b.flushMessages()
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
		if b.lastAckCtx != nil {
			if err = b.lastAckCtx.Ack(); err != nil {
				logger.Error("ack", "error", err)
			}
			b.lastAckCtx = nil
		}
	}
}

func (b *Bulk) requestFunc(concurrentRequestIndex int, batchItems []BatchItem) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		reader.Reset(getBytes(batchItems))
		r, err := b.esClient.Bulk(reader)
		if err != nil {
			return err
		}
		errorData, err := hasResponseError(r)
		b.handleResponse(getActions(batchItems), errorData)
		if err != nil {
			return err
		}
		return nil
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

func hasResponseError(r *esapi.Response) (map[string]string, error) {
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
	if !ok || !hasError {
		return nil, nil
	}
	return joinErrors(b)
}

func joinErrors(body map[string]any) (map[string]string, error) {
	var sb strings.Builder
	ivd := make(map[string]string)
	sb.WriteString("bulk request has error. Errors will be listed below:\n")

	items, ok := body["items"].([]any)
	if !ok {
		return nil, nil
	}

	for _, i := range items {
		item, ok := i.(map[string]any)
		if !ok {
			continue
		}

		for _, v := range item {
			iv, ok := v.(map[string]any)
			if !ok {
				continue
			}

			if iv["error"] != nil {
				itemValue := fmt.Sprintf("%v\n", i)
				sb.WriteString(itemValue)
				itemValueDataKey := fmt.Sprintf("%s:%s", iv["_id"].(string), iv["_index"].(string))
				ivd[itemValueDataKey] = itemValue
			}
		}
	}
	return ivd, fmt.Errorf(sb.String())
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

func (b *Bulk) handleResponse(batchActions []*elasticsearch2.Action, errs map[string]string) {
	if b.responseHandler == nil {
		return
	}

	for _, a := range batchActions {
		key := getActionKey(*a)
		if _, ok := errs[key]; ok {
			b.responseHandler.OnError(&elasticsearch2.ResponseHandlerContext{
				Action: a,
				Err:    fmt.Errorf(errs[key]),
			})
			continue
		}

		b.responseHandler.OnSuccess(&elasticsearch2.ResponseHandlerContext{
			Action: a,
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
