package cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch/bulk"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/internal/slices"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	partitionCache  sync.Map
	handler         Handler
	responseHandler elasticsearch.ResponseHandler
	cfg             *config.Config
	esClient        *es.Client
	cdc             cdc.Connector
	bulk            bulk.Indexer
	metrics         []prometheus.Collector
}

func NewConnector(ctx context.Context, cfg config.Config, handler Handler, options ...Option) (Connector, error) {
	cfg.SetDefault()

	esConnector := &connector{
		cfg:     &cfg,
		handler: handler,
	}

	Options(options).Apply(esConnector)

	pqCDC, err := cdc.NewConnector(ctx, esConnector.cfg.CDC, esConnector.listener)
	if err != nil {
		return nil, err
	}
	esConnector.cdc = pqCDC
	esConnector.cfg.CDC = *pqCDC.GetConfig()

	esClient, err := client.NewClient(esConnector.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "elasticsearch new client")
	}
	esConnector.esClient = esClient

	esConnector.bulk, err = bulk.NewBulk(
		esConnector.cfg,
		esClient,
		pqCDC,
		bulk.WithResponseHandler(esConnector.responseHandler),
	)
	if err != nil {
		return nil, errors.Wrap(err, "elasticsearch new bulk")
	}
	pqCDC.SetMetricCollectors(esConnector.bulk.GetMetric().PrometheusCollectors()...)
	pqCDC.SetMetricCollectors(esConnector.metrics...)

	return esConnector, nil
}

func (c *connector) Start(ctx context.Context) {
	go func() {
		logger.Info("waiting for connector start...")
		if err := c.cdc.WaitUntilReady(ctx); err != nil {
			panic(err)
		}
		logger.Info("bulk process started")
		c.bulk.StartBulk()
	}()
	c.cdc.Start(ctx)
}

func (c *connector) Close() {
	c.cdc.Close()
	c.bulk.Close()
}

func (c *connector) listener(ctx *replication.ListenerContext) {
	var msg Message
	switch m := ctx.Message.(type) {
	case *format.Insert:
		msg = NewInsertMessage(c.esClient, m)
	case *format.Update:
		msg = NewUpdateMessage(c.esClient, m)
	case *format.Delete:
		msg = NewDeleteMessage(c.esClient, m)
	default:
		return
	}

	fullTableName := c.getFullTableName(msg.TableNamespace, msg.TableName)

	indexName := c.resolveTableToIndexName(fullTableName, msg.TableNamespace, msg.TableName)
	if indexName == "" {
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}

	actions := c.handler(msg)
	if len(actions) == 0 {
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}

	batchSizeLimit := c.cfg.Elasticsearch.BatchSizeLimit
	if len(actions) > batchSizeLimit {
		chunks := slices.ChunkWithSize[elasticsearch.Action](actions, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.bulk.AddActions(ctx, msg.EventTime, chunk, indexName, idx == lastChunkIndex)
		}
	} else {
		c.bulk.AddActions(ctx, msg.EventTime, actions, indexName, true)
	}
}

func (c *connector) resolveTableToIndexName(fullTableName, tableNamespace, tableName string) string {
	tableIndexMapping := c.cfg.Elasticsearch.TableIndexMapping
	if len(tableIndexMapping) == 0 {
		return ""
	}

	if indexName, exists := tableIndexMapping[fullTableName]; exists {
		return indexName
	}

	if t, ok := timescaledb.HyperTables.Load(fullTableName); ok {
		parentName := t.(string)
		if indexName, exists := tableIndexMapping[parentName]; exists {
			return indexName
		}
	}

	parentTableName := c.getParentTableName(fullTableName, tableNamespace, tableName)
	if parentTableName != "" {
		if indexName, exists := tableIndexMapping[parentTableName]; exists {
			return indexName
		}
	}

	return ""
}

func (c *connector) getParentTableName(fullTableName, tableNamespace, tableName string) string {
	if cachedValue, found := c.partitionCache.Load(fullTableName); found {
		parentName, ok := cachedValue.(string)
		if !ok {
			logger.Error("invalid cache value type for table", "table", fullTableName)
			return ""
		}

		if parentName != "" {
			logger.Debug("matched partition table to parent from cache",
				"partition", fullTableName,
				"parent", parentName)
		}
		return parentName
	}

	parentTableName := c.findParentTable(tableNamespace, tableName)
	c.partitionCache.Store(fullTableName, parentTableName)

	if parentTableName != "" {
		logger.Debug("matched partition table to parent",
			"partition", fullTableName,
			"parent", parentTableName)
	}

	return parentTableName
}

func (c *connector) getFullTableName(tableNamespace, tableName string) string {
	return fmt.Sprintf("%s.%s", tableNamespace, tableName)
}

func (c *connector) findParentTable(tableNamespace, tableName string) string {
	tableParts := strings.Split(tableName, "_")
	if len(tableParts) <= 1 {
		return ""
	}

	for i := 1; i < len(tableParts); i++ {
		parentNameCandidate := strings.Join(tableParts[:i], "_")
		fullParentName := c.getFullTableName(tableNamespace, parentNameCandidate)

		if _, exists := c.cfg.Elasticsearch.TableIndexMapping[fullParentName]; exists {
			return fullParentName
		}
	}

	return ""
}
