package elasticsearch

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/elastic/go-elasticsearch/v7"

	"math"
)

func NewElasticClient(config *config.Config) (*elasticsearch.Client, error) {
	return elasticsearch.NewClient(elasticsearch.Config{
		MaxRetries:            math.MaxInt,
		Addresses:             config.Elasticsearch.URLs,
		Transport:             newTransport(config.Elasticsearch),
		CompressRequestBody:   config.Elasticsearch.CompressionEnabled,
		DiscoverNodesOnStart:  !config.Elasticsearch.DisableDiscoverNodesOnStart,
		DiscoverNodesInterval: *config.Elasticsearch.DiscoverNodesInterval,
		Logger:                &LoggerAdapter{},
	})
}
