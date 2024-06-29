package client

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/elastic/go-elasticsearch/v8"
)

func NewClient(config *config.Config) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		MaxRetries:            5,
		Addresses:             config.Elasticsearch.URLs,
		Transport:             NewTransport(config.Elasticsearch),
		CompressRequestBody:   config.Elasticsearch.CompressionEnabled,
		DiscoverNodesOnStart:  !config.Elasticsearch.DisableDiscoverNodesOnStart,
		DiscoverNodesInterval: *config.Elasticsearch.DiscoverNodesInterval,
		Logger:                &LoggerAdapter{},
	})
	if err != nil {
		return nil, err
	}

	if _, err = client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}
