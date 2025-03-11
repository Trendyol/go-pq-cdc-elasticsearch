package client

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-playground/errors"
)

func NewClient(config *config.Config) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:              config.Elasticsearch.Username,
		Password:              config.Elasticsearch.Password,
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

	r, err := client.Ping()
	if err != nil {
		return nil, err
	}

	if r.StatusCode == 401 {
		return nil, errors.New("unauthorized")
	}

	return client, nil
}
