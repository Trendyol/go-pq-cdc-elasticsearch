package client

import (
	"encoding/json"

	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc/logger"
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

	if config.Elasticsearch.Version == "" {
		version, err := detectElasticsearchVersion(client)
		if err != nil {
			logger.Warn("elasticsearch version detection failed", "error", err, "fallback_version", "7.0.0", "hint", "specify 'elasticsearch.version' in config to set manually")
			config.Elasticsearch.Version = "7.0.0"
		} else {
			logger.Info("elasticsearch version detected", "version", version)
			config.Elasticsearch.Version = version
		}
	}

	return client, nil
}

func detectElasticsearchVersion(client *elasticsearch.Client) (string, error) {
	info, err := client.Info()
	if err != nil {
		return "", err
	}

	var response map[string]interface{}
	if err := json.NewDecoder(info.Body).Decode(&response); err != nil {
		return "", err
	}
	defer info.Body.Close()

	version, ok := response["version"].(map[string]interface{})
	if !ok {
		return "", errors.New("version info not found in Elasticsearch response")
	}

	number, ok := version["number"].(string)
	if !ok {
		return "", errors.New("version number not found in Elasticsearch response")
	}

	return number, nil
}
