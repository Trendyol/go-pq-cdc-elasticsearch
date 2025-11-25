package config

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
)

type Elasticsearch struct {
	Username                    string            `yaml:"username"`
	Password                    string            `yaml:"password"`
	BatchByteSizeLimit          string            `yaml:"batchByteSizeLimit"`
	TableIndexMapping           map[string]string `yaml:"tableIndexMapping"`
	MaxConnsPerHost             *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration         *time.Duration    `yaml:"maxIdleConnDuration"`
	DiscoverNodesInterval       *time.Duration    `yaml:"discoverNodesInterval"`
	TypeName                    string            `yaml:"typeName"`
	Version                     string            `yaml:"version"`
	URLs                        []string          `yaml:"urls"`
	BatchSizeLimit              int               `yaml:"batchSizeLimit"`
	BatchTickerDuration         time.Duration     `yaml:"batchTickerDuration"`
	ConcurrentRequest           int               `yaml:"concurrentRequest"`
	CompressionEnabled          bool              `yaml:"compressionEnabled"`
	DisableDiscoverNodesOnStart bool              `yaml:"disableDiscoverNodesOnStart"`
}

type RejectionLog struct {
	Index         string `yaml:"index"`
	IncludeSource bool   `yaml:"includeSource"`
}

type Config struct {
	Elasticsearch Elasticsearch
	CDC           config.Config
}

func (c *Config) SetDefault() {
	if c.Elasticsearch.BatchTickerDuration == 0 {
		c.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}

	if c.Elasticsearch.BatchSizeLimit == 0 {
		c.Elasticsearch.BatchSizeLimit = 1000
	}

	if c.Elasticsearch.BatchByteSizeLimit == "" {
		c.Elasticsearch.BatchByteSizeLimit = "10mb"
	}

	if c.Elasticsearch.ConcurrentRequest == 0 {
		c.Elasticsearch.ConcurrentRequest = 1
	}

	if c.Elasticsearch.DiscoverNodesInterval == nil {
		duration := 5 * time.Minute
		c.Elasticsearch.DiscoverNodesInterval = &duration
	}
}
