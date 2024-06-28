package cdc

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
)

type Handler func(msg Message) []elasticsearch.Action
