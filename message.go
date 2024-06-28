package cdc

import (
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/elastic/go-elasticsearch/v7"
	"time"
)

type Message struct {
	ElasticsearchClient *elasticsearch.Client
	EventTime           time.Time
	TableName           string
	TableNamespace      string

	OldData map[string]any
	NewData map[string]any

	IsInsert bool
	IsUpdate bool
	IsDelete bool
}

func NewInsertMessage(esClient *elasticsearch.Client, m *format.Insert) Message {
	return Message{
		ElasticsearchClient: esClient,
		EventTime:           m.MessageTime,
		TableName:           m.TableName,
		TableNamespace:      m.TableNamespace,
		OldData:             nil,
		NewData:             m.Decoded,
		IsInsert:            true,
		IsUpdate:            false,
		IsDelete:            false,
	}
}

func NewUpdateMessage(esClient *elasticsearch.Client, m *format.Update) Message {
	return Message{
		ElasticsearchClient: esClient,
		EventTime:           m.MessageTime,
		TableName:           m.TableName,
		TableNamespace:      m.TableNamespace,
		OldData:             m.OldDecoded,
		NewData:             m.NewDecoded,
		IsInsert:            false,
		IsUpdate:            true,
		IsDelete:            false,
	}
}

func NewDeleteMessage(esClient *elasticsearch.Client, m *format.Delete) Message {
	return Message{
		ElasticsearchClient: esClient,
		EventTime:           m.MessageTime,
		TableName:           m.TableName,
		TableNamespace:      m.TableNamespace,
		OldData:             m.OldDecoded,
		NewData:             nil,
		IsInsert:            false,
		IsUpdate:            false,
		IsDelete:            true,
	}
}
