package cdc

import (
	"time"

	"github.com/elastic/go-elasticsearch/v7"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

type Message struct {
	ElasticsearchClient *elasticsearch.Client
	EventTime           time.Time
	TableName           string
	TableNamespace      string

	OldData map[string]any
	NewData map[string]any

	Type MessageType

	// LSN is the WAL position of the change and increases for a given row.
	// Snapshot events carry the LSN the snapshot was taken at, shared by every snapshot row.
	LSN pq.LSN

	// TransactionID is the top-level PostgreSQL transaction that produced the change,
	// zero for snapshot events. It is 32-bit and wraps around: it groups changes, LSN orders them.
	TransactionID uint32
}

func NewInsertMessage(esClient *elasticsearch.Client, m *format.Insert) Message {
	return Message{
		ElasticsearchClient: esClient,
		EventTime:           m.MessageTime,
		TableName:           m.TableName,
		TableNamespace:      m.TableNamespace,
		OldData:             nil,
		NewData:             m.Decoded,
		Type:                InsertMessage,
		LSN:                 m.LSN,
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
		Type:                UpdateMessage,
		LSN:                 m.LSN,
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
		Type:                DeleteMessage,
		LSN:                 m.LSN,
	}
}

func NewSnapshotMessage(esClient *elasticsearch.Client, m *format.Snapshot) Message {
	return Message{
		ElasticsearchClient: esClient,
		EventTime:           m.ServerTime,
		TableName:           m.Table,
		TableNamespace:      m.Schema,
		OldData:             nil,
		NewData:             m.Data,
		Type:                SnapshotMessage,
		LSN:                 m.LSN,
	}
}

type MessageType string

const (
	InsertMessage   MessageType = "INSERT"
	UpdateMessage   MessageType = "UPDATE"
	DeleteMessage   MessageType = "DELETE"
	SnapshotMessage MessageType = "SNAPSHOT"
)

func (m MessageType) IsInsert() bool   { return m == InsertMessage }
func (m MessageType) IsUpdate() bool   { return m == UpdateMessage }
func (m MessageType) IsDelete() bool   { return m == DeleteMessage }
func (m MessageType) IsSnapshot() bool { return m == SnapshotMessage }
