package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

/*
This example demonstrates the snapshot feature in action.

Snapshot Mode: "initial"
- Takes a snapshot of existing data (users and books tables)
- Then transitions to real-time CDC mode
- Ensures zero data loss between snapshot and CDC phases

The PostgreSQL database comes pre-populated with data via init.sql:
- 1000 users
- 500 books

All this data will be captured via snapshot first, then any new changes
will be captured via CDC.

Key Features Demonstrated:

 1. Snapshot vs CDC Message Detection:
    - Use msg.Type.IsSnapshot() to identify snapshot messages
    - Use msg.Type.IsInsert(), IsUpdate(), IsDelete() for CDC operations

 2. Elasticsearch Actions:
    - Snapshot data is indexed using NewIndexAction
    - All operations result in proper Elasticsearch bulk actions

 3. Message Body:
    - All column data included in the document
    - Proper ID extraction for document identification

This allows downstream systems to:
- Receive complete initial data load via snapshot
- Continue receiving real-time updates via CDC
- Maintain data consistency between PostgreSQL and Elasticsearch
*/
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()
	cfg := config.Config{
		CDC: cdcconfig.Config{
			Host:      "127.0.0.1",
			Username:  "es_cdc_user",
			Password:  "es_cdc_pass",
			Database:  "es_cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "es_cdc_publication_snapshot",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationTruncate,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "users",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
					publication.Table{
						Name:            "books",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "es_cdc_slot_snapshot",
				SlotActivityCheckerInterval: 3000,
			},
			// Snapshot configuration
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled:           true,
				Mode:              cdcconfig.SnapshotModeInitial, // Take snapshot only if no previous snapshot exists
				ChunkSize:         1000,                          // Process 1000 rows per chunk
				ClaimTimeout:      30 * time.Second,              // Reclaim timeout for stale chunks
				HeartbeatInterval: 5 * time.Second,               // Worker heartbeat interval
				// InstanceID is auto-generated if not specified
				// Tables field is optional - if not specified, all publication tables will be snapshotted
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelDebug,
			},
		},
		Elasticsearch: config.Elasticsearch{
			Username:                    "elastic",
			Password:                    "es_cdc_es_pass",
			BatchSizeLimit:              10000,
			BatchTickerDuration:         time.Millisecond * 100,
			DisableDiscoverNodesOnStart: true,
			TableIndexMapping: map[string]string{
				"public.users": "users",
				"public.books": "books",
			},
			TypeName: "_doc",
			URLs:     []string{"http://127.0.0.1:9200"},
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(msg cdc.Message) []elasticsearch.Action {
	tableName := msg.TableNamespace + "." + msg.TableName

	switch {
	case msg.Type.IsSnapshot():
		return handleSnapshot(msg, tableName)
	case msg.Type.IsInsert():
		return handleInsert(msg, tableName)
	case msg.Type.IsUpdate():
		return handleUpdate(msg, tableName)
	case msg.Type.IsDelete():
		return handleDelete(msg, tableName)
	}

	return []elasticsearch.Action{}
}

func handleSnapshot(msg cdc.Message, tableName string) []elasticsearch.Action {
	slog.Info("📸 snapshot data captured", "table", tableName, "type", "SNAPSHOT", "timestamp", msg.EventTime)
	return buildIndexAction(msg.NewData)
}

func handleInsert(msg cdc.Message, tableName string) []elasticsearch.Action {
	slog.Info("✨ insert captured", "table", tableName, "type", "INSERT", "timestamp", msg.EventTime)
	return buildIndexAction(msg.NewData)
}

func handleUpdate(msg cdc.Message, tableName string) []elasticsearch.Action {
	slog.Info("🔄 update captured", "table", tableName, "type", "UPDATE", "timestamp", msg.EventTime)
	return buildIndexAction(msg.NewData)
}

func handleDelete(msg cdc.Message, tableName string) []elasticsearch.Action {
	slog.Info("🗑️  delete captured", "table", tableName, "type", "DELETE", "timestamp", msg.EventTime)
	id := strconv.Itoa(int(msg.OldData["id"].(int32)))
	return []elasticsearch.Action{
		elasticsearch.NewDeleteAction([]byte(id), nil),
	}
}

func buildIndexAction(data map[string]any) []elasticsearch.Action {
	id := strconv.Itoa(int(data["id"].(int32)))
	payload, _ := json.Marshal(data)

	return []elasticsearch.Action{
		elasticsearch.NewIndexAction([]byte(id), payload, nil),
	}
}
