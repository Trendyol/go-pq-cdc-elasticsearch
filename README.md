# go-pq-cdc-elasticsearch [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp.svg)](https://pkg.go.dev/github.com/Trendyol/go-pq-cdc-elasticsearch) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-pq-cdc-elasticsearch)](https://goreportcard.com/report/github.com/Trendyol/go-pq-cdc-elasticsearch) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-pq-cdc-elasticsearch/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-pq-cdc-elasticsearch)

Elasticsearch connector for [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc).

go-pq-cdc-elasticsearch streams documents from PostgreSql and writes to Elasticsearch index in near real-time.

### Contents

| Section | Description |
|---------|-------------|
| [Snapshot Feature](#-snapshot-feature) | Initial data synchronization before CDC |
| [Usage](#usage) | How to use the connector |
| [Examples](#examples) | Code examples |
| [Availability](#availability) | High availability setup |
| [Configuration](#configuration) | All configuration options |
| [API](#api) | REST API endpoints |
| [Exposed Metrics](#exposed-metrics) | Prometheus metrics |
| [Compatibility](#compatibility) | Version compatibility |
| [Breaking Changes](#breaking-changes) | Breaking changes log |

## 📸 Snapshot Feature

**Capture existing data before starting CDC!** The snapshot feature enables initial data synchronization, ensuring Elasticsearch receives both historical and real-time data.

✨ **Key Highlights:**

- **Zero Data Loss**: Consistent point-in-time snapshot using PostgreSQL's `pg_export_snapshot()`
- **Chunk-Based Processing**: Memory-efficient processing of large tables
- **Multi-Instance Support**: Parallel processing across multiple instances for faster snapshots
- **Crash Recovery**: Automatic resume from failures with chunk-level tracking
- **No Duplicates**: Seamless transition from snapshot to CDC mode
- **Flexible Modes**: Choose between `initial`, `never`, or `snapshot_only` based on your needs

### Snapshot Modes

| Mode            | Description                                                                                      | Use Case                                        |
|-----------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------|
| `initial`       | Takes snapshot only if no previous snapshot exists, then starts CDC                              | First-time setup with existing data             |
| `never`         | Skips snapshot entirely, starts CDC immediately                                                  | New tables or when historical data not needed   |
| `snapshot_only` | Takes snapshot and exits (no CDC, no replication slot required)                                  | One-time data migration or backfill             |

### How It Works

1. **Snapshot Phase**: Captures existing data in chunks for memory efficiency
2. **Consistent Point**: Uses PostgreSQL's `pg_export_snapshot()` to ensure data consistency
3. **CDC Phase**: Seamlessly transitions to real-time change data capture
4. **No Gaps**: Ensures all changes during snapshot are captured via CDC

### Identifying Snapshot vs CDC Messages

Your handler function can distinguish between snapshot and CDC messages:

```go
func Handler(msg cdc.Message) []elasticsearch.Action {
    // Check if this is a snapshot message (historical data)
    if msg.Type.IsSnapshot() {
        // Handle snapshot data - index to Elasticsearch
        id := strconv.Itoa(int(msg.NewData["id"].(int32)))
        data, _ := json.Marshal(msg.NewData)
        return []elasticsearch.Action{
            elasticsearch.NewIndexAction([]byte(id), data, nil),
        }
    }
    
    // Handle real-time CDC operations
    if msg.Type.IsInsert() { /* ... */ }
    if msg.Type.IsUpdate() { /* ... */ }
    if msg.Type.IsDelete() { /* ... */ }
}
```

For detailed configuration and usage, see the [snapshot example](./example/snapshot).

### Usage

> ### ⚠️ For production usage check the [production tutorial](./docs/production_tutorial.md) doc

> ### ⚠️ For other usages check the dockerfile and code at [examples](./example).

```sh
go get github.com/Trendyol/go-pq-cdc-elasticsearch
```

```go
package main

import (
	"context"
	"encoding/json"
	cdc "github.com/Trendyol/go-pq-cdc-elasticsearch"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"log/slog"
	"os"
	"strconv"
	"time"
)

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
				Name:              "es_cdc_publication",
				CreateIfNotExists: true,
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationTruncate,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{publication.Table{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{
				Name:                        "es_cdc_slot",
				CreateIfNotExists:           true,
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
		},
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      10000,
			BatchTickerDuration: time.Millisecond * 100,
			TableIndexMapping: map[string]string{
				"public.users": "users",
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
	switch msg.Type {
	case cdc.InsertMessage:
		b, _ := json.Marshal(msg.NewData)
		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(strconv.Itoa(int(msg.NewData["id"].(int32)))), b, nil),
		}
	case cdc.DeleteMessage:
		return []elasticsearch.Action{
			elasticsearch.NewDeleteAction([]byte(strconv.Itoa(int(msg.OldData["id"].(int32)))), nil),
		}
	case cdc.UpdateMessage:
		msg.NewData["old_name"] = msg.OldData["name"]
		b, _ := json.Marshal(msg.NewData)
		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(strconv.Itoa(int(msg.NewData["id"].(int32)))), b, nil),
		}
	default:
		return nil
	}
}


```

### Examples

* [Simple](./example/simple)
* [Snapshot](./example/snapshot)

### Availability

The go-pq-cdc operates in passive/active modes for PostgreSQL change data capture (CDC). Here's how it ensures
availability:

* **Active Mode:** When the PostgreSQL replication slot (slot.name) is active, go-pq-cdc continuously monitors changes
  and streams them to downstream systems as configured.
* **Passive Mode:** If the PostgreSQL replication slot becomes inactive (detected via slot.slotActivityCheckerInterval),
  go-pq-cdc automatically captures the slot again and resumes data capturing. Other deployments also monitor slot
  activity,
  and when detected as inactive, they initiate data capturing.

This setup ensures continuous data synchronization and minimal downtime in capturing database changes.

### Configuration

| Variable                                                                                     |       Type        |           Required            | Default | Description                                                                                           | Options                                                                                                                                                                  |
|----------------------------------------------------------------------------------------------|:-----------------:|:-----------------------------:|:-------:|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cdc.host`                                                                                   |      string       |              yes              |    -    | PostgreSQL host                                                                                       | Should be a valid hostname or IP address. Example: `localhost`.                                                                                                          |
| `cdc.username`                                                                               |      string       |              yes              |    -    | PostgreSQL username                                                                                   | Should have sufficient privileges to perform required database operations.                                                                                               |
| `cdc.password`                                                                               |      string       |              yes              |    -    | PostgreSQL password                                                                                   | Keep secure and avoid hardcoding in the source code.                                                                                                                     |
| `cdc.database`                                                                               |      string       |              yes              |    -    | PostgreSQL database                                                                                   | The database must exist and be accessible by the specified user.                                                                                                         |
| `cdc.debugMode`                                                                              |       bool        |              no               |  false  | For debugging purposes                                                                                | Enables pprof for trace.                                                                                                                                                 |
| `cdc.metric.port`                                                                            |        int        |              no               |  8080   | Set API port                                                                                          | Choose a port that is not in use by other applications.                                                                                                                  |
| `cdc.logger.logLevel`                                                                        |      string       |              no               |  info   | Set logging level                                                                                     | [`DEBUG`, `WARN`, `INFO`, `ERROR`]                                                                                                                                       |
| `cdc.logger.logger`                                                                          |      Logger       |              no               |  slog   | Set logger                                                                                            | Can be customized with other logging frameworks if `slog` is not used.                                                                                                   |
| `cdc.publication.createIfNotExists`                                                          |       bool        |              no               |    -    | Create publication if not exists. Otherwise, return `publication is not exists` error.                |                                                                                                                                                                          |
| `cdc.publication.name`                                                                       |      string       |              yes              |    -    | Set PostgreSQL publication name                                                                       | Should be unique within the database.                                                                                                                                    |
| `cdc.publication.operations`                                                                 |     []string      |              yes              |    -    | Set PostgreSQL publication operations. List of operations to track; all or a subset can be specified. | **INSERT:** Track insert operations. <br> **UPDATE:** Track update operations.  <br> **DELETE:** Track delete operations.                                                |
| `cdc.publication.tables`                                                                     |      []Table      |              yes              |    -    | Set tables which are tracked by data change capture                                                   | Define multiple tables as needed.                                                                                                                                        |
| `cdc.publication.tables[i].name`                                                             |      string       |              yes              |    -    | Set the data change captured table name                                                               | Must be a valid table name in the specified database.                                                                                                                    |
| `cdc.publication.tables[i].replicaIdentity`                                                  |      string       |              yes              |    -    | Set the data change captured table replica identity [`FULL`, `DEFAULT`]                               | **FULL:** Captures all columns of old row when a row is updated or deleted. <br> **DEFAULT:** Captures only the primary key of old row when a row is updated or deleted. |
| `publication.tables[i].schema`                                                               |      string       |              no               | public  | Set the data change captured table schema name                                                        | Must be a valid table name in the specified database.                                                                                                                    |
| `cdc.slot.createIfNotExists`                                                                 |       bool        |              no               |    -    | Create replication slot if not exists. Otherwise, return `replication slot is not exists` error.      |                                                                                                                                                                          |
| `cdc.slot.name`                                                                              |      string       |              yes              |    -    | Set the logical replication slot name                                                                 | Should be unique and descriptive.                                                                                                                                        |
| `cdc.slot.slotActivityCheckerInterval`                                                       |        int        |              yes              |  1000   | Set the slot activity check interval time in milliseconds                                             | Specify as an integer value in milliseconds (e.g., `1000` for 1 second).                                                                                                 |
| `cdc.snapshot.enabled`                                                                       |       bool        |              no               |  false  | Enable initial snapshot feature                                                                       | When enabled, captures existing data before starting CDC.                                                                                                                |
| `cdc.snapshot.mode`                                                                          |      string       |              no               |  never  | Snapshot mode: `initial`, `never`, or `snapshot_only`                                                 | **initial:** Take snapshot only if no previous snapshot exists, then start CDC. <br> **never:** Skip snapshot, start CDC immediately. <br> **snapshot_only:** Take snapshot and exit (no CDC). |
| `cdc.snapshot.chunkSize`                                                                     |       int64       |              no               |  8000   | Number of rows per chunk during snapshot                                                              | Adjust based on table size. Larger chunks = fewer chunks but more memory per chunk.                                                                                      |
| `cdc.snapshot.claimTimeout`                                                                  |   time.Duration   |              no               |   30s   | Timeout to reclaim stale chunks                                                                       | If a worker doesn't send heartbeat for this duration, chunk is reclaimed by another worker.                                                                              |
| `cdc.snapshot.heartbeatInterval`                                                             |   time.Duration   |              no               |    5s   | Interval for worker heartbeat updates                                                                 | Workers send heartbeat every N seconds to indicate they're processing a chunk.                                                                                           |
| `cdc.snapshot.instanceId`                                                                    |      string       |              no               |  auto   | Custom instance identifier (optional)                                                                 | Auto-generated as hostname-pid if not specified. Useful for tracking workers in multi-instance scenarios.                                                                |
| `cdc.snapshot.tables`                                                                        |      []Table      |              no*              |    -    | Tables to snapshot (required for `snapshot_only` mode, optional for `initial` mode)                   | **snapshot_only:** Must be specified here (independent from publication). <br> **initial:** If specified, must be a subset of publication tables. If not specified, all publication tables are snapshotted. |
| `elasticsearch.username`                                                                     |      string       | no (yes, if the auth enabled) |    -    | The username for authenticating to Elasticsearch.                                                     | Maps table names to Elasticsearch indices.                                                                                                                               |
| `elasticsearch.password`                                                                     |      string       | no (yes, if the auth enabled) |    -    | The password associated with the elasticsearch.username for authenticating to Elasticsearch.          | Maps table names to Elasticsearch indices.                                                                                                                               |
| `elasticsearch.tableIndexMapping`                                                            | map[string]string |              yes              |    -    | Mapping of PostgreSQL table events to Elasticsearch indices                                           | Maps table names to Elasticsearch indices.                                                                                                                               |
| `elasticsearch.urls`                                                                         |     []string      |              yes              |    -    | Elasticsearch connection URLs                                                                         | List of URLs to connect to Elasticsearch instances.                                                                                                                      |
| `elasticsearch.batchSizeLimit`                                                               |        int        |              no               |  1000   | Maximum message count per batch                                                                       | Flush is triggered if this limit is exceeded.                                                                                                                            |
| `elasticsearch.batchTickerDuration`                                                          |   time.Duration   |              no               | 10 sec  | Automatic batch flush interval                                                                        | Specify in a human-readable format, e.g., `10s` for 10 seconds.                                                                                                          |
| `elasticsearch.batchByteSizeLimit`                                                           |      string       |              no               |  10mb   | Maximum size (bytes) per batch                                                                        | Flush is triggered if this size is exceeded.                                                                                                                             |
| `elasticsearch.maxConnsPerHost`                                                              |        int        |              no               |   512   | Maximum connections per host                                                                          | Limits connections to each Elasticsearch host.                                                                                                                           |
| `elasticsearch.maxIdleConnDuration`                                                          |   time.Duration   |              no               | 10 sec  | Duration to keep idle connections alive                                                               | Specify in a human-readable format, e.g., `10s` for 10 seconds.                                                                                                          |
| `elasticsearch.typeName`                                                                     |      string       |              no               |    -    | Elasticsearch index type name                                                                         | Typically used in Elasticsearch for index types.                                                                                                                         |
| `elasticsearch.concurrentRequest`                                                            |        int        |              no               |    1    | Number of concurrent bulk requests                                                                    | Specify the count of bulk requests that can run concurrently.                                                                                                            |
| `elasticsearch.compressionEnabled`                                                           |       bool        |              no               |  false  | Enable compression for large messages                                                                 | Useful if message sizes are large, but may increase CPU usage.                                                                                                           |
| `elasticsearch.disableDiscoverNodesOnStart`                                                  |       bool        |              no               |  false  | Disable node discovery on client initialization                                                       | Skips node discovery when the client starts.                                                                                                                             |
| `elasticsearch.discoverNodesInterval`                                                        |   time.Duration   |              no               |  5 min  | Periodic node discovery interval                                                                      | Specify in a human-readable format, e.g., `5m` for 5 minutes.                                                                                                            |
| `elasticsearch.version`                                                                      |      string       |              no               |    -    | Elasticsearch version to determine compatibility features                                             | Used to handle version-specific behaviors, such as `_type` parameter support (removed in ES 8.0+). If not specified, version is automatically detected from the cluster. |

### API

| Endpoint             | Description                                                                               |
|----------------------|-------------------------------------------------------------------------------------------|
| `GET /status`        | Returns a 200 OK status if the client is able to ping the PostgreSQL server successfully. |
| `GET /metrics`       | Prometheus metric endpoint.                                                               |
| `GET /debug/pprof/*` | (Only for `debugMode=true`) [pprof](https://pkg.go.dev/net/http/pprof)                    |

### Exposed Metrics

The client collects relevant metrics related to PostgreSQL change data capture (CDC) and makes them available at
the `/metrics` endpoint.

| Metric Name                                                  | Description                                                                     | Labels                      | Value Type |
|--------------------------------------------------------------|---------------------------------------------------------------------------------|-----------------------------|------------|
| go_pq_cdc_elasticsearch_process_latency_current              | The latest elasticsearch connector process latency in nanoseconds.              | slot_name, host             | Gauge      |
| go_pq_cdc_elasticsearch_bulk_request_process_latency_current | The latest elasticsearch connector bulk request process latency in nanoseconds. | slot_name, host             | Gauge      |
| go_pq_cdc_elasticsearch_index_total                          | Total number of index operation.                                                | slot_name, host, index_name | Counter    |
| go_pq_cdc_elasticsearch_delete_total                         | Total number of delete operation.                                               | slot_name, host, index_name | Counter    |

### Snapshot Metrics

| Metric Name                                                  | Description                                                                                           | Labels                      | Value Type   |
|--------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------|--------------|
| go_pq_cdc_snapshot_in_progress                               | Indicates whether snapshot is currently in progress (1 for active, 0 for inactive).                  | slot_name, host             | Gauge        |
| go_pq_cdc_snapshot_total_tables                              | Total number of tables to snapshot.                                                                   | slot_name, host             | Gauge        |
| go_pq_cdc_snapshot_total_chunks                              | Total number of chunks to process across all tables.                                                  | slot_name, host             | Gauge        |
| go_pq_cdc_snapshot_completed_chunks                          | Number of chunks completed in snapshot.                                                               | slot_name, host             | Gauge        |
| go_pq_cdc_snapshot_total_rows                                | Total number of rows read during snapshot.                                                            | slot_name, host             | Counter      |
| go_pq_cdc_snapshot_duration_seconds                          | Duration of the last snapshot operation in seconds.                                                   | slot_name, host             | Gauge        |

You can also use all cdc related metrics explained [here](https://github.com/Trendyol/go-pq-cdc#exposed-metrics).
All cdc related metrics are automatically injected. It means you don't need to do anything.

### Compatibility

| go-pq-cdc Version | Minimum PostgreSQL Server Version |
|-------------------|-----------------------------------|
| 0.0.2 or higher   | 14                                |

### Elasticsearch Version Compatibility

The connector supports different versions of Elasticsearch through the `elasticsearch.version` configuration parameter:

| Elasticsearch Version | Type Parameter Behavior                                 |
|-----------------------|--------------------------------------------------------|
| Below 8.0             | `_type` parameter is included in the index requests     |
| 8.0 and above         | `_type` parameter is automatically omitted              |

If no version is specified, the connector will automatically detect the Elasticsearch cluster version by querying the Info API after connection. This eliminates the need to manually configure the version.

### Breaking Changes

| Date taking effect | Version | Change | How to check |
|--------------------|---------|--------|--------------|
| -                  | -       | -      | -            |

