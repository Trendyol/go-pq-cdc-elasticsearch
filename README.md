# go-pq-cdc-elasticsearch [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp.svg)](https://pkg.go.dev/github.com/Trendyol/go-pq-cdc-elasticsearch) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-pq-cdc-elasticsearch)](https://goreportcard.com/report/github.com/Trendyol/go-pq-cdc-elasticsearch) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-pq-cdc-elasticsearch/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-pq-cdc-elasticsearch)

Elasticsearch connector for [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc).

go-pq-cdc-elasticsearch streams documents from PostgreSql and writes to Elasticsearch index in near real-time.

### Contents

* [Usage](#usage)
* [Examples](#examples)
* [Availability](#availability)
* [Configuration](#configuration)
* [API](#api)
* [Exposed Metrics](#exposed-metrics)
* [Compatibility](#compatibility)
* [Breaking Changes](#breaking-changes)

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

