package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	psql "postgres://es_cdc_user:es_cdc_pass@127.0.0.1/es_cdc_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	CREATE TABLE books (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;

	INSERT INTO books (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;
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
				Name:              "es_cdc_publication",
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
				Name:                        "es_cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
		},
		Elasticsearch: config.Elasticsearch{
			Username:            "elastic",
			Password:            "es_cdc_es_pass",
			BatchSizeLimit:      10000,
			BatchTickerDuration: time.Millisecond * 100,
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
	slog.Info("message received", "type", msg.Type, "msg", fmt.Sprintf("%#v", msg))
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
