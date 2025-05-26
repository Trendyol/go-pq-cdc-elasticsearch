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
	psql "postgres://script_cdc_user:script_cdc_pass@127.0.0.1/script_cdc_db?replication=database"

	CREATE TABLE products (
		id serial PRIMARY KEY,
		name text NOT NULL,
		price decimal(10,2),
		stock integer,
		last_updated timestamptz
	);

	-- Insert sample data
	INSERT INTO products (name, price, stock, last_updated)
	VALUES
		('Product 1', 99.99, 100, NOW()),
		('Product 2', 149.99, 50, NOW()),
		('Product 3', 199.99, 25, NOW());

	-- Update stock using script
	UPDATE products SET stock = stock + 10 WHERE id = 1;

	-- Update price conditionally
	UPDATE products SET price = 89.99 WHERE id = 2;

	-- Update multiple fields
	UPDATE products SET stock = stock - 5, last_updated = NOW() WHERE id = 3;
*/

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()
	cfg := config.Config{
		CDC: cdcconfig.Config{
			Host:      "127.0.0.1",
			Username:  "script_cdc_user",
			Password:  "script_cdc_pass",
			Database:  "script_cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "script_cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationTruncate,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "products",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "script_cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
		},
		Elasticsearch: config.Elasticsearch{
			Username:            "elastic",
			Password:            "script_cdc_es_pass",
			BatchSizeLimit:      10000,
			BatchTickerDuration: time.Millisecond * 100,
			TableIndexMapping: map[string]string{
				"public.products": "products",
			},
			TypeName:                    "_doc",
			URLs:                        []string{"http://127.0.0.1:9200"},
			DisableDiscoverNodesOnStart: true,
			CompressionEnabled:          false,
			MaxConnsPerHost:             &[]int{10}[0],
			MaxIdleConnDuration:         &[]time.Duration{30 * time.Second}[0],
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
		// For inserts, we'll use a regular index action with initial version
		insertData := msg.NewData
		insertData["version"] = 1
		b, _ := json.Marshal(insertData)
		return []elasticsearch.Action{
			elasticsearch.NewIndexAction([]byte(strconv.Itoa(int(msg.NewData["id"].(int32)))), b, nil),
		}

	case cdc.DeleteMessage:
		// For deletes, we'll use a regular delete action
		return []elasticsearch.Action{
			elasticsearch.NewDeleteAction([]byte(strconv.Itoa(int(msg.OldData["id"].(int32)))), nil),
		}

	case cdc.UpdateMessage:
		id := strconv.Itoa(int(msg.NewData["id"].(int32)))

		if oldStock, newStock := msg.OldData["stock"], msg.NewData["stock"]; oldStock != newStock {
			script := elasticsearch.Script{
				Source: `
					if (ctx._source.version == null) {
						ctx._source.version = 1;
					} else {
						ctx._source.version += 1;
					}
					ctx._source.stock = params.new_stock;
				`,
				Params: map[string]interface{}{"new_stock": newStock},
			}
			return []elasticsearch.Action{
				elasticsearch.NewScriptUpdateAction([]byte(id), script, nil),
			}
		}

		if oldPrice, newPrice := msg.OldData["price"], msg.NewData["price"]; oldPrice != newPrice {
			script := elasticsearch.Script{
				Source: `
					if (ctx._source.version == null) {
						ctx._source.version = 1;
					} else {
						ctx._source.version += 1;
					}
					if (ctx._source.price != params.new_price) {
						ctx._source.price = params.new_price;
					}
				`,
				Params: map[string]interface{}{"new_price": newPrice},
			}
			return []elasticsearch.Action{
				elasticsearch.NewScriptUpdateAction([]byte(id), script, nil),
			}
		}

		script := elasticsearch.Script{
			Source: `
				if (ctx._source.version == null) {
					ctx._source.version = 1;
				} else {
					ctx._source.version += 1;
				}
				ctx._source.name = params.new_data.name;
				ctx._source.last_updated = params.new_data.last_updated;
			`,
			Params: map[string]interface{}{"new_data": msg.NewData},
		}
		return []elasticsearch.Action{
			elasticsearch.NewScriptUpdateAction([]byte(id), script, nil),
		}

	default:
		return nil
	}
}
