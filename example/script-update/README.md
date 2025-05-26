# Script Update Example

This example demonstrates how to use painless scripts and partial updates with go-pq-cdc-elasticsearch.

## Features Demonstrated

1. Script Updates
   - Simple field updates
   - Conditional updates
   - Using script parameters

2. Doc Updates
   - Partial document updates
   - Field-specific updates

## Prerequisites

- Docker and Docker Compose
- Go 1.20 or later

## Setup

1. Start the required services:

```bash
export STACK_VERSION=7.17.11
docker-compose up -d
```

2. Wait for all services to be healthy (check with `docker-compose ps`)

3. Create the database table and insert sample data:

```sql
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
```

## Running the Example

1. Build and run the example:

```bash
go mod tidy
go run main.go
```

2. Test different update scenarios:

```sql
-- Update stock using script
UPDATE products SET stock = stock + 10 WHERE id = 1;

-- Update price conditionally
UPDATE products SET price = 89.99 WHERE id = 2;

-- Update multiple fields
UPDATE products SET stock = stock - 5, last_updated = NOW() WHERE id = 3;
```

## Understanding the Code

The example demonstrates three types of updates:

1. **Script Updates for Stock Changes**:

```go
script := map[string]interface{}{
    "source": "ctx._source.stock = params.new_stock",
    "params": map[string]interface{}{
        "new_stock": newStock,
    },
}
```

2. **Conditional Script Updates for Price Changes**:

```go
script := map[string]interface{}{
    "source": "if (ctx._source.price != params.new_price) { ctx._source.price = params.new_price }",
    "params": map[string]interface{}{
        "new_price": newPrice,
    },
}
```

3. **Doc Updates for Other Fields**:

```go
updateData := map[string]interface{}{
    "name":         msg.NewData["name"],
    "last_updated": msg.NewData["last_updated"],
}
```

## Monitoring

- Check Elasticsearch indices: http://localhost:9200/products/_search
- View logs: `docker-compose logs -f`
- Monitor metrics: http://localhost:8081/metrics

## Cleanup

```bash
docker-compose down -v
``` 