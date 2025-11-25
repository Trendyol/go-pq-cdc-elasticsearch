# Snapshot Mode Example

This example demonstrates the **initial snapshot feature** of `go-pq-cdc-elasticsearch`. It shows how to capture existing data from PostgreSQL tables and index them into Elasticsearch before starting real-time CDC.

## What This Example Does

1. **Starts PostgreSQL** with pre-populated data (via `init.sql`):
   - 1,000 users
   - 500 books

2. **Takes Snapshot** (Snapshot Mode: `initial`):
   - Captures all existing data from `users` and `books` tables
   - Processes data in chunks (1,000 rows per chunk)
   - Indexes snapshot data into Elasticsearch

3. **Transitions to CDC Mode**:
   - After snapshot completes, seamlessly switches to real-time CDC
   - Captures any INSERT, UPDATE, DELETE operations
   - Ensures zero data loss between snapshot and CDC phases

## Prerequisites

- Docker and Docker Compose
- Go 1.22 or higher

## Running the Example

### 1. Start Infrastructure

Start PostgreSQL and Elasticsearch:

```bash
docker-compose up -d
```

Wait a few seconds for services to be ready. PostgreSQL will automatically initialize the database with sample data using `init.sql`.

### 2. Verify Data

You can verify the initial data:

```bash
# Connect to PostgreSQL
psql "postgres://es_cdc_user:es_cdc_pass@127.0.0.1/es_cdc_db"

# Check data
SELECT COUNT(*) FROM users;  -- Should return 1000
SELECT COUNT(*) FROM books;  -- Should return 500
```

### 3. Run the Connector

```bash
go run main.go
```

### 4. Observe the Snapshot Process

You'll see logs indicating:
- Snapshot initialization
- Chunk processing progress
- Total rows snapshotted
- Transition to CDC mode

Example log output:
```json
{"level":"INFO","msg":"📸 snapshot data captured","table":"public.users","type":"SNAPSHOT","timestamp":"2024-11-24T10:15:30Z"}
{"level":"INFO","msg":"📸 snapshot data captured","table":"public.books","type":"SNAPSHOT","timestamp":"2024-11-24T10:15:31Z"}
{"level":"INFO","msg":"Snapshot completed successfully","duration":"5.2s","total_rows":1500}
{"level":"INFO","msg":"✨ insert captured","table":"public.users","type":"INSERT","timestamp":"2024-11-24T10:16:00Z"}
```

**Notice the difference:**
- 📸 `SNAPSHOT` messages = Initial data capture (historical data)
- ✨ `INSERT` messages = Real-time changes (new data after snapshot)
- 🔄 `UPDATE` messages = Real-time updates
- 🗑️  `DELETE` messages = Real-time deletions

### 5. Check Elasticsearch Indices

Access Kibana at http://localhost:5601 (login: elastic / es_cdc_es_pass) to see the indexed documents:
- Index `users`: Contains all 1,000 users from snapshot
- Index `books`: Contains all 500 books from snapshot

Or use curl:

```bash
# Check users count
curl -u elastic:es_cdc_es_pass "http://localhost:9200/users/_count" | jq

# Check books count
curl -u elastic:es_cdc_es_pass "http://localhost:9200/books/_count" | jq

# Sample user document
curl -u elastic:es_cdc_es_pass "http://localhost:9200/users/_search?size=1" | jq
```

### 6. Test Real-Time CDC

Insert new data to see real-time CDC in action:

```bash
psql "postgres://es_cdc_user:es_cdc_pass@127.0.0.1/es_cdc_db"

-- Insert a new user
INSERT INTO users (name, email) VALUES ('New User', 'new@example.com');

-- Insert a new book
INSERT INTO books (title, author, isbn) VALUES ('New Book', 'New Author', 'ISBN-999');

-- Update a user
UPDATE users SET name = 'Updated User' WHERE id = 1;

-- Delete a book
DELETE FROM books WHERE id = 1;
```

These changes will be captured by CDC and indexed to Elasticsearch immediately.

## Monitoring

### Metrics

Access metrics at http://localhost:8081/metrics

Snapshot-specific metrics:
- `go_pq_cdc_snapshot_in_progress`: Whether snapshot is running
- `go_pq_cdc_snapshot_total_tables`: Number of tables being snapshotted
- `go_pq_cdc_snapshot_total_chunks`: Total chunks to process
- `go_pq_cdc_snapshot_completed_chunks`: Completed chunks
- `go_pq_cdc_snapshot_total_rows`: Total rows read during snapshot
- `go_pq_cdc_snapshot_duration_seconds`: Snapshot duration

### Health Check

```bash
curl http://localhost:8081/status
```

## Configuration Highlights

### Snapshot Configuration

```go
Snapshot: cdcconfig.SnapshotConfig{
    Enabled:           true,                          // Enable snapshot
    Mode:              cdcconfig.SnapshotModeInitial, // Take snapshot only if no previous snapshot exists
    ChunkSize:         1000,                          // Process 1000 rows per chunk
    ClaimTimeout:      30 * time.Second,              // Reclaim timeout for stale chunks
    HeartbeatInterval: 5 * time.Second,               // Worker heartbeat interval
}
```

### Handler Function - Distinguishing Snapshot from CDC

```go
func Handler(msg cdc.Message) []elasticsearch.Action {
    // Check if this is a snapshot message
    if msg.Type.IsSnapshot() {
        slog.Info("📸 snapshot data captured")
        // Index historical data from snapshot
        return []elasticsearch.Action{
            elasticsearch.NewIndexAction([]byte(id), payload, nil),
        }
    }

    // Handle real-time CDC operations
    if msg.Type.IsInsert() {
        slog.Info("✨ insert captured")
        // ...
    }
    if msg.Type.IsUpdate() {
        slog.Info("🔄 update captured")
        // ...
    }
    if msg.Type.IsDelete() {
        slog.Info("🗑️  delete captured")
        return []elasticsearch.Action{
            elasticsearch.NewDeleteAction([]byte(id), nil),
        }
    }
}
```

**Key Points:**
- Use `msg.Type.IsSnapshot()` to identify snapshot messages
- Use `msg.Type.IsInsert()`, `IsUpdate()`, `IsDelete()` for CDC operations
- Snapshot and Insert/Update use `NewIndexAction` for indexing
- Delete uses `NewDeleteAction` for removing documents

## Snapshot Modes

| Mode            | Description                                              |
|-----------------|----------------------------------------------------------|
| `initial`       | Take snapshot only if no previous snapshot exists, then start CDC (used in this example) |
| `never`         | Skip snapshot, start CDC immediately                     |
| `snapshot_only` | Take snapshot and exit (no CDC)                          |

## Cleanup

```bash
# Stop services
docker-compose down

# Remove volumes (optional, to reset data)
docker-compose down -v
```

## Key Differences from Simple Example

- ✅ **Pre-populated data**: Database starts with existing data via `init.sql`
- ✅ **Snapshot enabled**: Captures existing data before CDC
- ✅ **Zero data loss**: Ensures all data (historical + real-time) is captured
- ✅ **Chunk-based processing**: Memory-efficient processing of large datasets

## Learn More

- [go-pq-cdc Snapshot Documentation](https://github.com/Trendyol/go-pq-cdc#-new-snapshot-feature)
- [Main README](../../README.md)

