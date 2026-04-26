# Lazarus — Phase 2 Implementation

## Overview

Phase 2 extends the core sync pipeline with three operational capabilities: day-level Iceberg partitioning, a record restore API, and a compaction job. These are independent concerns and can be shipped incrementally.

---

## 1. Partitioning

### Configuration

Partition `SystemModstamp` by day using Iceberg's built-in transform. This prevents the small files problem that accumulates from 10-minute sync cadence without requiring any changes to the watermark logic.

```yaml
- name: Account
  primary_key: Id
  watermark_field: SystemModstamp
  partition_by:
    - field: SystemModstamp
      transform: day
  overflow: false
  removed_field_policy: tombstone
  exclude_fields: []
```

### Behavior

Each sync appends to the current day's partition. By end of day, a single partition contains ~144 small Parquet files. Compaction (see Section 3) consolidates these at day close.

```
s3://bucket/salesforce_leads_raw/
  day=2026-04-24/   ← compacted, single file
  day=2026-04-25/   ← active, ~144 files accumulating
```

### Guidance

| Transform | Use when |
|---|---|
| `day` | Default — 10-minute cadence, standard lead volume |
| `hour` | High volume orgs where day partitions exceed 1GB |

Start with `day`. Move to `hour` only if Athena query performance degrades on partition scans.

---

## 2. Watermark

### Strategy

The watermark is set to **job start time minus 5 minutes**, not `max(SystemModstamp)` from the result set. This protects against records whose `SystemModstamp` falls within the query window but were not yet visible to the Bulk API cursor at job execution time — a dirty read that would be permanently missed if the watermark ratcheted past them.

```
job_start          = 09:00:00
watermark_used     = 08:50:00   ← previous watermark from DynamoDB
job_completes      = 09:02:00

next_watermark     = job_start - 5m = 08:55:00   ← intentional overlap
```

The 5-minute lag ensures the next job re-queries a small overlap window, catching any records that were in flight during the previous job. Duplicate records pulled in the overlap are handled by upsert on `Id` at the Iceberg write layer.

### DynamoDB Record Shape

```json
{
  "object":          "Lead",
  "watermark":       "2026-04-25T08:55:00Z",
  "last_job_start":  "2026-04-25T09:00:00Z",
  "last_job_id":     "7503S000003XXXXX"
}
```

| Field | Purpose |
|---|---|
| `watermark` | Query lower bound for next job (`job_start - 5m`) |
| `last_job_start` | Actual job start time, used to compute next watermark |
| `last_job_id` | Bulk API job ID for auditability and debugging |

### Implementation

```go
// internal/watermark/watermark.go
const WatermarkBuffer = 5 * time.Minute

func (s *Store) Advance(ctx context.Context, object string, jobStart time.Time, jobID string) error {
    return s.put(ctx, object, Record{
        Object:       object,
        Watermark:    jobStart.Add(-WatermarkBuffer),
        LastJobStart: jobStart,
        LastJobID:    jobID,
    })
}
```

### Salesforce Query

```sql
SELECT Id, SystemModstamp, ...
FROM Lead
WHERE SystemModstamp > :watermark
ORDER BY SystemModstamp ASC
```

The overlap window means some records will be pulled twice per cycle. This is expected and cheap — deduplication is handled on write, not on read.

---

## 3. Restore

The raw append-only table is the source of truth for record-level restore. Because every sync result is written as a new row, the full state history of any record is queryable back to the retention window (default 60 days).

### Restore Modes

| Mode | Scope | Notes |
|---|---|---|
| Single record | One Salesforce ID | Surgical, low risk |
| Full object | All records of an object type | Use with caution, validate with preview first |
| Full object (with dependencies) | Object + related child records | Phase 2.1 — deferred |

### API Endpoints

```
POST /restore/preview    — dry run, returns payload without writing to Salesforce
POST /restore/execute    — async, returns job_id
GET  /restore/status/:id — poll Bulk API job status
```

### Request Shape

```json
{
  "object_type": "Lead",
  "record_id": "001C000000XyzABC",   // omit for full object restore
  "before": "2026-04-25T09:00:00Z",  // point-in-time anchor
  "bulk_size": 10000                  // optional, default 10000
}
```

### Query — Last Good State

```sql
-- Single record
SELECT *
FROM salesforce_leads_raw
WHERE id = '001C000000XyzABC'
  AND systemmodstamp < '2026-04-25T09:00:00Z'
ORDER BY systemmodstamp DESC
LIMIT 1

-- Full object
SELECT DISTINCT ON (id) *
FROM salesforce_leads_raw
WHERE systemmodstamp < '2026-04-25T09:00:00Z'
ORDER BY id, systemmodstamp DESC
```

### Restore Flow

```
1. POST /restore/preview
     └── Athena queries last good state
     └── Returns record payload for inspection

2. POST /restore/execute
     └── Athena queries last good state
     └── Lazarus pushes records via Bulk API 2.0 (upsert on Id)
     └── Returns { job_id }

3. GET /restore/status/:job_id
     └── Polls Salesforce Bulk API job status
     └── Returns { status, records_processed, errors }

4. Next sync cycle picks up restored records
     └── Appends restored state to raw table
     └── Restore itself becomes part of the audit trail
```

### Project Structure

```
cmd/
  api/              ← HTTP entrypoint
  sync/             ← existing sync loop
internal/
  athena/           ← query last good state, compaction
  salesforce/       ← bulk upsert client
  watermark/        ← dynamo read/write
  restore/          ← restore service, shared by cmd and api
server/
  handler.go
  middleware.go     ← auth, logging, panic recovery
  routes.go
```

### Deferred — Full Object with Dependencies (Phase 2.1)

Restoring a parent object (e.g. Account) without its child records (Contacts, Opportunities) risks referential inconsistency in Salesforce. This requires a dependency graph per object type and ordered restore execution. Deferred until the single-record and full-object paths are validated in production.

---

## 4. Compaction

### Problem

At 10-minute sync cadence, each day partition accumulates ~144 small Parquet files. Athena query performance degrades with small files — each file incurs metadata overhead on scan.

### Solution

Run an Iceberg `OPTIMIZE` job nightly against the previous day's partition. This bin-packs small files into a single efficient Parquet file without altering snapshot history or breaking time travel.

### Configuration

```yaml
compaction:
  enabled: true
  schedule: "0 23 * * *"   # 11pm daily
  lookback_days: 1          # compact yesterday's partition
```

### Athena Query

```sql
OPTIMIZE salesforce_leads_raw
REWRITE DATA USING BIN_PACK
WHERE day = DATE '2026-04-24'
```

### Implementation

```go
// internal/athena/compact.go
func (c *Client) Optimize(table string, date time.Time) error {
    query := fmt.Sprintf(`
        OPTIMIZE %s
        REWRITE DATA USING BIN_PACK
        WHERE day = DATE '%s'
    `, table, date.Format("2006-01-02"))

    return c.Execute(query)
}
```

```go
// cmd/compact/main.go
func main() {
    cfg := parseFlags() // --table, --date, --dry-run

    date := time.Now().AddDate(0, 0, -cfg.LookbackDays)

    if cfg.DryRun {
        log.Printf("would compact %s for %s", cfg.Table, date.Format("2006-01-02"))
        return
    }

    if err := athena.Optimize(cfg.Table, date); err != nil {
        log.Fatal(err)
    }
}
```

### Scheduling

Run as a scheduled ECS task via EventBridge, or as an internal cron within the Lazarus sync process if it is long-running.

| Option | Notes |
|---|---|
| EventBridge + ECS task | Preferred — decoupled, observable, retryable |
| Internal cron in sync loop | Simpler, one less infra piece |

### Important

Compaction rewrites Parquet files but preserves Iceberg snapshot history. Time travel and record restore remain fully intact after compaction runs.

---

## Retention

| Table | Retention | Rationale |
|---|---|---|
| `salesforce_leads_raw` | 60 days | Covers most incident discovery windows |
| `salesforce_leads_current` | Indefinite | Operational source of truth |

Enforce via S3 lifecycle rule on the raw prefix as a backstop to Iceberg snapshot expiry.

```sql
ALTER TABLE salesforce_leads_raw
SET TBLPROPERTIES (
  'history.expire.min-snapshots-to-keep' = '10',
  'history.expire.max-snapshot-age-ms'   = '5184000000'  -- 60 days
)
```

---

## Delivery Order

| Phase | Deliverable | Dependency |
|---|---|---|
| 2.0 | Day partitioning | None — config change only |
| 2.0 | Compaction job | Partitioning |
| 2.0 | Restore — single record | Raw table, Athena client |
| 2.0 | Restore — full object | Single record path |
| 2.1 | Restore — with dependencies | Object dependency graph |