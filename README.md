# Lazarus

Lazarus is a Salesforce data pipeline with two binaries:

- **`cmd/backfill`** — bulk-queries Salesforce objects via Bulk API 2.0 and writes them to append-only Apache Iceberg tables in S3 (catalogued in AWS Glue). Runs incrementally using a DynamoDB watermark; each sync appends new and updated records without overwriting history.

- **`cmd/restore`** — HTTP API that queries the raw Iceberg tables via Athena to find the last known state of any record before a given point in time, then pushes it back to Salesforce via Bulk API 2.0 upsert. Useful for rolling back accidental deletes or bad data loads.

Because every sync cycle appends rather than overwrites, the full state history of any record is queryable back to the retention window (default 60 days). The restore API exploits this to support both single-record and full-object point-in-time recovery.

## Prerequisites

- Go 1.25+
- AWS CLI

## AWS CLI setup

### Install

```bash
# macOS
brew install awscli

# or download directly
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o /tmp/AWSCLIV2.pkg
sudo installer -pkg /tmp/AWSCLIV2.pkg -target /
```

### Authenticate

If your organisation uses SSO (recommended):

```bash
aws configure sso
# follow the prompts: SSO start URL, region, account, role
aws sso login --profile your-profile-name
export AWS_PROFILE=your-profile-name
```

Or with a plain access key:

```bash
aws configure
# prompts for: AWS Access Key ID, AWS Secret Access Key, region, output format
```

Verify it works:

```bash
aws sts get-caller-identity
```

### Bootstrap AWS resources (once)

These three resources must exist before the first run. Running the commands again on an existing setup is harmless — they will return an error that can be ignored.

```bash
# S3 bucket for Iceberg data (choose any globally unique name)
aws s3 mb s3://your-bucket-name --region us-east-1

# DynamoDB table for watermark state
aws dynamodb create-table \
  --table-name lazarus-watermarks \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Glue database (namespace for Iceberg tables)
aws glue create-database --database-input '{"Name":"lazarus_dev"}'
```

## Local development

### 1. Configure environment

Edit `.env.local` and fill in `S3_BUCKET` plus your Salesforce Connected App credentials, then source it:

```bash
source .env.local
```

### 2. Run

**Dry-run a single object** — describes the SF schema and reports drift; nothing is written:

```bash
go run ./cmd/backfill --object Account --dry-run
```

**Full backfill of a single object:**

```bash
go run ./cmd/backfill --object Account
```

**Full backfill of all objects in `config/objects.yaml`:**

```bash
go run ./cmd/backfill
```

### 3. Inspect results

```bash
# files written to S3
aws s3 ls s3://your-bucket-name/iceberg/ --recursive

# watermarks persisted in DynamoDB
aws dynamodb scan --table-name lazarus-watermarks
```

### 4. Tear down (avoid recurring charges)

Only S3 storage costs money at rest. DynamoDB on-demand and Glue Catalog are both free at this scale, but deleting them is good hygiene.

```bash
# remove all Iceberg data and metadata from S3 (destructive — data is gone)
aws s3 rm s3://your-bucket-name --recursive
aws s3 rb s3://your-bucket-name

# delete the DynamoDB watermarks table
aws dynamodb delete-table --table-name lazarus-watermarks

# drop the Glue database (also deletes all table definitions inside it)
aws glue delete-database --name lazarus_dev
```

To verify nothing billable remains:

```bash
aws s3 ls                                          # bucket should be gone
aws dynamodb list-tables                           # lazarus-watermarks should not appear
aws glue get-databases                             # lazarus_dev should not appear
```

## Configuration

Objects are declared in `config/objects.yaml`. Each entry maps to one Salesforce object and one Iceberg table.

```yaml
objects:
  - name: Account
    primary_key: Id
    watermark_field: SystemModstamp   # incremental cursor
    partition_by: [SystemModstamp]
    overflow: false                   # spill unmapped fields to _overflow JSON column
    removed_field_policy: halt        # halt | tombstone
    exclude_fields: []
```

| Field | Description |
|---|---|
| `name` | Salesforce API object name |
| `primary_key` | Field used as the Iceberg row identifier |
| `watermark_field` | Field used for incremental reads (`WHERE field > last_value`) |
| `partition_by` | Iceberg partition columns |
| `overflow` | When `true`, unmapped fields are spilled to an `_overflow` JSON column instead of being dropped |
| `removed_field_policy` | `halt` stops the run if a column disappears from SF; `tombstone` keeps the column with nulls |
| `exclude_fields` | Fields to omit from the Iceberg table entirely |

## Flags

| Flag | Default | Description |
|---|---|---|
| `--registry` | `config/objects.yaml` | Path to the objects manifest |
| `--object` | *(all)* | Run a single named object |
| `--dry-run` | `false` | Describe SF schema and report drift; no data written |
| `--batch-size` | `5000` | Records per `WriteBatch` call |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--glue-database` | `$GLUE_DATABASE` | Glue database (namespace) for Iceberg tables |
| `--s3-bucket` | `$S3_BUCKET` | S3 bucket for Iceberg data and metadata |
| `--s3-prefix` | `$S3_KEY_PREFIX` | S3 key prefix for all tables |
| `--dynamo-table` | `$DYNAMO_TABLE` | DynamoDB table for watermark state |

## Production deployment

AWS credentials are picked up from the standard credential chain (instance profile, ECS task role, etc.). Set the following environment variables:

```bash
GLUE_DATABASE=my-glue-db
S3_BUCKET=my-iceberg-bucket
S3_KEY_PREFIX=iceberg/
DYNAMO_TABLE=lazarus-watermarks

SF_LOGIN_URL=https://login.salesforce.com
SF_CLIENT_ID=...
SF_CLIENT_SECRET=...
```

---

## Restore API (`cmd/restore`)

The restore server queries the raw Iceberg tables in Athena and writes records back to Salesforce via Bulk API 2.0.

### Bootstrap additional AWS resources (once)

```bash
# S3 prefix for Athena to write query results (reuse your existing bucket)
# No separate resource needed — just agree on a prefix, e.g. s3://your-bucket/athena-results/
```

### Run locally

```bash
source .env.local   # same Salesforce credentials as backfill

ATHENA_DATABASE=lazarus_dev \
ATHENA_OUTPUT_LOCATION=s3://your-bucket-name/athena-results/ \
go run ./cmd/restore
```

The server listens on `:8080` by default. Override with `--addr` or `$RESTORE_ADDR`.

### Flags

| Flag | Default | Description |
|---|---|---|
| `--addr` | `$RESTORE_ADDR` / `:8080` | HTTP listen address |
| `--athena-database` | `$ATHENA_DATABASE` | Glue/Athena database to query |
| `--athena-workgroup` | `$ATHENA_WORKGROUP` / `primary` | Athena workgroup |
| `--athena-output` | `$ATHENA_OUTPUT_LOCATION` | S3 URI for Athena result output |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |

### Endpoints

#### `POST /restore/preview`

Returns the last known state of record(s) before a given timestamp without writing anything to Salesforce.

```bash
curl -s -X POST http://localhost:8080/restore/preview \
  -H 'Content-Type: application/json' \
  -d '{
    "object_type": "Lead",
    "record_id":   "001C000000XyzABC",
    "before":      "2026-04-25T09:00:00Z"
  }' | jq .
```

Omit `record_id` to preview the last good state of every record in the object.

Response:
```json
{
  "records": [
    { "Id": "001C000000XyzABC", "FirstName": "Jane", "SystemModstamp": "2026-04-24T14:30:00Z", ... }
  ]
}
```

#### `POST /restore/execute`

Queries the same last good state and pushes it back to Salesforce via a Bulk API 2.0 upsert job. Returns a job ID for polling.

```bash
curl -s -X POST http://localhost:8080/restore/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "object_type": "Lead",
    "record_id":   "001C000000XyzABC",
    "before":      "2026-04-25T09:00:00Z"
  }' | jq .
```

Response:
```json
{ "job_id": "7503S000003XXXXX" }
```

#### `GET /restore/status/{job_id}`

Polls the Salesforce Bulk API job status. Call repeatedly until `status` is `JobComplete`, `Failed`, or `Aborted`.

```bash
curl -s http://localhost:8080/restore/status/7503S000003XXXXX | jq .
```

Response:
```json
{
  "status":            "JobComplete",
  "records_processed": 1,
  "errors":            []
}
```

### Typical restore workflow

```
1. Identify the point-in-time anchor (e.g. just before a bad data load ran)

2. Preview what would be restored:
   POST /restore/preview  { object_type, record_id, before }

3. If the payload looks correct, execute the restore:
   POST /restore/execute  { object_type, record_id, before }

4. Poll until the Bulk API job completes:
   GET  /restore/status/:job_id

5. The next backfill sync cycle appends the restored state to the raw table,
   making the restore itself part of the audit trail.
```
