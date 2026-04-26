// Package iceberg manages the Apache Iceberg table layer for the backfill pipeline.
//
// Call order for a single object:
//
//  1. LoadRegistry → reg.All() to enumerate configured objects.
//  2. ColumnsFromDescribe to map Salesforce fields to Iceberg columns.
//  3. SchemaManager.Prepare to absorb drift and partition mapped/overflow fields.
//  4. Writer.EnsureTable to bootstrap or verify the Glue-backed Iceberg table.
//  5. Writer.EvolveSchema when new columns are detected.
//  6. Writer.WriteBatch for each fixed-size batch of encoded records.
package iceberg
