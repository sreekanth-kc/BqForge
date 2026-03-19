DATA_INGESTION = {
    "title": "Data Ingestion",
    "description": (
        "Best practices for loading data into BigQuery efficiently and reliably, "
        "covering batch loads, streaming, Storage Write API, file formats, "
        "deduplication, and DML patterns."
    ),
    "practices": [
        {
            "id": "DI-001",
            "title": "Use Storage Write API instead of legacy streaming inserts",
            "severity": "HIGH",
            "impact": "Cost & Reliability",
            "description": (
                "The legacy insertAll streaming API charges per row, does not support "
                "exactly-once semantics natively, and has lower throughput limits. "
                "The BigQuery Storage Write API supports committed, buffered, and "
                "pending stream modes with exactly-once delivery, higher throughput, "
                "and significantly lower cost (~10× cheaper for high-volume writes)."
            ),
            "do": [
                "Use COMMITTED stream for real-time writes with exactly-once guarantees",
                "Use PENDING stream for atomic batch commits (write → finalise → commit)",
                "Use DEFAULT stream for best-effort fire-and-forget at lower cost",
                "Batch rows into write requests of 1–10 MB for optimal throughput",
            ],
            "dont": [
                "Use legacy insertAll API for new high-volume streaming pipelines",
                "Send one row per Storage Write API request (creates excessive overhead)",
                "Mix PENDING and COMMITTED streams on the same table in the same job",
            ],
            "example": (
                "from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types\n\n"
                "client = BigQueryWriteClient()\n"
                "parent = client.table_path('project', 'dataset', 'table')\n"
                "write_stream = client.create_write_stream(\n"
                "    parent=parent,\n"
                "    write_stream=types.WriteStream(type_=types.WriteStream.Type.COMMITTED)\n"
                ")\n"
                "# Append rows to write_stream.name in batches"
            ),
        },
        {
            "id": "DI-002",
            "title": "Use columnar file formats for batch loads",
            "severity": "HIGH",
            "impact": "Cost & Performance",
            "description": (
                "BigQuery natively reads Parquet, Avro, and ORC without parsing overhead. "
                "These formats are compressed, self-describing, and allow column pruning "
                "at the load stage. Compared to CSV/JSON, they load faster, consume "
                "fewer slots, and preserve data types without schema inference errors."
            ),
            "do": [
                "Export from source systems in Parquet or Avro for BigQuery batch loads",
                "Use snappy or zstd compression for Parquet files stored in Cloud Storage",
                "Split large files into 256 MB–1 GB shards for parallel load throughput",
                "Use --source_format=PARQUET in bq load or sourceFormat in load job config",
            ],
            "dont": [
                "Load large datasets as CSV (no compression, no type info, parse errors are common)",
                "Create a single very large file — BigQuery parallelises across multiple files",
                "Use JSON Lines for schema-sensitive tables (implicit string coercion causes type mismatches)",
            ],
            "example": (
                "# Load Parquet from Cloud Storage\n"
                "bq load \\\n"
                "  --source_format=PARQUET \\\n"
                "  --parquet_enable_list_inference=true \\\n"
                "  project:dataset.table \\\n"
                "  'gs://bucket/data/events/dt=2024-06-01/*.parquet'"
            ),
        },
        {
            "id": "DI-003",
            "title": "Batch DML operations — avoid single-row mutations",
            "severity": "HIGH",
            "impact": "Performance & Cost",
            "description": (
                "BigQuery DML (INSERT, UPDATE, DELETE) triggers background compaction "
                "after each statement. Issuing thousands of single-row DML statements "
                "generates excessive mutation metadata and can exhaust the DML quota. "
                "Always batch mutations into bulk INSERT … SELECT or MERGE statements."
            ),
            "do": [
                "Collect rows in a staging table, then INSERT INTO target SELECT FROM staging",
                "Use MERGE for upsert patterns — one statement handles all inserts and updates",
                "Schedule bulk DML during off-peak hours for large UPDATE/DELETE operations",
                "Use TRUNCATE TABLE + INSERT instead of per-row DELETEs for full refreshes",
            ],
            "dont": [
                "Loop and INSERT one row at a time in application code",
                "Issue UPDATE WHERE id = @id in a tight loop for thousands of records",
                "Mix high-frequency DML with real-time queries on the same table",
            ],
            "example": (
                "-- BAD: loop that inserts one row at a time (avoid)\n"
                "-- for row in rows: bq.query('INSERT INTO t VALUES (...)')\n\n"
                "-- GOOD: stage all rows then bulk-insert\n"
                "INSERT INTO dataset.target_table (id, value, updated_at)\n"
                "SELECT id, value, updated_at\n"
                "FROM dataset.staging_table\n"
                "WHERE load_date = CURRENT_DATE()"
            ),
        },
        {
            "id": "DI-004",
            "title": "Use MERGE for upsert patterns instead of DELETE + INSERT",
            "severity": "MEDIUM",
            "impact": "Performance & Correctness",
            "description": (
                "A DELETE followed by INSERT is two full-table or partition-level DML "
                "operations that are not atomic. A MERGE statement performs both update "
                "and insert in a single pass, is atomic, and is easier to make idempotent. "
                "Limit the MERGE source to recent partitions to avoid full-table scans."
            ),
            "do": [
                "Restrict MERGE source to new/changed partitions using WHERE partition_col = @date",
                "Include a WHEN MATCHED AND source.updated_at > target.updated_at condition to avoid regressing",
                "Use MERGE as the final step of a scheduled query for daily/hourly refreshes",
            ],
            "dont": [
                "MERGE without a partition filter on multi-TB tables (full scan on every run)",
                "Use DELETE + INSERT as two separate pipeline steps (non-atomic, risk of partial writes)",
            ],
            "example": (
                "MERGE dataset.customers AS T\n"
                "USING (\n"
                "  SELECT customer_id, name, email, updated_at\n"
                "  FROM dataset.customers_staging\n"
                "  WHERE DATE(updated_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)\n"
                ") AS S\n"
                "ON T.customer_id = S.customer_id\n"
                "WHEN MATCHED AND S.updated_at > T.updated_at THEN\n"
                "  UPDATE SET name = S.name, email = S.email, updated_at = S.updated_at\n"
                "WHEN NOT MATCHED THEN\n"
                "  INSERT (customer_id, name, email, updated_at)\n"
                "  VALUES (S.customer_id, S.name, S.email, S.updated_at)"
            ),
        },
        {
            "id": "DI-005",
            "title": "Implement idempotent deduplication for streaming pipelines",
            "severity": "HIGH",
            "impact": "Data Quality & Reliability",
            "description": (
                "Streaming pipelines can deliver duplicate records during retries or "
                "failures. BigQuery's legacy insertId deduplication window is only "
                "1 minute and is best-effort. Use Storage Write API COMMITTED streams "
                "for exactly-once semantics, or implement downstream deduplication "
                "with ROW_NUMBER() in scheduled queries."
            ),
            "do": [
                "Use Storage Write API COMMITTED streams for guaranteed exactly-once delivery",
                "For best-effort streams, schedule a daily dedup query using ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)",
                "Add an ingestion_timestamp column and use it in dedup logic",
                "Use a deduplicated view as the canonical query interface",
            ],
            "dont": [
                "Rely solely on insertId for deduplication in high-throughput pipelines",
                "Build complex application-layer dedup without leveraging BQ's native capabilities",
            ],
            "example": (
                "-- Deduplicate by keeping the latest record per id\n"
                "CREATE OR REPLACE TABLE dataset.events_deduped AS\n"
                "SELECT * EXCEPT(row_num)\n"
                "FROM (\n"
                "  SELECT *,\n"
                "    ROW_NUMBER() OVER (\n"
                "      PARTITION BY event_id\n"
                "      ORDER BY ingestion_timestamp DESC\n"
                "    ) AS row_num\n"
                "  FROM dataset.events_raw\n"
                ")\n"
                "WHERE row_num = 1"
            ),
        },
        {
            "id": "DI-006",
            "title": "Validate and enforce schema on load",
            "severity": "MEDIUM",
            "impact": "Data Quality",
            "description": (
                "Schema mismatches during loads (wrong types, missing required columns, "
                "extra columns) silently corrupt data or cause load failures. Use "
                "schema autodetect only for exploration; enforce explicit schemas in "
                "production. Enable schema update options only intentionally."
            ),
            "do": [
                "Always specify an explicit schema JSON for production load jobs",
                "Use --schema_update_option=ALLOW_FIELD_ADDITION only when intentionally evolving schema",
                "Validate Parquet/Avro schema against BigQuery schema before loading via dry-run",
                "Run schema validation as a CI step using BigQuery Data Transfer Service or custom scripts",
            ],
            "dont": [
                "Use autodetect=True in production load pipelines (infers types from sample rows only)",
                "Allow ALLOW_FIELD_RELAXATION globally — it silently converts REQUIRED to NULLABLE",
                "Ignore INFORMATION_SCHEMA.LOAD_JOBS error details after a failed load",
            ],
            "example": (
                "from google.cloud import bigquery\n\n"
                "schema = [\n"
                "    bigquery.SchemaField('event_id', 'STRING', mode='REQUIRED'),\n"
                "    bigquery.SchemaField('event_ts', 'TIMESTAMP', mode='REQUIRED'),\n"
                "    bigquery.SchemaField('amount',   'NUMERIC',   mode='NULLABLE'),\n"
                "]\n"
                "job_config = bigquery.LoadJobConfig(\n"
                "    schema=schema,\n"
                "    source_format=bigquery.SourceFormat.PARQUET,\n"
                "    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,\n"
                ")\n"
                "client.load_table_from_uri('gs://bucket/data/*.parquet', table_ref, job_config=job_config)"
            ),
        },
    ],
}
