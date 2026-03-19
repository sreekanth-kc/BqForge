SCHEMA_DESIGN = {
    "title": "Schema Design",
    "description": (
        "Best practices for designing BigQuery table schemas that balance storage "
        "cost, query performance, and maintainability."
    ),
    "practices": [
        {
            "id": "SD-001",
            "title": "Partition tables on a date/timestamp column",
            "severity": "HIGH",
            "impact": "Cost & Performance",
            "description": (
                "Partitioning physically separates data by a time unit (DAY, MONTH, YEAR) "
                "or integer range. Queries that filter on the partition column only scan "
                "relevant partitions, dramatically reducing cost."
            ),
            "do": [
                "PARTITION BY DATE(created_at) for event tables",
                "PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0,1000000,10000)) for integer keys",
                "Set partition expiration: OPTIONS(partition_expiration_days=365)",
            ],
            "dont": [
                "Leave high-volume append tables unpartitioned",
                "Partition by a high-cardinality column that creates thousands of tiny partitions",
            ],
            "example": (
                "CREATE TABLE `project.dataset.events`\n"
                "PARTITION BY DATE(event_timestamp)\n"
                "CLUSTER BY user_id, event_type\n"
                "OPTIONS(partition_expiration_days=730)\n"
                "AS SELECT * FROM staging.raw_events WHERE FALSE"
            ),
        },
        {
            "id": "SD-002",
            "title": "Cluster tables on frequently filtered/joined columns",
            "severity": "HIGH",
            "impact": "Performance",
            "description": (
                "Clustering sorts data within each partition by up to 4 columns. "
                "Queries that filter or join on cluster keys skip irrelevant blocks, "
                "reducing bytes scanned with no extra cost."
            ),
            "do": [
                "CLUSTER BY (country, user_id) when queries commonly filter by region then user",
                "Choose cluster columns by cardinality: higher cardinality first",
                "Re-cluster large tables periodically if DML operations disrupt order",
            ],
            "dont": [
                "Cluster on boolean or very low-cardinality columns (little benefit)",
                "Use more than 4 cluster columns (limit is 4)",
            ],
            "example": (
                "CREATE TABLE sales\n"
                "PARTITION BY DATE(sale_date)\n"
                "CLUSTER BY region, product_category, customer_id\n"
                "AS SELECT * FROM staging.sales"
            ),
        },
        {
            "id": "SD-003",
            "title": "Use STRUCT and ARRAY to denormalise related data",
            "severity": "MEDIUM",
            "impact": "Performance & Simplicity",
            "description": (
                "BigQuery is optimised for denormalised schemas. Storing nested/repeated "
                "fields as STRUCT<> or ARRAY<> avoids expensive JOINs and keeps related "
                "data co-located in the same row group."
            ),
            "do": [
                "Use ARRAY<STRUCT<key STRING, value STRING>> for tag/attribute collections",
                "Nest address fields inside a STRUCT<> on a user record",
                "Use UNNEST() to flatten arrays in queries",
            ],
            "dont": [
                "Create separate child tables that always join back to the parent",
                "Store JSON strings as STRING columns when you can use native STRUCT/ARRAY",
            ],
            "example": (
                "CREATE TABLE users (\n"
                "  user_id STRING,\n"
                "  name    STRING,\n"
                "  address STRUCT<street STRING, city STRING, country STRING>,\n"
                "  tags    ARRAY<STRING>\n"
                ")"
            ),
        },
        {
            "id": "SD-004",
            "title": "Choose the right data types",
            "severity": "MEDIUM",
            "impact": "Storage & Performance",
            "description": (
                "Correct data types reduce storage, speed up comparisons, and avoid "
                "implicit casts that can break partition pruning."
            ),
            "do": [
                "Use INT64 for integer IDs, not STRING",
                "Use TIMESTAMP or DATE for time values, not STRING",
                "Use NUMERIC/BIGNUMERIC for financial amounts needing exact precision",
                "Use BOOL instead of STRING 'true'/'false'",
            ],
            "dont": [
                "Store dates as YYYYMMDD integers or strings",
                "Use FLOAT64 for monetary values (precision loss)",
                "Use STRING for enum-like columns with only a few values (consider ENUM pattern)",
            ],
            "example": (
                "-- Good\n"
                "CREATE TABLE transactions (\n"
                "  transaction_id INT64,\n"
                "  amount         NUMERIC(18,2),\n"
                "  created_at     TIMESTAMP,\n"
                "  is_refunded    BOOL\n"
                ")"
            ),
        },
        {
            "id": "SD-005",
            "title": "Avoid wide tables with hundreds of columns",
            "severity": "MEDIUM",
            "impact": "Maintainability & Cost",
            "description": (
                "Tables with hundreds of sparse columns are hard to maintain and "
                "queries accidentally reading many columns become expensive. "
                "Use STRUCT grouping or split into logical sub-tables."
            ),
            "do": [
                "Group related columns into STRUCTs (e.g., metrics STRUCT<clicks INT64, impressions INT64>)",
                "Split event properties into a key-value ARRAY<STRUCT<>> if the schema is dynamic",
            ],
            "dont": [
                "Add a new column for every event attribute (leads to 500+ column tables)",
                "Use nullable columns as a substitute for proper schema design",
            ],
            "example": (
                "-- Instead of 50 separate metric columns:\n"
                "metrics STRUCT<\n"
                "  clicks       INT64,\n"
                "  impressions  INT64,\n"
                "  conversions  INT64,\n"
                "  revenue      NUMERIC\n"
                ">"
            ),
        },
        {
            "id": "SD-006",
            "title": "Prefer time-partitioned tables over date-sharded tables",
            "severity": "HIGH",
            "impact": "Performance & Maintainability",
            "description": (
                "Date-sharded tables (e.g., events_20240101, events_20240102) require "
                "wildcard queries and maintain separate schema metadata per shard, "
                "increasing management overhead and query planning cost. A single "
                "time-partitioned table with PARTITION BY DATE gives the same pruning "
                "with a simpler schema and better optimiser support."
            ),
            "do": [
                "Migrate existing shards: CREATE TABLE events PARTITION BY event_date AS SELECT ...",
                "Use _PARTITIONDATE pseudo-column or the partition column in WHERE for pruning",
                "Set partition_expiration_days to auto-expire old partitions",
            ],
            "dont": [
                "Create new date-suffix tables (events_YYYYMMDD) for ongoing workloads",
                "Query sharded tables with dataset.* wildcard — no pruning without _TABLE_SUFFIX",
            ],
            "example": (
                "-- Migrate sharded tables to a single partitioned table\n"
                "CREATE OR REPLACE TABLE dataset.events\n"
                "PARTITION BY event_date\n"
                "CLUSTER BY user_id\n"
                "OPTIONS(partition_expiration_days=365)\n"
                "AS\n"
                "SELECT * FROM `dataset.events_*`\n"
                "WHERE _TABLE_SUFFIX >= '20230101'"
            ),
        },
        {
            "id": "SD-007",
            "title": "Use BigQuery native JSON type for semi-structured data",
            "severity": "MEDIUM",
            "impact": "Performance & Flexibility",
            "description": (
                "The native JSON type stores data in a lossless binary encoding and "
                "supports path-based access (col.field, col[0]) without string parsing. "
                "It outperforms JSON stored as STRING and is more flexible than rigid "
                "STRUCT schemas for truly dynamic payloads."
            ),
            "do": [
                "Declare columns as JSON for event properties or webhook payloads with variable structure",
                "Use col.key or JSON_VALUE(col, '$.key') for field extraction",
                "Index specific JSON paths with search indexes for query acceleration",
            ],
            "dont": [
                "Store JSON as STRING and call JSON_EXTRACT on every query (parses string repeatedly)",
                "Use JSON type for well-known fixed schemas — native STRUCT is faster and typed",
            ],
            "example": (
                "CREATE TABLE dataset.events (\n"
                "  event_id   STRING,\n"
                "  event_date DATE,\n"
                "  event_type STRING,\n"
                "  properties JSON     -- dynamic per-event fields\n"
                ");\n\n"
                "-- Query a JSON field\n"
                "SELECT event_id, properties.user_agent\n"
                "FROM dataset.events\n"
                "WHERE event_date = CURRENT_DATE()\n"
                "  AND JSON_VALUE(properties, '$.country') = 'IN'"
            ),
        },
        {
            "id": "SD-008",
            "title": "Choose physical storage billing for compressed savings",
            "severity": "MEDIUM",
            "impact": "Storage Cost",
            "description": (
                "BigQuery offers two storage billing models per dataset: logical "
                "(uncompressed bytes) and physical (compressed bytes on disk + "
                "time-travel + failsafe). For datasets with high compression ratios "
                "(columnar data, repetitive values), physical billing can cut storage "
                "costs by 30–70%. Evaluate on a dataset-by-dataset basis."
            ),
            "do": [
                "ALTER DATASET SET OPTIONS(storage_billing_model='PHYSICAL') for large, compressible datasets",
                "Use INFORMATION_SCHEMA.TABLE_STORAGE to compare logical vs active_physical bytes",
                "Reduce time-travel window (default 7 days) to minimise physical storage overhead",
            ],
            "dont": [
                "Switch all datasets to physical billing without comparing logical vs physical bytes first",
                "Use physical billing for datasets with low compression ratios (may cost more)",
            ],
            "example": (
                "-- Compare before switching billing model\n"
                "SELECT\n"
                "  table_name,\n"
                "  total_logical_bytes / 1e9            AS logical_gb,\n"
                "  active_physical_bytes / 1e9          AS physical_gb,\n"
                "  ROUND(active_physical_bytes / NULLIF(total_logical_bytes, 0), 2) AS compression_ratio\n"
                "FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE\n"
                "WHERE table_schema = 'my_dataset'\n"
                "ORDER BY logical_gb DESC"
            ),
        },
    ],
}
