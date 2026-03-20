PARTITIONING = {
    "title": "Table Partitioning & Expiration",
    "description": (
        "Best practices for partitioning BigQuery tables to reduce scan cost, "
        "enforce TTL policies, and avoid unbounded table growth."
    ),
    "practices": [
        {
            "id": "PT-001",
            "title": "Always partition large tables",
            "severity": "critical",
            "impact": "cost",
            "description": (
                "Tables over 1 GB that are queried with date/time filters should be partitioned. "
                "Without partitioning, every query scans the entire table."
            ),
            "do": [
                "Use DATE or TIMESTAMP partitioning on the most-queried time column.",
                "Enable requirePartitionFilter on production tables to prevent accidental full scans.",
                "Choose MONTH or YEAR partitioning for very large tables to reduce partition count.",
            ],
            "dont": [
                "Don't leave large, frequently queried tables unpartitioned.",
                "Don't create more than 4000 partitions per table.",
                "Don't use high-cardinality STRING columns as partition keys.",
            ],
            "example": (
                "CREATE TABLE my_dataset.events\n"
                "PARTITION BY DATE(event_ts)\n"
                "OPTIONS(require_partition_filter = TRUE)\n"
                "AS SELECT * FROM source_table;"
            ),
        },
        {
            "id": "PT-002",
            "title": "Set partition expiration (TTL)",
            "severity": "high",
            "impact": "cost",
            "description": (
                "Partition expiration automatically deletes old partitions, "
                "preventing unbounded storage growth and reducing long-term storage costs."
            ),
            "do": [
                "Set partition_expiration_days appropriate to your retention needs.",
                "Use table-level default TTL for event/log tables with short retention windows.",
                "Combine partition TTL with table-level expiration for staging tables.",
            ],
            "dont": [
                "Don't forget TTL on high-volume staging or raw ingestion tables.",
                "Don't set TTL shorter than your SLA recovery window.",
            ],
            "example": (
                "ALTER TABLE my_dataset.events\n"
                "SET OPTIONS (partition_expiration_days = 90);"
            ),
        },
        {
            "id": "PT-003",
            "title": "Always filter on the partition column",
            "severity": "critical",
            "impact": "cost",
            "description": (
                "Queries on partitioned tables must filter on the partition column to benefit from "
                "partition pruning. A WHERE clause on a non-partition column will still scan all partitions."
            ),
            "do": [
                "Filter directly on the partition column: WHERE event_date = '2024-01-01'.",
                "Use DATE(timestamp_col) when the partition key is derived from a TIMESTAMP column.",
                "Use TIMESTAMP_TRUNC for TIMESTAMP-partitioned tables.",
            ],
            "dont": [
                "Don't filter on a computed expression that wraps the partition column: WHERE YEAR(event_date) = 2024.",
                "Don't assume a WHERE clause on a non-partition column triggers pruning.",
            ],
            "example": (
                "-- Good: partition pruning active\n"
                "SELECT * FROM events WHERE DATE(event_ts) = '2024-01-15';\n\n"
                "-- Bad: full scan even on partitioned table\n"
                "SELECT * FROM events WHERE EXTRACT(YEAR FROM event_ts) = 2024;"
            ),
        },
        {
            "id": "PT-004",
            "title": "Use integer-range partitioning for non-time keys",
            "severity": "medium",
            "impact": "cost",
            "description": (
                "When the natural query filter is an integer ID (e.g. customer_id, shard_id), "
                "use integer-range partitioning instead of forcing a timestamp column."
            ),
            "do": [
                "Define start, end, and interval for integer range partitions.",
                "Keep the number of partitions manageable (< 4000).",
            ],
            "dont": [
                "Don't use high-cardinality integers with very small intervals — creates too many partitions.",
            ],
            "example": (
                "CREATE TABLE my_dataset.customers\n"
                "PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 10000000, 100000));"
            ),
        },
    ],
}
