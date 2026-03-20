STORAGE_PRICING = {
    "title": "Storage Pricing & Optimization",
    "description": (
        "Best practices for managing BigQuery storage costs through tiering, "
        "compression, and lifecycle policies."
    ),
    "practices": [
        {
            "id": "SP-001",
            "title": "Leverage long-term storage pricing automatically",
            "severity": "medium",
            "impact": "cost",
            "description": (
                "BigQuery automatically classifies tables/partitions unmodified for 90+ days "
                "as long-term storage, charged at ~50% of active storage rates. "
                "Avoid unnecessary writes that reset the 90-day clock."
            ),
            "do": [
                "Avoid appending or overwriting stable historical partitions unnecessarily.",
                "Use INSERT-only patterns for event data; never UPDATE historical partitions.",
                "Monitor active vs long-term bytes in INFORMATION_SCHEMA.TABLE_STORAGE.",
            ],
            "dont": [
                "Don't run nightly full-table overwrites on historical data — resets all partitions to active pricing.",
                "Don't create redundant copies of rarely-accessed tables.",
            ],
            "example": (
                "-- Check active vs long-term storage per table:\n"
                "SELECT table_name,\n"
                "  ROUND(active_logical_bytes / 1e9, 2) AS active_gb,\n"
                "  ROUND(long_term_logical_bytes / 1e9, 2) AS long_term_gb\n"
                "FROM my_dataset.INFORMATION_SCHEMA.TABLE_STORAGE\n"
                "ORDER BY active_logical_bytes DESC;"
            ),
        },
        {
            "id": "SP-002",
            "title": "Use physical storage billing where appropriate",
            "severity": "medium",
            "impact": "cost",
            "description": (
                "BigQuery offers physical (compressed) storage billing in addition to logical billing. "
                "For highly compressible data, physical billing can be significantly cheaper."
            ),
            "do": [
                "Compare logical vs physical bytes for each dataset to identify savings.",
                "Switch datasets with high compression ratios to physical billing.",
                "Audit compression ratio via INFORMATION_SCHEMA.TABLE_STORAGE.",
            ],
            "dont": [
                "Don't switch to physical billing for datasets with low compression ratios (JSON, already-compressed data).",
                "Don't switch billing models mid-month — billing is prorated but the switch is immediate.",
            ],
            "example": (
                "-- Compare logical vs physical bytes:\n"
                "SELECT table_name,\n"
                "  ROUND(active_logical_bytes / 1e9, 2) AS logical_gb,\n"
                "  ROUND(active_physical_bytes / 1e9, 2) AS physical_gb,\n"
                "  ROUND(active_logical_bytes / NULLIF(active_physical_bytes, 0), 2) AS compression_ratio\n"
                "FROM my_dataset.INFORMATION_SCHEMA.TABLE_STORAGE;"
            ),
        },
        {
            "id": "SP-003",
            "title": "Delete unused datasets and tables proactively",
            "severity": "high",
            "impact": "cost",
            "description": (
                "Unused tables, temporary exports, and dev datasets accumulate silently. "
                "Regular audits and table expiration policies prevent storage sprawl."
            ),
            "do": [
                "Set default_table_expiration_days on dev and staging datasets.",
                "Schedule periodic audits of tables with zero query activity.",
                "Tag tables with team/owner labels to facilitate cleanup accountability.",
            ],
            "dont": [
                "Don't leave export jobs writing to the same dataset indefinitely without TTL.",
                "Don't assume tables are in use just because they exist — query INFORMATION_SCHEMA.JOBS to verify.",
            ],
            "example": (
                "-- Set default table expiration on a dataset (30 days):\n"
                "ALTER SCHEMA my_dev_dataset\n"
                "SET OPTIONS (default_table_expiration_days = 30);\n\n"
                "-- Find tables with no queries in the last 90 days:\n"
                "SELECT t.table_name\n"
                "FROM my_dataset.INFORMATION_SCHEMA.TABLES t\n"
                "WHERE t.table_name NOT IN (\n"
                "  SELECT DISTINCT referenced_table.table_id\n"
                "  FROM INFORMATION_SCHEMA.JOBS_BY_PROJECT,\n"
                "    UNNEST(referenced_tables) AS referenced_table\n"
                "  WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)\n"
                ");"
            ),
        },
    ],
}
