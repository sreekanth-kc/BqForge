BI_ENGINE = {
    "title": "BI Engine",
    "description": (
        "Best practices for BigQuery BI Engine — in-memory analysis service that "
        "accelerates SQL queries and Looker/Data Studio dashboards."
    ),
    "practices": [
        {
            "id": "BE-001",
            "title": "Pin only the right tables to BI Engine memory",
            "severity": "high",
            "impact": "performance",
            "description": (
                "BI Engine caches specific tables in memory. Pinning the wrong tables "
                "(too large, rarely queried, or frequently updated) wastes reservation capacity."
            ),
            "do": [
                "Pin tables that are < 10 GB and queried by dashboards many times per hour.",
                "Prefer dimension/lookup tables over large fact tables.",
                "Monitor BI Engine utilization in INFORMATION_SCHEMA.BI_CAPACITIES.",
            ],
            "dont": [
                "Don't pin tables that change every few minutes — BI Engine refresh lag will cause stale results.",
                "Don't pin tables larger than your total BI Engine reservation.",
                "Don't pin tables that are only queried once a day.",
            ],
            "example": (
                "-- Pin a table to BI Engine:\n"
                "ALTER TABLE my_dataset.dim_product\n"
                "SET OPTIONS (enable_refresh = TRUE);"
            ),
        },
        {
            "id": "BE-002",
            "title": "Understand BI Engine fallback behavior",
            "severity": "medium",
            "impact": "performance",
            "description": (
                "BI Engine has SQL feature limitations. Queries using unsupported features "
                "silently fall back to standard BigQuery execution, losing the speed benefit."
            ),
            "do": [
                "Check query execution details for 'BI Engine Mode: FULL' vs 'PARTIAL' vs 'DISABLED'.",
                "Simplify dashboard queries to avoid unsupported operations (UDFs, certain aggregations).",
                "Use INFORMATION_SCHEMA.JOBS to identify fallback queries.",
            ],
            "dont": [
                "Don't assume all queries against pinned tables are served from BI Engine.",
                "Don't use user-defined functions (UDFs) in BI Engine-targeted queries.",
                "Don't rely on BI Engine for queries with more than 16 GROUP BY columns.",
            ],
            "example": (
                "-- Check BI Engine usage in query metadata:\n"
                "SELECT job_id, bi_engine_statistics\n"
                "FROM INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);"
            ),
        },
        {
            "id": "BE-003",
            "title": "Right-size your BI Engine reservation",
            "severity": "medium",
            "impact": "cost",
            "description": (
                "BI Engine reservations are billed per GB-hour. Over-provisioning is wasteful; "
                "under-provisioning causes fallback for large dashboards."
            ),
            "do": [
                "Start with the total size of your pinned tables + 20% headroom.",
                "Scale up only when INFORMATION_SCHEMA shows high fallback rates.",
                "Use separate reservations for prod and dev/staging environments.",
            ],
            "dont": [
                "Don't provision BI Engine in every region — only where your dashboards run.",
                "Don't forget to delete dev reservations when not in use.",
            ],
            "example": (
                "-- Create a BI Engine reservation (via API or Console):\n"
                "-- gcloud bigquery bi-engine reservations create \\\n"
                "--   --location=US --size=10Gi"
            ),
        },
    ],
}
