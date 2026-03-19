MATERIALIZED_VIEWS = {
    "title": "Materialized Views",
    "description": (
        "Best practices for creating and managing BigQuery materialized views "
        "to pre-aggregate expensive computations, reduce query costs, and "
        "accelerate BI dashboards."
    ),
    "practices": [
        {
            "id": "MV-001",
            "title": "Use incremental materialized views for near-real-time aggregations",
            "severity": "HIGH",
            "impact": "Cost & Freshness",
            "description": (
                "Incremental materialized views only process new data since the last "
                "refresh rather than recomputing the entire result. Combined with "
                "max_staleness, they deliver sub-minute freshness at a fraction of "
                "the cost of full-table scans."
            ),
            "do": [
                "Set OPTIONS(enable_refresh=true, refresh_interval_minutes=30) for near-real-time",
                "Use max_staleness=INTERVAL '15' MINUTE so BQ serves the MV when fresh enough",
                "Partition the underlying base table; BQ will only re-aggregate new partitions",
            ],
            "dont": [
                "Use a non-incremental MV on a multi-TB table that changes frequently",
                "Set refresh_interval_minutes too low (< 5 min) – you pay per refresh slot-time",
            ],
            "example": (
                "CREATE MATERIALIZED VIEW dataset.hourly_revenue\n"
                "OPTIONS(\n"
                "  enable_refresh = true,\n"
                "  refresh_interval_minutes = 60,\n"
                "  max_staleness = INTERVAL '90' MINUTE\n"
                ")\n"
                "AS\n"
                "SELECT\n"
                "  TIMESTAMP_TRUNC(order_ts, HOUR) AS hour,\n"
                "  region,\n"
                "  SUM(amount)  AS revenue,\n"
                "  COUNT(*)     AS order_count\n"
                "FROM dataset.orders\n"
                "GROUP BY 1, 2"
            ),
        },
        {
            "id": "MV-002",
            "title": "Tune max_staleness to balance freshness vs cost",
            "severity": "MEDIUM",
            "impact": "Cost & Performance",
            "description": (
                "When a query hits a table with an associated MV, BigQuery automatically "
                "serves the MV result if it is within max_staleness. Setting this too "
                "tightly forces full scans; setting it too loosely serves stale data "
                "for time-sensitive dashboards."
            ),
            "do": [
                "Set max_staleness based on the dashboard SLA (e.g., INTERVAL '1' HOUR for daily reports)",
                "Monitor INFORMATION_SCHEMA.MATERIALIZED_VIEWS_BY_PROJECT for last_refresh_time",
                "Use a tighter staleness for operational dashboards, looser for strategic reports",
            ],
            "dont": [
                "Leave max_staleness unset (defaults to no auto-rewrite, defeating the purpose)",
                "Set INTERVAL '0' SECOND – this forces a full refresh on every query hit",
            ],
            "example": (
                "-- Operational dashboard: serve MV if refreshed in last 15 minutes\n"
                "ALTER MATERIALIZED VIEW dataset.live_orders\n"
                "SET OPTIONS(max_staleness = INTERVAL '15' MINUTE)"
            ),
        },
        {
            "id": "MV-003",
            "title": "Avoid non-deterministic functions in MV definitions",
            "severity": "HIGH",
            "impact": "Correctness",
            "description": (
                "Functions like CURRENT_TIMESTAMP(), RAND(), or GENERATE_UUID() are "
                "evaluated at refresh time, not at query time. This means the MV "
                "result may not match what a live query would return, causing subtle "
                "correctness bugs."
            ),
            "do": [
                "Use deterministic expressions; derive time ranges from the base table data",
                "Pass timestamps as query parameters instead of embedding CURRENT_DATE() in the MV",
            ],
            "dont": [
                "SELECT *, CURRENT_TIMESTAMP() AS fetched_at FROM base_table in an MV",
                "Use RAND() or UUID functions inside MV definitions",
            ],
            "example": (
                "-- BAD: CURRENT_TIMESTAMP() is evaluated at refresh time\n"
                "-- CREATE MATERIALIZED VIEW ... AS\n"
                "-- SELECT id, amount, CURRENT_TIMESTAMP() AS snapshot_ts FROM orders\n\n"
                "-- GOOD: derive snapshot from the data itself\n"
                "CREATE MATERIALIZED VIEW dataset.orders_summary AS\n"
                "SELECT DATE(order_ts) AS order_date, SUM(amount) AS daily_revenue\n"
                "FROM dataset.orders\n"
                "GROUP BY 1"
            ),
        },
        {
            "id": "MV-004",
            "title": "Align MV partition and cluster with query patterns",
            "severity": "HIGH",
            "impact": "Performance & Cost",
            "description": (
                "Materialized views inherit the partition column from the base table "
                "but you can specify clustering. Choose cluster columns that match "
                "the filters your BI tool or downstream queries use most often."
            ),
            "do": [
                "CLUSTER BY the dimensions most commonly used in WHERE / GROUP BY of consumer queries",
                "Ensure the MV SELECT includes the partition column so pruning works",
                "Test with INFORMATION_SCHEMA.JOBS to confirm the MV rewrite is being used",
            ],
            "dont": [
                "Create an unpartitioned MV on a partitioned base table – you lose pruning",
                "Cluster on high-cardinality columns that are never filtered (wasted overhead)",
            ],
            "example": (
                "CREATE MATERIALIZED VIEW dataset.sales_by_region\n"
                "CLUSTER BY region, product_id\n"
                "AS\n"
                "SELECT\n"
                "  sale_date,       -- partition column from base table\n"
                "  region,\n"
                "  product_id,\n"
                "  SUM(revenue) AS revenue\n"
                "FROM dataset.sales\n"
                "GROUP BY 1, 2, 3"
            ),
        },
        {
            "id": "MV-005",
            "title": "Monitor MV refresh cost and staleness via INFORMATION_SCHEMA",
            "severity": "MEDIUM",
            "impact": "Cost Visibility",
            "description": (
                "MV refreshes consume slot-time and are billed like regular queries. "
                "Track refresh frequency, bytes processed, and last_refresh_time to "
                "catch runaway MVs and tune refresh intervals."
            ),
            "do": [
                "Query INFORMATION_SCHEMA.MATERIALIZED_VIEWS_BY_PROJECT for staleness overview",
                "Join with INFORMATION_SCHEMA.JOBS to find high-cost refresh jobs",
                "Set billing alerts specifically for service accounts used by scheduled refreshes",
            ],
            "dont": [
                "Ignore MV refresh jobs in your cost attribution model",
                "Create dozens of overlapping MVs on the same base table without auditing costs",
            ],
            "example": (
                "-- Find MVs that haven't refreshed recently\n"
                "SELECT\n"
                "  table_schema,\n"
                "  table_name,\n"
                "  last_refresh_time,\n"
                "  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_refresh_time, MINUTE) AS stale_minutes\n"
                "FROM `region-us`.INFORMATION_SCHEMA.MATERIALIZED_VIEWS_BY_PROJECT\n"
                "ORDER BY stale_minutes DESC"
            ),
        },
        {
            "id": "MV-006",
            "title": "Use BI Engine for sub-second dashboard query acceleration",
            "severity": "MEDIUM",
            "impact": "Performance",
            "description": (
                "BigQuery BI Engine is an in-memory analysis service that caches the "
                "most-frequently-queried table data and accelerates SQL workloads from "
                "Looker Studio, Looker, and the BigQuery API. It complements materialised "
                "views by serving cached sub-second responses without any schema changes."
            ),
            "do": [
                "Enable BI Engine reservations for projects running heavy Looker Studio dashboards",
                "Start with a small reservation (1–10 GB) and scale based on cache hit rate metrics",
                "Combine BI Engine with partitioned/clustered tables for best coverage",
                "Monitor cache_hit_ratio in INFORMATION_SCHEMA.BI_CAPACITIES",
            ],
            "dont": [
                "Use BI Engine as a substitute for proper partitioning and MV design",
                "Reserve more GB than your hot dataset size (unused capacity is still billed)",
            ],
            "example": (
                "-- Create a 10 GB BI Engine reservation in us-central1\n"
                "-- (done via Console: BigQuery → BI Engine → Create reservation)\n\n"
                "-- Monitor cache effectiveness\n"
                "SELECT\n"
                "  reservation_id,\n"
                "  SUM(bi_engine_statistics.bi_engine_reasons) AS cache_misses\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)\n"
                "  AND bi_engine_statistics IS NOT NULL\n"
                "GROUP BY reservation_id"
            ),
        },
        {
            "id": "MV-007",
            "title": "Use MERGE for incremental MV-like updates on unsupported patterns",
            "severity": "MEDIUM",
            "impact": "Correctness & Cost",
            "description": (
                "Native materialized views have SQL restrictions (no subqueries, no "
                "DISTINCT on non-aggregate queries, limited JOIN patterns). When your "
                "aggregation logic exceeds MV capabilities, implement an incremental "
                "MERGE pattern into a destination table as an alternative, processing "
                "only new partitions each run."
            ),
            "do": [
                "Use MERGE ... WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT for upserts",
                "Limit MERGE source to new partitions using WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
                "Wrap MERGE in a scheduled query with idempotent logic for safe retries",
            ],
            "dont": [
                "Run full-table MERGE scans daily when only yesterday's partition changed",
                "Use INSERT OVERWRITE when MERGE would avoid a full-table rewrite",
            ],
            "example": (
                "-- Incremental MERGE: update yesterday's aggregates only\n"
                "MERGE dataset.daily_summary AS T\n"
                "USING (\n"
                "  SELECT DATE(order_ts) AS day, region, SUM(amount) AS revenue\n"
                "  FROM dataset.orders\n"
                "  WHERE DATE(order_ts) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)\n"
                "  GROUP BY 1, 2\n"
                ") AS S\n"
                "ON T.day = S.day AND T.region = S.region\n"
                "WHEN MATCHED THEN UPDATE SET T.revenue = S.revenue\n"
                "WHEN NOT MATCHED THEN INSERT (day, region, revenue) VALUES (S.day, S.region, S.revenue)"
            ),
        },
    ],
}
