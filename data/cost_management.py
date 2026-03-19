COST_MANAGEMENT = {
    "title": "Cost Management",
    "description": (
        "Best practices to monitor, control, and reduce BigQuery spend "
        "without sacrificing analytical capability."
    ),
    "practices": [
        {
            "id": "CO-001",
            "title": "Always preview query cost before running",
            "severity": "HIGH",
            "impact": "Cost",
            "description": (
                "The BigQuery console dry-run and the `--dry_run` flag in bq CLI "
                "return bytes billed without executing the query. Build this check "
                "into CI pipelines and analyst workflows."
            ),
            "do": [
                "bq query --dry_run --use_legacy_sql=false 'SELECT ...'",
                "Use the Google Cloud Pricing Calculator to estimate monthly cost",
                "Set per-query byte limits via maximumBytesBilled in job config",
            ],
            "dont": [
                "Run exploratory queries on production tables without a LIMIT or dry-run",
                "Ignore the bytes-processed estimate shown in the console",
            ],
            "example": (
                "# Python client dry-run example\n"
                "from google.cloud import bigquery\n"
                "client = bigquery.Client()\n"
                "job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=True)\n"
                "job = client.query('SELECT * FROM dataset.table LIMIT 100', job_config=job_config)\n"
                "print(f'Bytes processed: {job.total_bytes_processed / 1e9:.2f} GB')"
            ),
        },
        {
            "id": "CO-002",
            "title": "Leverage query result caching",
            "severity": "MEDIUM",
            "impact": "Cost & Performance",
            "description": (
                "BigQuery caches query results for 24 hours. Identical queries "
                "(same SQL, same referenced tables, no volatile functions) are free. "
                "Avoid defeating the cache with non-deterministic functions."
            ),
            "do": [
                "Use CURRENT_DATE() in parameterised queries rather than hardcoded dates when possible",
                "Enable use_query_cache=True in job configuration (it is on by default)",
                "Schedule dashboards to hit cached results",
            ],
            "dont": [
                "Include NOW() or RAND() in queries that run repeatedly with the same intent",
                "Disable caching globally to 'ensure freshness' unless data truly changes per second",
            ],
            "example": (
                "-- Cache-friendly: date is a query parameter, not NOW()\n"
                "DECLARE report_date DATE DEFAULT '2024-06-01';\n"
                "SELECT COUNT(*) FROM events WHERE event_date = report_date"
            ),
        },
        {
            "id": "CO-003",
            "title": "Use materialised views and scheduled queries",
            "severity": "MEDIUM",
            "impact": "Cost & Performance",
            "description": (
                "Pre-aggregate expensive computations into materialised views or "
                "scheduled query results. Dashboards query the small summary table "
                "instead of scanning the raw fact table."
            ),
            "do": [
                "CREATE MATERIALIZED VIEW for daily/hourly aggregates queried frequently",
                "Schedule nightly ETL to build denormalised reporting tables",
                "Use incremental materialised views (max_staleness option) for near-real-time",
            ],
            "dont": [
                "Run the same heavy GROUP BY query dozens of times per day from a dashboard",
                "Materialise entire raw tables without any aggregation/filtering",
            ],
            "example": (
                "CREATE MATERIALIZED VIEW dataset.daily_revenue\n"
                "OPTIONS(enable_refresh=true, refresh_interval_minutes=60)\n"
                "AS\n"
                "SELECT DATE(order_date) AS day, SUM(amount) AS revenue\n"
                "FROM dataset.orders\n"
                "GROUP BY day"
            ),
        },
        {
            "id": "CO-004",
            "title": "Set budgets, quotas, and cost alerts",
            "severity": "HIGH",
            "impact": "Cost Governance",
            "description": (
                "Use Google Cloud Budget Alerts and BigQuery custom quotas to prevent "
                "runaway queries from causing bill shock. Per-project and per-user "
                "daily byte limits act as a safety net."
            ),
            "do": [
                "Set Google Cloud budget alerts at 50%, 90%, 100% of monthly budget",
                "Use custom quotas: project_daily_scan_quota in BigQuery Admin",
                "Set maximumBytesBilled in all programmatic query job configs",
                "Enable INFORMATION_SCHEMA.JOBS_BY_PROJECT monitoring",
            ],
            "dont": [
                "Run BigQuery without any budget alerts in production",
                "Grant all analysts BIGQUERY_ADMIN which bypasses query cost controls",
            ],
            "example": (
                "# Set a hard 10 GB per-query limit\n"
                "job_config = bigquery.QueryJobConfig(\n"
                "    maximum_bytes_billed=10 * 1024**3  # 10 GB\n"
                ")\n"
                "# Query raises google.api_core.exceptions.InternalServerError if exceeded"
            ),
        },
        {
            "id": "CO-005",
            "title": "Use long-term storage pricing and table expiration",
            "severity": "MEDIUM",
            "impact": "Storage Cost",
            "description": (
                "Tables and partitions not modified for 90+ consecutive days drop to "
                "long-term storage pricing (~50% cheaper). Set expiration on staging "
                "and temporary tables to avoid forgotten data accumulating costs."
            ),
            "do": [
                "OPTIONS(expiration_timestamp=TIMESTAMP '2025-12-31 00:00:00 UTC') on temp tables",
                "Set default_table_expiration_ms on scratch/staging datasets",
                "Regularly audit storage with INFORMATION_SCHEMA.TABLE_STORAGE",
            ],
            "dont": [
                "Leave staging tables with no expiration after pipelines complete",
                "Compress and re-upload data just to reset the 90-day clock (wastes slot time)",
            ],
            "example": (
                "CREATE TABLE dataset.temp_staging\n"
                "OPTIONS(\n"
                "  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)\n"
                ")\n"
                "AS SELECT ..."
            ),
        },
        {
            "id": "CO-006",
            "title": "Use BigQuery Editions (capacity pricing) for predictable workloads",
            "severity": "HIGH",
            "impact": "Cost Governance",
            "description": (
                "On-demand pricing bills per TB scanned — efficient for light workloads "
                "but unpredictable for heavy pipelines. BigQuery Editions (Standard, "
                "Enterprise, Enterprise Plus) provide reserved slots with autoscaling "
                "and 1-year/3-year commitments for significant discounts. Use the "
                "slot estimator to compare break-even points before committing."
            ),
            "do": [
                "Run the BigQuery slot estimator against 30 days of INFORMATION_SCHEMA.JOBS history",
                "Start with autoscaling reservations (baseline=0, max=N) to right-size capacity",
                "Use separate reservations per environment (prod, dev, batch) to prevent contention",
                "Purchase 1-year commitments once baseline slot usage is stable",
            ],
            "dont": [
                "Move to capacity pricing without analysing actual slot consumption patterns first",
                "Use a single reservation for all workloads (batch will starve interactive queries)",
            ],
            "example": (
                "-- Check average and peak slot usage to estimate reservation size\n"
                "SELECT\n"
                "  DATE(creation_time)                        AS day,\n"
                "  MAX(total_slot_ms) / (1000 * 60 * 60 * 24) AS peak_slot_hours\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "GROUP BY day\n"
                "ORDER BY peak_slot_hours DESC"
            ),
        },
        {
            "id": "CO-007",
            "title": "Avoid repeated table overwrites to reduce time-travel storage",
            "severity": "MEDIUM",
            "impact": "Storage Cost",
            "description": (
                "Every WRITE_TRUNCATE or CREATE OR REPLACE retains the previous "
                "table snapshot in time-travel storage (up to 7 days by default). "
                "On the physical storage billing model, this significantly inflates "
                "costs for frequently-refreshed tables. Use WRITE_APPEND + MERGE or "
                "reduce the time-travel window for high-churn tables."
            ),
            "do": [
                "Use WRITE_APPEND + MERGE for incremental updates instead of full overwrites",
                "Set time_travel_hours = 48 (minimum) on high-churn staging tables",
                "Use INFORMATION_SCHEMA.TABLE_STORAGE to audit time-travel byte accumulation",
            ],
            "dont": [
                "Run TRUNCATE + INSERT repeatedly on large tables under physical billing",
                "Leave default 7-day time travel on tables that are overwritten hourly",
            ],
            "example": (
                "-- Reduce time-travel retention on a staging table overwritten daily\n"
                "ALTER TABLE dataset.daily_staging\n"
                "SET OPTIONS(max_time_travel_hours = 48);\n\n"
                "-- Check time-travel storage cost contribution\n"
                "SELECT table_name,\n"
                "       time_travel_physical_bytes / 1e9 AS tt_gb\n"
                "FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE\n"
                "ORDER BY tt_gb DESC LIMIT 20"
            ),
        },
        {
            "id": "CO-008",
            "title": "Preview data without running queries",
            "severity": "LOW",
            "impact": "Cost",
            "description": (
                "Exploratory SELECT queries on large tables to verify schema or sample "
                "rows incur scan charges. Use free alternatives: the BigQuery console "
                "Preview tab, `bq head`, or the tabledata.list API — none of these "
                "trigger query billing."
            ),
            "do": [
                "Use the BigQuery console 'Preview' tab for free row sampling",
                "Use `bq head -n 20 dataset.table` in the CLI for schema inspection",
                "Use tabledata.list API in scripts for metadata validation without billing",
            ],
            "dont": [
                "SELECT * FROM table LIMIT 10 to check the schema — still scans the first column blocks",
                "Run full COUNT(*) queries to verify row counts (use TABLE_STORAGE instead)",
            ],
            "example": (
                "# Free row preview via CLI (no query charge)\n"
                "bq head --max_rows=20 project:dataset.tablename\n\n"
                "# Free row count from metadata (no query charge)\n"
                "bq show --format=prettyjson project:dataset.tablename | jq '.numRows'"
            ),
        },
        {
            "id": "CO-009",
            "title": "Materialise intermediate stages to reduce repeated scanning",
            "severity": "MEDIUM",
            "impact": "Cost & Performance",
            "description": (
                "Complex multi-stage queries that filter, join, and aggregate the same "
                "large dataset repeatedly are better broken into explicit destination "
                "table stages. Each intermediate table is written once and queried "
                "from its smaller result, dramatically reducing total bytes billed."
            ),
            "do": [
                "Break ETL pipelines into: raw → filtered_staging → aggregated_result destination tables",
                "Use CREATE TABLE ... AS SELECT to materialise filter + join results before aggregation",
                "Set table expiration on intermediate staging tables to avoid cost accumulation",
            ],
            "dont": [
                "Reference the same 1 TB CTE or subquery in 5 different branches of a single query",
                "Re-run the same expensive base scan in every downstream pipeline step",
            ],
            "example": (
                "-- Stage 1: filter once to a smaller destination table\n"
                "CREATE OR REPLACE TABLE dataset.orders_2024\n"
                "OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))\n"
                "AS\n"
                "SELECT * FROM dataset.raw_orders\n"
                "WHERE order_date >= '2024-01-01';\n\n"
                "-- Stage 2: aggregate from the small table (cheap)\n"
                "SELECT region, SUM(amount) FROM dataset.orders_2024\n"
                "GROUP BY region"
            ),
        },
    ],
}
