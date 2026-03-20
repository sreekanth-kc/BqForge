SCHEDULED_QUERIES = {
    "title": "Scheduled Queries & Dataform Patterns",
    "description": (
        "Best practices for BigQuery scheduled queries, Dataform workflows, "
        "and recurring pipeline management."
    ),
    "practices": [
        {
            "id": "SQ-001",
            "title": "Always label scheduled queries with owner and purpose",
            "severity": "high",
            "impact": "governance",
            "description": (
                "Unlabeled scheduled queries become 'zombie' jobs — they run indefinitely, "
                "accumulate cost, and nobody knows who owns them or what they do. "
                "Labels make cost attribution and auditing possible."
            ),
            "do": [
                "Add labels: owner, team, purpose, and environment to every scheduled query.",
                "Include the query name in the job label: {'pipeline': 'daily_user_rollup'}.",
                "Use Dataform's built-in labels config for pipeline-wide label inheritance.",
            ],
            "dont": [
                "Don't create scheduled queries without assigning a team/owner label.",
                "Don't reuse generic labels like {'type': 'etl'} — they're not specific enough for cost attribution.",
            ],
            "example": (
                "-- BigQuery Scheduled Query configuration (labels section):\n"
                "labels:\n"
                "  owner: 'data-eng-team'\n"
                "  pipeline: 'daily_revenue_rollup'\n"
                "  environment: 'prod'\n"
                "  cost_center: 'analytics'"
            ),
        },
        {
            "id": "SQ-002",
            "title": "Use incremental/partitioned writes, not full overwrites",
            "severity": "critical",
            "impact": "cost",
            "description": (
                "Scheduled queries that overwrite entire tables on every run reset long-term "
                "storage pricing, re-process historical data unnecessarily, and are expensive. "
                "Write only to the latest partition."
            ),
            "do": [
                "Use WRITE_TRUNCATE on a specific partition: $({run_time|'%Y%m%d'}).",
                "Use INSERT INTO with a WHERE clause filtering to the current run window.",
                "Use Dataform incremental tables with `where` predicates.",
            ],
            "dont": [
                "Don't use WRITE_TRUNCATE on the entire table for recurring loads.",
                "Don't process all historical data on every run when only new data has arrived.",
            ],
            "example": (
                "-- Write to today's partition only (scheduled query destination):\n"
                "-- Destination: my_dataset.daily_summary${run_time|\"%Y%m%d\"}\n"
                "-- Write disposition: WRITE_TRUNCATE\n\n"
                "SELECT date, SUM(revenue) AS total_revenue\n"
                "FROM source_table\n"
                "WHERE DATE(event_ts) = DATE(@run_time)  -- parameterized run time\n"
                "GROUP BY date;"
            ),
        },
        {
            "id": "SQ-003",
            "title": "Set failure notifications and retry limits on scheduled queries",
            "severity": "high",
            "impact": "reliability",
            "description": (
                "Silent failures in scheduled queries cause data staleness that goes undetected. "
                "Every scheduled query should have alerting and a bounded retry policy."
            ),
            "do": [
                "Configure Cloud Pub/Sub notifications for scheduled query failures.",
                "Set maximum retry attempts to avoid unbounded cost from retry storms.",
                "Route failure notifications to a team channel, not an individual's email.",
            ],
            "dont": [
                "Don't leave scheduled queries running without failure alerts.",
                "Don't set unlimited retries on expensive queries.",
            ],
            "example": (
                "# Monitoring alert via Cloud Monitoring:\n"
                "# Resource: bigquery_scheduled_query\n"
                "# Metric: state = FAILED\n"
                "# Notification: PagerDuty / Slack webhook"
            ),
        },
        {
            "id": "SQ-004",
            "title": "Use Dataform assertions to validate output data",
            "severity": "high",
            "impact": "reliability",
            "description": (
                "Pipeline outputs should be validated after each run. Dataform assertions "
                "run automatically and block downstream dependencies if data quality fails."
            ),
            "do": [
                "Add row count assertions: output must have > 0 rows.",
                "Add uniqueness assertions on primary key columns.",
                "Add referential integrity checks between related tables.",
            ],
            "dont": [
                "Don't push pipeline outputs to downstream consumers without validation.",
                "Don't skip assertions in production to save cost — a bad write costs more to fix.",
            ],
            "example": (
                "// Dataform SQLX assertion:\n"
                "config {\n"
                "  type: 'assertion',\n"
                "  description: 'daily_summary must have rows for each day'\n"
                "}\n\n"
                "SELECT date\n"
                "FROM ${ref('daily_summary')}\n"
                "GROUP BY date\n"
                "HAVING COUNT(*) = 0  -- Fails if any date has 0 rows"
            ),
        },
        {
            "id": "SQ-005",
            "title": "Stagger scheduled query start times to avoid slot contention",
            "severity": "medium",
            "impact": "performance",
            "description": (
                "When many scheduled queries start at exactly the same time (e.g. midnight), "
                "they compete for slots and all run slower. Stagger start times by 5–15 minutes."
            ),
            "do": [
                "Spread query start times: 00:00, 00:05, 00:15, 00:30 instead of all at 00:00.",
                "Prioritize critical pipelines with earlier start times.",
                "Use Dataform workflow scheduling to control dependency ordering.",
            ],
            "dont": [
                "Don't schedule all daily pipelines at exactly midnight.",
                "Don't assume flat-rate slots eliminate all contention — scheduling still matters.",
            ],
            "example": (
                "# Cron schedules spread across the hour:\n"
                "# Critical pipeline:   0 0 * * *   (00:00)\n"
                "# Secondary pipeline:  5 0 * * *   (00:05)\n"
                "# Reporting rollup:   15 0 * * *   (00:15)\n"
                "# Archive job:        30 0 * * *   (00:30)"
            ),
        },
    ],
}
