MONITORING = {
    "title": "Monitoring & Observability",
    "description": (
        "Best practices for observing BigQuery job health, tracking cost attribution, "
        "detecting anomalies, and building operational dashboards using "
        "INFORMATION_SCHEMA and Cloud Monitoring."
    ),
    "practices": [
        {
            "id": "MO-001",
            "title": "Use INFORMATION_SCHEMA.JOBS as your primary audit trail",
            "severity": "HIGH",
            "impact": "Visibility & Debugging",
            "description": (
                "INFORMATION_SCHEMA.JOBS_BY_PROJECT (and JOBS_BY_FOLDER/JOBS_BY_ORGANIZATION) "
                "records every query, load, extract, and copy job. It is the first place "
                "to look for slow queries, expensive scans, failed jobs, and quota violations."
            ),
            "do": [
                "Query JOBS_BY_PROJECT for the last 24h to spot anomalous bytes_billed spikes",
                "Filter on error_result IS NOT NULL to find failed jobs and their error messages",
                "Use job_type = 'QUERY' and statement_type filters for query-specific analysis",
                "Store periodic snapshots in a separate logging dataset for > 180-day retention",
            ],
            "dont": [
                "Rely only on the BigQuery UI history (limited to 1000 jobs per user)",
                "Forget that INFORMATION_SCHEMA jobs data is only retained for 180 days",
            ],
            "example": (
                "-- Top 10 most expensive queries in the last 24 hours\n"
                "SELECT\n"
                "  user_email,\n"
                "  query,\n"
                "  ROUND(total_bytes_billed / 1e12, 4)     AS tb_billed,\n"
                "  ROUND(total_slot_ms / 1e6, 2)           AS slot_seconds,\n"
                "  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_s\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "  AND state = 'DONE'\n"
                "ORDER BY tb_billed DESC\n"
                "LIMIT 10"
            ),
        },
        {
            "id": "MO-002",
            "title": "Set up slot utilisation alerts in Cloud Monitoring",
            "severity": "HIGH",
            "impact": "Performance & Cost Governance",
            "description": (
                "BigQuery exposes slot utilisation metrics via Cloud Monitoring. "
                "For reserved capacity (flat-rate/editions), sustained high utilisation "
                "means queries are queuing. For on-demand, it signals unexpectedly "
                "large workloads that will generate a big bill."
            ),
            "do": [
                "Create a Cloud Monitoring alert on bigquery.googleapis.com/slots/allocated_for_project",
                "Set a threshold at 80% of your reservation capacity to get early warning",
                "Use alerting policies with notification channels (PagerDuty, Slack, email)",
                "Dashboard: combine slot_utilization + bytes_billed to correlate cost spikes",
            ],
            "dont": [
                "Wait for bill shock at month-end to discover runaway workloads",
                "Monitor only total project slots without breaking down by reservation or team",
            ],
            "example": (
                "# gcloud: create alert if allocated slots exceed 800 for 5 minutes\n"
                "gcloud alpha monitoring policies create \\\n"
                "  --display-name='BQ High Slot Utilisation' \\\n"
                "  --condition-filter='metric.type=\"bigquery.googleapis.com/slots/allocated_for_project\"' \\\n"
                "  --condition-threshold-value=800 \\\n"
                "  --condition-threshold-duration=300s \\\n"
                "  --notification-channels=projects/PROJECT/notificationChannels/CHANNEL_ID"
            ),
        },
        {
            "id": "MO-003",
            "title": "Attribute cost by team and workload with labels",
            "severity": "MEDIUM",
            "impact": "Cost Accountability",
            "description": (
                "BigQuery job labels propagate to Cloud Billing exports, enabling "
                "per-team, per-pipeline, and per-environment cost breakdown. "
                "Without labels, all costs appear as a single project-level line item."
            ),
            "do": [
                "Set labels on every programmatic query: {'team': 'data-eng', 'pipeline': 'daily-etl'}",
                "Enforce labeling via a wrapper function or middleware in your data platform",
                "Export Billing data to BigQuery and JOIN on labels for chargeback reporting",
            ],
            "dont": [
                "Run ad-hoc analyst queries without labels (hard to attribute retroactively)",
                "Use free-form label values without a schema (leads to inconsistent reporting)",
            ],
            "example": (
                "from google.cloud import bigquery\n\n"
                "job_config = bigquery.QueryJobConfig(\n"
                "    labels={\n"
                "        'team':        'analytics',\n"
                "        'pipeline':    'user-metrics',\n"
                "        'environment': 'production',\n"
                "    }\n"
                ")\n"
                "client.query(sql, job_config=job_config)"
            ),
        },
        {
            "id": "MO-004",
            "title": "Monitor and alert on query errors and failed jobs",
            "severity": "HIGH",
            "impact": "Reliability",
            "description": (
                "Failed BigQuery jobs are silent by default unless you explicitly monitor "
                "error_result in INFORMATION_SCHEMA or configure log-based alerts. "
                "Pipeline failures left undetected lead to stale data and broken dashboards."
            ),
            "do": [
                "Query INFORMATION_SCHEMA.JOBS WHERE error_result IS NOT NULL daily",
                "Create a log-based metric on bigquery_resource.type with severity=ERROR",
                "Set up alerting for scheduled query failures via Cloud Scheduler + Pub/Sub",
                "Include error_result.message in Slack/PagerDuty alert payloads for fast triage",
            ],
            "dont": [
                "Assume a scheduled query succeeded just because it ran",
                "Ignore quota exceeded errors (resourcesExceeded) – they indicate under-provisioning",
            ],
            "example": (
                "-- Find failed jobs in the last hour with their error messages\n"
                "SELECT\n"
                "  job_id,\n"
                "  user_email,\n"
                "  creation_time,\n"
                "  error_result.reason  AS error_reason,\n"
                "  error_result.message AS error_message\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)\n"
                "  AND error_result IS NOT NULL\n"
                "ORDER BY creation_time DESC"
            ),
        },
        {
            "id": "MO-005",
            "title": "Track query performance trends over time",
            "severity": "MEDIUM",
            "impact": "Performance Regression Detection",
            "description": (
                "A query that took 10 seconds last month but now takes 5 minutes signals "
                "data growth, schema drift, or missing partition filters. Track p50/p95 "
                "query duration and bytes_billed per query signature over time."
            ),
            "do": [
                "Normalise SQL with query fingerprinting (strip literals) to group identical queries",
                "Build a weekly trend dashboard: avg_duration, avg_bytes_billed by query hash",
                "Alert when a specific query's bytes_billed grows > 2x its 30-day baseline",
                "Use INFORMATION_SCHEMA.JOBS.query_info.resource_warning for BQ-generated hints",
            ],
            "dont": [
                "Only monitor total project costs without drilling into individual query trends",
                "Ignore the resource_warning field – BQ uses it to flag inefficient queries",
            ],
            "example": (
                "-- Weekly p95 query duration trend by statement type\n"
                "SELECT\n"
                "  DATE_TRUNC(creation_time, WEEK)              AS week,\n"
                "  statement_type,\n"
                "  APPROX_QUANTILES(total_slot_ms, 100)[OFFSET(95)] / 1000 AS p95_slot_s,\n"
                "  COUNT(*)                                     AS query_count\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "  AND state = 'DONE'\n"
                "  AND error_result IS NULL\n"
                "GROUP BY 1, 2\n"
                "ORDER BY 1 DESC, 3 DESC"
            ),
        },
        {
            "id": "MO-006",
            "title": "Audit storage costs with INFORMATION_SCHEMA.TABLE_STORAGE",
            "severity": "MEDIUM",
            "impact": "Cost Visibility",
            "description": (
                "INFORMATION_SCHEMA.TABLE_STORAGE gives a per-table breakdown of "
                "active, long-term, time-travel, and fail-safe physical bytes. "
                "Use it to identify storage bloat, validate partition expiration, "
                "and decide whether to switch a dataset to physical billing."
            ),
            "do": [
                "Schedule a weekly query to flag tables where time_travel_physical_bytes > active_physical_bytes",
                "Identify tables with no expiration in scratch datasets",
                "Compare total_logical_bytes vs active_physical_bytes to find compression opportunities",
            ],
            "dont": [
                "Use table row counts or GB shown in the BQ console UI as billing proxies (they show logical bytes)",
                "Ignore fail_safe_physical_bytes — they contribute to costs for up to 7 days after deletion",
            ],
            "example": (
                "-- Top 20 tables by total storage cost contributors\n"
                "SELECT\n"
                "  table_schema,\n"
                "  table_name,\n"
                "  ROUND(total_logical_bytes / 1e9, 2)           AS logical_gb,\n"
                "  ROUND(active_physical_bytes / 1e9, 2)         AS active_physical_gb,\n"
                "  ROUND(time_travel_physical_bytes / 1e9, 2)    AS time_travel_gb,\n"
                "  ROUND(fail_safe_physical_bytes / 1e9, 2)      AS fail_safe_gb\n"
                "FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE\n"
                "ORDER BY total_logical_bytes DESC\n"
                "LIMIT 20"
            ),
        },
        {
            "id": "MO-007",
            "title": "Monitor reservation slot utilisation and queue wait times",
            "severity": "HIGH",
            "impact": "Performance & Capacity Planning",
            "description": (
                "For capacity-based BigQuery editions, sustained high slot utilisation "
                "causes queries to queue. INFORMATION_SCHEMA.JOBS exposes "
                "total_slot_ms and creation_time vs start_time, making it possible "
                "to detect queue wait. Cloud Monitoring metrics provide real-time slot "
                "dashboards for proactive autoscaling decisions."
            ),
            "do": [
                "Track queue wait: TIMESTAMP_DIFF(start_time, creation_time, SECOND) per reservation",
                "Alert on Cloud Monitoring metric bigquery.googleapis.com/slots/allocated_for_project > 80% of baseline",
                "Use the BigQuery slot recommender to validate reservation sizes quarterly",
                "Set autoscaling max_slots as a safety cap to prevent unexpected cost overruns",
            ],
            "dont": [
                "Set max_slots equal to baseline_slots (leaves no room for burst capacity)",
                "Purchase slot commitments without reviewing the slot recommender output",
            ],
            "example": (
                "-- Detect queue wait time by reservation\n"
                "SELECT\n"
                "  reservation_id,\n"
                "  DATE(creation_time)                                          AS day,\n"
                "  APPROX_QUANTILES(\n"
                "    TIMESTAMP_DIFF(start_time, creation_time, SECOND), 100\n"
                "  )[OFFSET(95)]                                                AS p95_queue_s,\n"
                "  COUNT(*)                                                     AS job_count\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "GROUP BY 1, 2\n"
                "ORDER BY p95_queue_s DESC"
            ),
        },
    ],
}
