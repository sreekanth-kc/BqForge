WORKLOAD_MANAGEMENT = {
    "title": "Workload Management",
    "description": (
        "Best practices for managing BigQuery slot capacity, reservations, "
        "autoscaling, job concurrency, and workload isolation to ensure "
        "predictable performance and cost across teams and environments."
    ),
    "practices": [
        {
            "id": "WM-001",
            "title": "Isolate workloads with separate reservations",
            "severity": "HIGH",
            "impact": "Performance & Reliability",
            "description": (
                "Sharing a single slot pool across interactive queries, batch ETL, "
                "and development workloads causes priority inversion — a heavy "
                "overnight batch job can starve analyst queries. Use separate "
                "reservations with assignment rules to guarantee isolation."
            ),
            "do": [
                "Create reservations: prod-interactive, prod-batch, dev — each with its own slot budget",
                "Assign specific projects or folders to reservations using CREATE ASSIGNMENT",
                "Give prod-interactive a higher baseline; prod-batch can rely on autoscale burst",
                "Use INFORMATION_SCHEMA.RESERVATIONS to audit capacity allocation",
            ],
            "dont": [
                "Route all workloads to the DEFAULT reservation without assignments",
                "Allow dev/test workloads to compete with production interactive queries",
            ],
            "example": (
                "-- Create isolated reservations (BigQuery Admin SDK / bq CLI)\n"
                "bq mk --reservation --location=US --slots=500 prod-interactive\n"
                "bq mk --reservation --location=US --slots=200 prod-batch\n"
                "bq mk --reservation --location=US --slots=50  dev\n\n"
                "-- Assign a project to the interactive reservation\n"
                "bq mk --reservation_assignment \\\n"
                "  --reservation=projects/admin-proj/locations/US/reservations/prod-interactive \\\n"
                "  --job_type=QUERY \\\n"
                "  --assignee_type=PROJECT \\\n"
                "  --assignee_id=analytics-prod-project"
            ),
        },
        {
            "id": "WM-002",
            "title": "Configure autoscaling baselines and max slots correctly",
            "severity": "HIGH",
            "impact": "Cost & Performance",
            "description": (
                "BigQuery Edition reservations support autoscaling: slots scale up "
                "during burst and back down during idle periods. Setting baseline too "
                "high wastes money; setting max too low throttles queries during peaks. "
                "Use INFORMATION_SCHEMA history to size baselines and cloud monitoring "
                "slot metrics to set sensible max caps."
            ),
            "do": [
                "Set baseline_slots = p50 of historical slot consumption for the workload type",
                "Set max_slots = 2–3× p95 of historical peak consumption for burst headroom",
                "Monitor bigquery.googleapis.com/slots/allocated_for_project to validate sizing",
                "Reassess quarterly using the BigQuery slot recommender tool",
            ],
            "dont": [
                "Set max_slots = baseline_slots (no burst capacity, queries queue immediately at peak)",
                "Set max_slots to a very large number without a billing budget alert as a backstop",
            ],
            "example": (
                "-- Estimate p50 and p95 slot consumption to right-size reservations\n"
                "SELECT\n"
                "  reservation_id,\n"
                "  APPROX_QUANTILES(total_slot_ms / 1000, 100)[OFFSET(50)] AS p50_slot_s,\n"
                "  APPROX_QUANTILES(total_slot_ms / 1000, 100)[OFFSET(95)] AS p95_slot_s\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "GROUP BY reservation_id"
            ),
        },
        {
            "id": "WM-003",
            "title": "Set job timeouts and query priorities to prevent runaway jobs",
            "severity": "MEDIUM",
            "impact": "Cost & Reliability",
            "description": (
                "Uncontrolled long-running queries can exhaust slot capacity and inflate "
                "costs. Set job_timeout_ms to cancel queries exceeding an expected "
                "duration, and use INTERACTIVE vs BATCH priority to control whether "
                "a job counts against the concurrency quota."
            ),
            "do": [
                "Set job_timeout_ms = 600000 (10 min) on exploratory analyst queries",
                "Use BATCH priority for non-urgent ETL jobs to allow BQ to schedule opportunistically",
                "Set maximum_bytes_billed as a hard cost ceiling in addition to timeout",
                "Monitor timed-out jobs in INFORMATION_SCHEMA.JOBS WHERE error_result.reason = 'jobBackendError'",
            ],
            "dont": [
                "Leave no timeout on analyst-facing query endpoints (a single bad query can drain capacity)",
                "Run all workloads at INTERACTIVE priority regardless of urgency",
            ],
            "example": (
                "from google.cloud import bigquery\n\n"
                "job_config = bigquery.QueryJobConfig(\n"
                "    job_timeout_ms=600_000,          # Cancel after 10 minutes\n"
                "    maximum_bytes_billed=50 * 1e9,   # Hard 50 GB cost cap\n"
                "    priority=bigquery.QueryPriority.BATCH,  # For non-urgent ETL\n"
                "    labels={'team': 'data-eng', 'pipeline': 'nightly-etl'},\n"
                ")"
            ),
        },
        {
            "id": "WM-004",
            "title": "Use capacity commitments for predictable cost and performance",
            "severity": "MEDIUM",
            "impact": "Cost Governance",
            "description": (
                "Annual and 3-year slot commitments offer significant discounts "
                "(up to 25–50%) over monthly or flex commitments. Use FLEX commitments "
                "for burst windows (e.g., month-end reporting); use annual commitments "
                "for the stable baseline identified from 30-day slot history."
            ),
            "do": [
                "Start with FLEX commitments to validate slot sizing before locking into annual",
                "Layer FLEX on top of ANNUAL commitments during predictable peak periods",
                "Review commitments quarterly in INFORMATION_SCHEMA.CAPACITY_COMMITMENTS",
                "Use the Cloud Billing export to measure actual cost vs on-demand equivalent",
            ],
            "dont": [
                "Purchase annual commitments before measuring actual slot baseline usage",
                "Forget to cancel FLEX commitments after temporary peak periods end",
            ],
            "example": (
                "-- View current capacity commitments\n"
                "SELECT\n"
                "  commitment_id,\n"
                "  slot_count,\n"
                "  plan,\n"
                "  state,\n"
                "  commitment_start_time,\n"
                "  commitment_end_time\n"
                "FROM `region-us`.INFORMATION_SCHEMA.CAPACITY_COMMITMENTS\n"
                "ORDER BY commitment_start_time DESC"
            ),
        },
        {
            "id": "WM-005",
            "title": "Label all jobs for cost attribution and workload analysis",
            "severity": "MEDIUM",
            "impact": "Cost Accountability",
            "description": (
                "Job labels are the only reliable way to attribute BigQuery costs to "
                "teams, pipelines, and environments in the Cloud Billing export. "
                "Without labels, all project costs appear as a single undifferentiated "
                "line item. Define a labelling schema and enforce it via platform wrappers."
            ),
            "do": [
                "Standardise label keys: team, pipeline, environment, product, cost_center",
                "Enforce labels programmatically in shared BigQuery client wrappers or dbt profiles",
                "Join billing export with INFORMATION_SCHEMA.JOBS labels for chargeback reports",
                "Alert on unlabelled jobs: WHERE labels IS NULL AND user_email NOT IN exclusion_list",
            ],
            "dont": [
                "Use free-form label values without an enforced vocabulary (breaks grouping)",
                "Rely on user_email alone for cost attribution (shared service accounts obscure ownership)",
            ],
            "example": (
                "-- Find top cost by team label over the last 30 days\n"
                "SELECT\n"
                "  labels['team']                          AS team,\n"
                "  ROUND(SUM(total_bytes_billed) / 1e12, 3) AS tb_billed,\n"
                "  COUNT(*)                                AS job_count\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,\n"
                "  UNNEST([labels]) AS kv\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "GROUP BY team\n"
                "ORDER BY tb_billed DESC"
            ),
        },
    ],
}
