# QostEx — Full Feature Reference

> **32 tools · 81 practices · 13 categories**
> Last updated: 2026-03-20

---

## Table of Contents

1. [Overview](#overview)
2. [Knowledge Base Tools](#knowledge-base-tools) *(no GCP required)*
3. [Query Intelligence Tools](#query-intelligence-tools) *(no GCP required)*
4. [GCP Live Tools — Connection & Schema](#gcp-live-tools--connection--schema)
5. [GCP Live Tools — Query Execution & Cost](#gcp-live-tools--query-execution--cost)
6. [GCP Live Tools — Observability & Governance](#gcp-live-tools--observability--governance)
7. [GCP Live Tools — Data Quality & Schema Governance](#gcp-live-tools--data-quality--schema-governance)
8. [GCP Live Tools — Materialized Views & Job Management](#gcp-live-tools--materialized-views--job-management)
9. [GCP Live Tools — Intelligence & Diagnostics](#gcp-live-tools--intelligence--diagnostics)
10. [Resources](#resources)
11. [Practice Knowledge Base](#practice-knowledge-base)
12. [GCP Authentication](#gcp-authentication)

---

## Overview

QostEx is a Model Context Protocol (MCP) server that gives AI assistants (Claude, etc.) deep BigQuery intelligence — from curated best-practice knowledge to live GCP query execution, cost analysis, and schema exploration.

| Dimension | Count |
|---|---:|
| Total tools | **32** |
| Tools (no GCP required) | **9** |
| Tools (GCP credentials required) | **23** |
| MCP Resources | **2** |
| Practice categories | **13** |
| Total practices | **81** |

---

## Knowledge Base Tools

> No GCP credentials needed. Works offline against the built-in practice library.

### `resolve_topic`
Resolve a natural-language BigQuery topic to the most relevant practice IDs.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | string | required | Topic or question, e.g. `"reduce query cost"` |
| `top_k` | integer | 5 | Max results to return (max 20) |

**Returns:** Ranked list of practice IDs with relevance scores.
**Workflow:** Call this first, then call `get_practices` for full content.

---

### `get_practices`
Retrieve best-practice content within a token budget — assembles the most relevant practices in relevance order.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `topic` | string | required | Topic to retrieve practices for |
| `max_tokens` | integer | 3000 | Approximate token budget |
| `practice_ids` | array | — | Fetch specific IDs instead of resolving by topic |

**Returns:** Markdown content of matching practices.

---

### `get_best_practices`
Retrieve all practices for a single category.

| Parameter | Type | Description |
|---|---|---|
| `category` | enum | One of the 13 categories (e.g. `query_optimization`, `security`) |

**Returns:** Full content of every practice in the category.

---

### `search_practices`
Full-text keyword search across all 81 practices.

| Parameter | Type | Description |
|---|---|---|
| `query` | string | Keyword(s) to search |

**Returns:** Matching practice IDs, titles, and description snippets.

---

### `get_practice_detail`
Get the complete detail for a single practice by ID.

| Parameter | Type | Description |
|---|---|---|
| `practice_id` | string | e.g. `QO-001`, `SD-003` |

**Returns:** Full JSON for the practice including do/don't/example.

---

### `list_all_practice_ids`
Return a compact index of all 81 practice IDs and titles, grouped by category.

*No parameters.*

---

## Query Intelligence Tools

> Static SQL analysis — no GCP credentials needed.

### `review_query`
Analyse a SQL query against 10 best-practice checks and emit a corrected SQL snippet for each finding.

| Parameter | Type | Description |
|---|---|---|
| `sql` | string | The BigQuery SQL to review |

**Checks performed:**

| # | Check | Practice |
|---|---|---|
| 1 | `SELECT *` — unneeded columns | QO-001 |
| 2 | `CROSS JOIN` — cartesian product | QO-003 |
| 3 | `ORDER BY` without `LIMIT` | QO-004 |
| 4 | Non-deterministic functions (cache busters) | CO-002 |
| 5 | **No WHERE clause** — full table scan | QO-002 |
| 6 | **WHERE clause with no date/time partition filter** — partition pruning inactive | QO-002 |
| 7 | `NOT IN (subquery)` — prefer NOT EXISTS | QO-005 |
| 8 | Deeply nested subqueries — suggest CTEs | QO-004 |
| 9 | `HAVING` without `GROUP BY` | QO-004 |
| 10 | `COUNT(DISTINCT)` — suggest APPROX variant | QO-006 |
| 11 | **Derived table on left of JOIN** — JOIN order issue | QO-003 |
| 12 | **RIGHT JOIN** — suggest LEFT JOIN rewrite | QO-003 |

**Returns:** Numbered findings, each with a `Suggested fix:` SQL code block.

---

### `generate_cte_refactor`
Detect deeply nested subqueries and suggest a CTE (WITH clause) based rewrite.

| Parameter | Type | Description |
|---|---|---|
| `sql` | string | The SQL to refactor |

**Returns:** CTE structure template with inline comments.

---

### `suggest_materialized_view`
Score a SQL query for MV suitability and generate the `CREATE MATERIALIZED VIEW` DDL.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sql` | string | required | Query to evaluate |
| `dataset_id` | string | `my_dataset` | Target dataset for the generated DDL |

**Scoring signals:** GROUP BY, aggregate functions, JOINs, WHERE filters, subquery depth.
**Returns:** Recommendation level (highly recommended / recommended / worth considering), generated DDL, and key considerations.

---

## GCP Live Tools — Connection & Schema

> Require GCP credentials. See [GCP Authentication](#gcp-authentication).

### `check_gcp_connection`
Verify credentials are configured and working. Run this first.

*No parameters.*

**Returns:** Connected project name and sample dataset names, or detailed error with setup instructions.

---

### `explore_schema`
Browse GCP project structure interactively.

| Parameter | Type | Description |
|---|---|---|
| `dataset_id` | string | Dataset to inspect (omit to list all datasets) |
| `table_id` | string | Table to inspect (requires `dataset_id`) |
| `project_id` | string | GCP project (defaults to authenticated project) |

**Behaviour:**
- No args → list all datasets
- `dataset_id` only → list tables in dataset
- `dataset_id` + `table_id` → show full column schema, partition key, cluster columns

---

### `get_table_info`
Get metadata for a BigQuery table.

| Parameter | Type | Description |
|---|---|---|
| `table_ref` | string | `project.dataset.table` or `dataset.table` |

**Returns:** Row count, size (GB), partition key + TTL, clustering columns, created/modified timestamps, staleness description, and estimated full-scan cost in USD.

---

### `compare_tables`
Diff schemas between two BigQuery tables.

| Parameter | Type | Description |
|---|---|---|
| `table_a` | string | First table reference |
| `table_b` | string | Second table reference |

**Returns:** Type/mode differences, columns only in A, columns only in B, partition and clustering differences.

---

## GCP Live Tools — Query Execution & Cost

### `dry_run_query`
Dry-run a SQL query — estimates bytes and cost without executing.

| Parameter | Type | Description |
|---|---|---|
| `sql` | string | The query to dry-run |

**Returns:** Bytes processed, cost in USD. No data returned, no charges incurred.

---

### `estimate_query_cost`
Friendly cost tier wrapper around dry-run.

| Parameter | Type | Description |
|---|---|---|
| `sql` | string | The query to estimate |

**Returns:** Cost in USD with tier label (negligible / low / moderate / high / very high) and cost-reduction tips when the estimate is significant.

---

### `execute_query`
Execute a SQL query with a safety byte-billed cap.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sql` | string | required | Query to execute |
| `max_rows` | integer | 100 | Row cap on returned results |
| `max_bytes_billed` | integer | 1,000,000,000 | Safety cap (1 GB default) |

**Returns:** Results as a markdown table with bytes scanned and estimated cost.

---

## GCP Live Tools — Observability & Governance

### `query_history`
Analyse recent query history from `INFORMATION_SCHEMA.JOBS_BY_PROJECT`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | integer | 7 | Lookback window |
| `top_n` | integer | 10 | Number of results |
| `region` | string | `us` | BQ region slug |

**Returns:** Top users by estimated cost with job count, total GB, avg/max duration.

---

### `get_cost_attribution`
Break down BigQuery spend by user or label.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | integer | 30 | Lookback window |
| `group_by` | enum | `user` | `user` or `label` |
| `region` | string | `us` | BQ region slug |

**Returns:** Cost breakdown per group with GB processed and total USD.

---

### `get_expensive_queries`
Surface the top N most expensive queries with SQL snippets.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | integer | 7 | Lookback window |
| `top_n` | integer | 10 | Number of queries |
| `region` | string | `us` | BQ region slug |

**Returns:** Ranked list with cost, GB, duration, user, job ID, and SQL snippet.

---

### `get_slot_utilization`
Show slot-hours consumed per reservation over a time window.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | integer | 7 | Lookback window |
| `region` | string | `us` | BQ region slug |

**Returns:** Per-reservation slot-hours and job count. On-demand jobs shown as `(on-demand)`.

---

### `detect_zombie_queries`
Find recurring unlabeled queries — automated jobs with no owner that silently accumulate cost.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | integer | 30 | Lookback window |
| `min_runs` | integer | 5 | Minimum runs to qualify as recurring |
| `region` | string | `us` | BQ region slug |

**Returns:** Run count, total cost, user, and SQL snippet for each zombie query, plus remediation guidance.

---

### `detect_performance_regression`
Compare query performance between a recent and baseline time window. Answers: *"this query was fast last week — what changed?"*

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days_recent` | integer | 7 | Size of the recent window |
| `days_baseline` | integer | 7 | Size of the baseline window |
| `min_runs` | integer | 3 | Minimum runs required in each window |
| `region` | string | `us` | BQ region slug |

**How it works:** Normalises and MD5-hashes query SQL for stable fingerprinting, then compares avg bytes processed and avg duration between windows. Flags queries with 30%+ regression.

**Returns:** Regressing queries with % increase in bytes/duration, SQL snippet, and common cause checklist.

---

## GCP Live Tools — Data Quality & Schema Governance

### `check_data_freshness`
Report how old a table's data is and flag it as STALE if it exceeds a threshold.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `table_ref` | string | required | Table reference |
| `stale_hours` | integer | 24 | Hours before STALE status is triggered |

**Returns:** FRESH/STALE status, last modified time, age, row count, and partition info.

---

### `detect_schema_drift`
Compare an expected schema (JSON array) against the actual BigQuery table schema.

| Parameter | Type | Description |
|---|---|---|
| `table_ref` | string | Table to inspect |
| `expected_schema_json` | string | JSON array: `[{"name": "col", "type": "INT64"}, ...]` |

**Returns:** Missing columns, extra columns, and type/mode mismatches.

---

### `suggest_schema_improvements`
Cross-reference a table's real schema against QostEx best practices and return actionable suggestions.

| Parameter | Type | Description |
|---|---|---|
| `table_ref` | string | Table reference |

**Checks performed:**
- No partitioning on tables > 1 GB → PT-001
- No clustering defined → SD-002
- STRING columns whose name implies a date/time → SD-003
- NULLABLE columns that look like primary keys → SD-004
- Very wide tables (> 50 columns) with no nesting → SD-005
- No ARRAY/REPEATED columns on wide schemas → SD-006

---

## GCP Live Tools — Materialized Views & Job Management

### `list_materialized_views`
List all materialized views in a dataset with refresh status.

| Parameter | Type | Description |
|---|---|---|
| `dataset_id` | string | Dataset to inspect |
| `project_id` | string | GCP project (optional) |

**Returns:** Per-MV: last refresh time, age, auto-refresh setting, refresh interval, size, and query definition.

---

### `list_jobs`
List recent or currently running BigQuery jobs.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | enum | `RUNNING` | `RUNNING`, `DONE`, or `PENDING` |
| `max_results` | integer | 20 | Number of jobs to return |

**Returns:** Job ID, type, user, and created timestamp for each job.

---

### `cancel_job`
Cancel a running BigQuery job.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `job_id` | string | required | Job ID to cancel |
| `location` | string | `US` | Job location |

---

### `profile_table`
Generate column-level statistics using TABLESAMPLE.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `table_ref` | string | required | Table reference |
| `sample_percent` | integer | 5 | % of rows to sample (1–100) |

**Returns per column:** non-null count, distinct count, min/max/avg (numeric), min/max length (string).

---

### `explain_query_plan`
Parse the execution plan of a completed job and surface bottlenecks.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `job_id` | string | required | Completed job ID |
| `location` | string | `US` | Job location |

**Returns:** Stage-by-stage table (records in/out, compute ms, parallel inputs, step kinds), auto-detected bottlenecks (dominant compute stages, 99%+ filter stages), bytes processed vs billed.

---

## GCP Live Tools — Intelligence & Diagnostics

### `map_table_lineage`
Build an upstream/downstream dependency graph for a table by parsing SQL from job history. No Dataplex or dbt required.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `table_ref` | string | required | Table reference |
| `days` | integer | 30 | Lookback window |
| `region` | string | `us` | BQ region slug |
| `direction` | enum | `both` | `upstream`, `downstream`, or `both` |

**How it works:** Queries `INFORMATION_SCHEMA.JOBS_BY_PROJECT` for `referenced_tables` and `destination_table` in all jobs that mention the target table, then aggregates dependency counts.

**Returns:** Upstream and downstream tables ranked by job count.

---

### `nl_to_sql`
Fetch real schema for a dataset/tables and return structured context — enabling accurate BigQuery SQL generation grounded in actual column names, partition keys, and clustering fields.

| Parameter | Type | Description |
|---|---|---|
| `description` | string | Natural language query description, e.g. `"total revenue per country for last 30 days"` |
| `dataset_id` | string | Dataset to pull schema from |
| `table_ids` | array | Specific table names (optional; first 10 tables fetched if omitted) |
| `project_id` | string | GCP project (optional) |

**Returns:** Full schema with column types, partition hints, clustering hints, and notes on which filters to use for partition pruning.

---

## Resources

MCP Resources are browsable, static assets that clients can read.

| URI | Description |
|---|---|
| `bigquery://overview` | High-level summary of all 13 categories and practice counts |
| `bigquery://<category>` | Full JSON for any practice category (e.g. `bigquery://query_optimization`) |
| `bigquery://prompt` | Ready-made system prompt snippet — paste into Claude project instructions to auto-activate QostEx |

---

## Practice Knowledge Base

**81 practices across 13 categories.**

| Category | Prefix | Practices | Key Topics |
|---|---|---:|---|
| Query Optimization | `QO` | 12 | SELECT *, partition filters, CTEs, JOIN order, APPROX functions |
| Schema Design | `SD` | 8 | Partitioning, clustering, nested/repeated fields, data types |
| Cost Management | `CO` | 9 | Slot reservations, query caching, on-demand vs flat-rate |
| Security & Access Control | `SE` | 8 | IAM, VPC-SC, data masking, audit logs |
| Materialized Views | `MV` | 7 | MV creation, refresh, smart tuning, incremental |
| Monitoring | `MO` | 7 | INFORMATION_SCHEMA, alerting, dashboards |
| Data Ingestion | `DI` | 6 | Streaming vs batch, load jobs, Dataflow |
| Workload Management | `WM` | 5 | Reservations, slot commitments, queues |
| Partitioning & Expiration | `PT` | 4 | DATE/TIMESTAMP/integer-range partitioning, TTL |
| BI Engine | `BE` | 3 | Table pinning, fallback behavior, right-sizing |
| Storage Pricing | `SP` | 3 | Long-term storage, physical billing, TTL |
| Authorized Views | `AV` | 4 | Row-level security, column policy tags, SESSION_USER() |
| Scheduled Queries | `SQ` | 5 | Labeling, incremental writes, Dataform assertions, staggering |

---

## GCP Authentication

QostEx tries credentials in this order:

| Priority | Method | How to set |
|---|---|---|
| 1 | Inline JSON (best for MCP config) | `GCP_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'` |
| 2 | Key file path | `GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json` |
| 3 | Application Default Credentials | `gcloud auth application-default login` |

**Claude Desktop / MCP config example:**

```json
{
  "mcpServers": {
    "qostex": {
      "command": "python3",
      "args": ["server.py"],
      "env": {
        "GCP_SERVICE_ACCOUNT_JSON": "{...paste service account JSON here...}"
      }
    }
  }
}
```

Run `check_gcp_connection` after configuring to verify the connection.

---

## Tool Quick Reference

| Tool | GCP? | Category |
|---|:---:|---|
| `resolve_topic` | — | Knowledge Base |
| `get_practices` | — | Knowledge Base |
| `get_best_practices` | — | Knowledge Base |
| `search_practices` | — | Knowledge Base |
| `get_practice_detail` | — | Knowledge Base |
| `list_all_practice_ids` | — | Knowledge Base |
| `review_query` | — | Query Intelligence |
| `generate_cte_refactor` | — | Query Intelligence |
| `suggest_materialized_view` | — | Query Intelligence |
| `check_gcp_connection` | Yes | Connection |
| `explore_schema` | Yes | Schema |
| `get_table_info` | Yes | Schema |
| `compare_tables` | Yes | Schema |
| `suggest_schema_improvements` | Yes | Schema |
| `dry_run_query` | Yes | Cost |
| `estimate_query_cost` | Yes | Cost |
| `execute_query` | Yes | Execution |
| `query_history` | Yes | Observability |
| `get_cost_attribution` | Yes | Observability |
| `get_expensive_queries` | Yes | Observability |
| `get_slot_utilization` | Yes | Observability |
| `detect_zombie_queries` | Yes | Governance |
| `detect_performance_regression` | Yes | Diagnostics |
| `check_data_freshness` | Yes | Data Quality |
| `detect_schema_drift` | Yes | Data Quality |
| `profile_table` | Yes | Data Quality |
| `list_materialized_views` | Yes | Materialized Views |
| `list_jobs` | Yes | Job Management |
| `cancel_job` | Yes | Job Management |
| `explain_query_plan` | Yes | Diagnostics |
| `map_table_lineage` | Yes | Lineage |
| `nl_to_sql` | Yes | Intelligence |
