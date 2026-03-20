# QostEx — User Guide

> Everything you need to install, configure, and use QostEx in Claude Desktop or any MCP-compatible client.

---

## Table of Contents

1. [What is QostEx?](#what-is-qostex)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Connecting to Claude Desktop](#connecting-to-claude-desktop)
5. [GCP Authentication Setup](#gcp-authentication-setup)
6. [Verifying the Setup](#verifying-the-setup)
7. [Using QostEx — Workflow Guide](#using-qostex--workflow-guide)
   - [No-GCP Mode (Best Practices Only)](#no-gcp-mode-best-practices-only)
   - [GCP Mode (Live Tools)](#gcp-mode-live-tools)
8. [Tool-by-Tool Usage Examples](#tool-by-tool-usage-examples)
9. [Auto-Activate via System Prompt](#auto-activate-via-system-prompt)
10. [Environment Variables](#environment-variables)
11. [Troubleshooting](#troubleshooting)

---

## What is QostEx?

QostEx is a **Model Context Protocol (MCP) server** that gives Claude (and other AI assistants) deep BigQuery intelligence:

- **Best-practice knowledge base** — 81 curated practices across 13 categories, searchable and token-budgeted
- **SQL reviewer** — static linting with auto-fix suggestions, comment-aware
- **Schema-aware SQL review** — checks your actual partition/cluster columns from live BigQuery metadata
- **Live GCP tools** — dry-run queries, explore schemas, profile tables, track costs, detect zombie queries, map lineage, and more

Two modes:
- **No-GCP mode** — works immediately, no credentials needed
- **GCP mode** — unlocks 23 additional live tools, requires a service account or ADC

---

## Prerequisites

- Python 3.11 or higher
- Claude Desktop (or any MCP-compatible client)
- Git

For GCP tools (optional):
- A Google Cloud project with BigQuery enabled
- A service account with **BigQuery Data Viewer** + **BigQuery Job User** roles, OR `gcloud` CLI with ADC configured

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/sreekanth-kc/BqForge.git
cd QostEx
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 3. Verify the server starts

```bash
python3 server.py
```

You should see the server start and wait for input. Press `Ctrl+C` to stop. If it starts without errors, the installation is complete.

---

## Connecting to Claude Desktop

### Step 1 — Find your Claude Desktop config file

| Platform | Path |
|---|---|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |

### Step 2 — Add QostEx to the config

Open the file and add the `qostex` entry under `mcpServers`:

```json
{
  "mcpServers": {
    "qostex": {
      "command": "/absolute/path/to/QostEx/.venv/bin/python3",
      "args": ["/absolute/path/to/QostEx/server.py"]
    }
  }
}
```

**Important:** Use absolute paths. Replace `/absolute/path/to/QostEx` with your actual path.

**Example on macOS:**
```json
{
  "mcpServers": {
    "qostex": {
      "command": "/Users/yourname/QostEx/.venv/bin/python3",
      "args": ["/Users/yourname/QostEx/server.py"]
    }
  }
}
```

### Step 3 — Restart Claude Desktop

Fully quit and reopen Claude Desktop. QostEx tools will appear in the tool list.

---

## GCP Authentication Setup

QostEx tries credentials in this priority order:

| Priority | Method | Best for |
|---|---|---|
| 1 | `GCP_SERVICE_ACCOUNT_JSON` env var — inline JSON string | Claude Desktop, CI/CD, any shared setup |
| 2 | `GOOGLE_APPLICATION_CREDENTIALS` env var — path to key file | Local dev with a downloaded key file |
| 3 | Application Default Credentials (ADC) | Local dev with `gcloud` CLI |

### Option A — Service Account JSON (Recommended)

**Step 1:** Create a service account in GCP Console:
- IAM & Admin → Service Accounts → Create
- Grant roles: `BigQuery Data Viewer` + `BigQuery Job User`
- Create a JSON key and download it

**Step 2:** Add it to your Claude Desktop config as an env var:

```json
{
  "mcpServers": {
    "qostex": {
      "command": "/Users/yourname/QostEx/.venv/bin/python3",
      "args": ["/Users/yourname/QostEx/server.py"],
      "env": {
        "GCP_SERVICE_ACCOUNT_JSON": "{\"type\":\"service_account\",\"project_id\":\"my-project\",\"private_key_id\":\"...\",\"private_key\":\"-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----\\n\",\"client_email\":\"qostex@my-project.iam.gserviceaccount.com\", ...}"
      }
    }
  }
}
```

> **Tip:** To paste the JSON cleanly, read the file and minify it first:
> ```bash
> cat sa-key.json | python3 -m json.tool --compact
> ```
> Then paste the output as the value of `GCP_SERVICE_ACCOUNT_JSON`.

### Option B — Key File Path

```json
{
  "mcpServers": {
    "qostex": {
      "command": "/Users/yourname/QostEx/.venv/bin/python3",
      "args": ["/Users/yourname/QostEx/server.py"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/Users/yourname/keys/sa-key.json"
      }
    }
  }
}
```

### Option C — Application Default Credentials (local dev only)

```bash
gcloud auth application-default login
```

No env vars needed — the SDK picks up ADC automatically. This only works on your local machine where `gcloud` is installed.

---

## Verifying the Setup

After restarting Claude Desktop, ask:

```
Check my GCP connection using QostEx
```

Claude will call `check_gcp_connection`. You should see:

```
Connected to GCP project: `my-project`
Sample datasets: analytics, raw_data, reporting…
```

If you see an error, check the [Troubleshooting](#troubleshooting) section.

---

## Using QostEx — Workflow Guide

### No-GCP Mode (Best Practices Only)

These tools work immediately without any credentials.

#### Look up best practices for a topic

```
What are BigQuery best practices for reducing query cost?
```

Claude will call `resolve_topic` → `get_practices` and return ranked, token-budgeted content.

#### Review a SQL query

```
Review this BigQuery query for best-practice issues:

SELECT * FROM `my_dataset.events`
WHERE user_id = 123
ORDER BY event_ts
```

Claude will call `review_query` and return numbered findings with SQL fix snippets.

#### Search for a specific practice

```
Search QostEx practices for "clustering"
```

#### Get all practices for a category

```
Show me all schema design practices from QostEx
```

---

### GCP Mode (Live Tools)

#### Explore your schema

```
Show me all datasets in my GCP project
```

```
List tables in the analytics dataset
```

```
Show me the schema for analytics.events — including partition and cluster info
```

#### Estimate query cost before running

```
What will this query cost to run?

SELECT user_id, COUNT(*) as events
FROM `my_project.analytics.events`
WHERE event_type = 'purchase'
GROUP BY user_id
```

Claude will call `estimate_query_cost` and return the cost tier and tips.

#### Run a query safely

```
Run this query and show me the first 50 rows:

SELECT date, SUM(revenue) as total
FROM `analytics.daily_revenue`
WHERE DATE(event_ts) >= '2024-01-01'
GROUP BY date
ORDER BY date DESC
LIMIT 50
```

The safety cap is 1 GB by default — increase with `max_bytes_billed` if needed.

#### Check who is spending the most

```
Who are the top 5 users by BigQuery spend this month?
```

```
Show me the 10 most expensive queries from the last 7 days
```

#### Detect zombie queries (unlabeled recurring jobs)

```
Are there any zombie queries running in my project? Look back 30 days.
```

#### Check data freshness

```
Is the analytics.events table fresh? Flag it if it hasn't been updated in 6 hours.
```

#### Detect schema drift

```
Check if analytics.events matches this expected schema:
[
  {"name": "event_id", "type": "STRING"},
  {"name": "user_id", "type": "INT64"},
  {"name": "event_ts", "type": "TIMESTAMP"},
  {"name": "event_type", "type": "STRING"}
]
```

#### Schema-aware SQL review (best way to review a query)

```
Review this query against the actual BigQuery schema:

SELECT user_id, SUM(amount) as total
FROM `my_project.analytics.orders`
WHERE status = 'completed'
GROUP BY user_id
```

Claude will call `review_query_with_schema`, fetch the real partition/cluster columns for `analytics.orders`, and tell you if your WHERE clause is actually pruning partitions.

#### Map table lineage

```
What tables depend on analytics.events? Show both upstream and downstream.
```

#### Detect performance regressions

```
Are any queries running slower or scanning more data than they were last week?
```

#### Explain a query's execution plan

```
Explain the execution plan for job ID bqjob_r123abc456_00000190abc_1
```

---

## Tool-by-Tool Usage Examples

| Ask Claude... | Tool called |
|---|---|
| "What are best practices for partitioning?" | `resolve_topic` → `get_practices` |
| "Search for practices about clustering" | `search_practices` |
| "Show me all practices for cost management" | `get_best_practices` |
| "What is practice QO-001?" | `get_practice_detail` |
| "Review this SQL for issues" | `review_query` |
| "Review this SQL against the real schema" | `review_query_with_schema` |
| "Is my GCP connection working?" | `check_gcp_connection` |
| "List my datasets" | `explore_schema` |
| "Show schema for dataset.table" | `explore_schema` |
| "How big is analytics.events? When was it last updated?" | `get_table_info` |
| "What columns does analytics.orders have?" | `explore_schema` |
| "Compare schemas of table_a and table_b" | `compare_tables` |
| "How much will this query cost?" | `estimate_query_cost` |
| "Dry run this query" | `dry_run_query` |
| "Run this query, max 100 rows" | `execute_query` |
| "Who is spending the most on BigQuery?" | `get_cost_attribution` |
| "Show me the most expensive queries this week" | `get_expensive_queries` |
| "Analyse query history for last 7 days" | `query_history` |
| "How are our slot reservations being used?" | `get_slot_utilization` |
| "Is analytics.orders fresh? Flag if stale > 12h" | `check_data_freshness` |
| "Check if this table matches the expected schema" | `detect_schema_drift` |
| "Suggest improvements for analytics.events schema" | `suggest_schema_improvements` |
| "Profile the analytics.orders table" | `profile_table` |
| "List materialized views in the reporting dataset" | `list_materialized_views` |
| "What jobs are currently running?" | `list_jobs` |
| "Cancel job bqjob_r123abc_00001" | `cancel_job` |
| "Explain the execution plan for job ID xyz" | `explain_query_plan` |
| "Find zombie queries — recurring jobs with no labels" | `detect_zombie_queries` |
| "What tables feed into analytics.events?" | `map_table_lineage` |
| "Have any queries gotten slower this week?" | `detect_performance_regression` |

---

## Auto-Activate via System Prompt

Instead of mentioning QostEx in every message, paste this into your **Claude Project Instructions** (Claude Desktop → Project → Instructions):

```
You have access to the QostEx MCP server for BigQuery best practices and live GCP tools.

When the user asks about BigQuery topics, writing queries, optimizing cost, or reviewing SQL:
1. Call resolve_topic(query="<topic>") to find relevant practice IDs
2. Call get_practices(topic="<topic>") to retrieve content within a token budget
3. Always cite practice IDs (e.g. QO-002, PT-001) in your recommendations

When the user shares SQL to review:
- Call review_query(sql="...") for static pattern checks
- If GCP is connected, also call review_query_with_schema(sql="...") for schema-aware partition checks

When the user asks about cost, schema, or job history:
- Use the appropriate GCP live tool (estimate_query_cost, explore_schema, get_expensive_queries, etc.)
```

After this, Claude will automatically use QostEx tools whenever BigQuery topics come up — no manual prompting needed.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | — | Inline service account JSON string (takes priority) |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to service account key file |
| `BQ_PRICE_PER_TB` | `6.25` | On-demand price per TB in USD — override for regional pricing |

**Example with custom pricing (for `europe-west1` region):**

```json
{
  "mcpServers": {
    "qostex": {
      "command": "/Users/yourname/QostEx/.venv/bin/python3",
      "args": ["/Users/yourname/QostEx/server.py"],
      "env": {
        "GCP_SERVICE_ACCOUNT_JSON": "{...}",
        "BQ_PRICE_PER_TB": "6.25"
      }
    }
  }
}
```

---

## Troubleshooting

### QostEx tools don't appear in Claude Desktop

1. Check that `claude_desktop_config.json` is valid JSON (paste it into [jsonlint.com](https://jsonlint.com))
2. Make sure you used **absolute paths** — relative paths don't work in MCP config
3. Fully quit Claude Desktop (Cmd+Q on macOS, not just close the window) and reopen
4. Check the Claude Desktop logs: `~/Library/Logs/Claude/mcp*.log`

### `check_gcp_connection` returns "GCP connection failed"

- Verify your service account has `BigQuery Data Viewer` and `BigQuery Job User` roles
- If using `GCP_SERVICE_ACCOUNT_JSON`, make sure the JSON is valid and fully on one line
- If using ADC, run `gcloud auth application-default login` again
- Check the project ID in your service account JSON matches your actual GCP project

### Tools like `query_history` or `get_expensive_queries` return no results

- These query `INFORMATION_SCHEMA.JOBS_BY_PROJECT` — the service account must have `bigquery.jobs.list` permission (included in `BigQuery Job User`)
- If your project is in a non-US region, pass `region="eu"` or the appropriate region slug

### Cost estimates seem wrong

Set `BQ_PRICE_PER_TB` to match your actual pricing (check GCP Console → Billing → BigQuery).

### `detect_zombie_queries` returns nothing

- The query looks for jobs with **zero labels** that ran 5+ times. If all your scheduled queries have labels, this is expected — it means your labeling hygiene is good.
- Lower `min_runs` to 2 to cast a wider net.

### `review_query_with_schema` says "Could not extract table references"

- Make sure your SQL uses standard `FROM dataset.table` or `FROM project.dataset.table` syntax
- Backtick-quoted names like `` FROM `project.dataset.table` `` are supported
- CTEs (WITH clauses) are supported but the CTE names themselves are filtered out — the tool looks for real table references only
