"""
BigQuery Best Practices MCP Server (QostEx)
Exposes BigQuery best practices as Tools and Resources.

Two-step workflow:
  1. resolve_topic("reduce query cost")  → ranked list of practice IDs
  2. get_practices(topic=..., max_tokens=3000) → focused content within token budget
"""

import json
import re
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types
import gcp_tools
import sql_parser

# ─────────────────────────────────────────────
# Knowledge base
# ─────────────────────────────────────────────
from data.query_optimization import QUERY_OPTIMIZATION
from data.schema_design import SCHEMA_DESIGN
from data.cost_management import COST_MANAGEMENT
from data.security import SECURITY
from data.materialized_views import MATERIALIZED_VIEWS
from data.monitoring import MONITORING
from data.data_ingestion import DATA_INGESTION
from data.workload_management import WORKLOAD_MANAGEMENT
from data.partitioning import PARTITIONING
from data.bi_engine import BI_ENGINE
from data.storage_pricing import STORAGE_PRICING
from data.authorized_views import AUTHORIZED_VIEWS
from data.scheduled_queries import SCHEDULED_QUERIES

ALL_PRACTICES: dict[str, dict] = {
    "query_optimization": QUERY_OPTIMIZATION,
    "schema_design": SCHEMA_DESIGN,
    "cost_management": COST_MANAGEMENT,
    "security": SECURITY,
    "materialized_views": MATERIALIZED_VIEWS,
    "monitoring": MONITORING,
    "data_ingestion": DATA_INGESTION,
    "workload_management": WORKLOAD_MANAGEMENT,
    "partitioning": PARTITIONING,
    "bi_engine": BI_ENGINE,
    "storage_pricing": STORAGE_PRICING,
    "authorized_views": AUTHORIZED_VIEWS,
    "scheduled_queries": SCHEDULED_QUERIES,
}

# Pre-built flat index: id → (category, practice_dict)
_PRACTICE_INDEX: dict[str, tuple[str, dict]] = {
    p["id"]: (cat, p)
    for cat, data in ALL_PRACTICES.items()
    for p in data["practices"]
}

# Stop-words stripped before keyword scoring
_STOP_WORDS = {
    "how", "do", "i", "to", "the", "a", "an", "in", "for", "of", "is",
    "are", "my", "what", "should", "can", "best", "practice", "bigquery",
    "bq", "using", "use", "with", "from", "on", "and", "or", "at", "be",
    "this", "that", "it", "its", "when", "which", "why", "where",
}

# ─────────────────────────────────────────────
# Server setup
# ─────────────────────────────────────────────
server = Server("qostex")


# ═══════════════════════════════════════════════
# RESOURCES  – static, browsable documentation
# ═══════════════════════════════════════════════
@server.list_resources()
async def list_resources() -> list[types.Resource]:
    resources = []
    for key, data in ALL_PRACTICES.items():
        resources.append(
            types.Resource(
                uri=f"bigquery://{key}",
                name=data["title"],
                description=data["description"],
                mimeType="application/json",
            )
        )
    resources.append(
        types.Resource(
            uri="bigquery://overview",
            name="BigQuery Best Practices Overview",
            description="High-level overview of all QostEx best-practice categories.",
            mimeType="text/plain",
        )
    )
    resources.append(
        types.Resource(
            uri="bigquery://prompt",
            name="QostEx – System Prompt Snippet",
            description=(
                "Paste this into your system prompt to activate automatic QostEx "
                "lookups whenever BigQuery topics arise."
            ),
            mimeType="text/plain",
        )
    )
    return resources


@server.read_resource()
async def read_resource(uri: types.AnyUrl) -> str:
    uri_str = str(uri)

    if uri_str == "bigquery://overview":
        lines = ["# QostEx – BigQuery Best Practices Overview\n"]
        total = sum(len(d["practices"]) for d in ALL_PRACTICES.values())
        lines.append(f"**{total} practices across {len(ALL_PRACTICES)} categories.**\n")
        for key, data in ALL_PRACTICES.items():
            lines.append(f"## {data['title']}")
            lines.append(f"{data['description']}\n")
            lines.append(f"Resource URI : `bigquery://{key}`")
            lines.append(f"Practices    : {len(data['practices'])}\n")
        return "\n".join(lines)

    if uri_str == "bigquery://prompt":
        return _SYSTEM_PROMPT_SNIPPET

    key = uri_str.replace("bigquery://", "")
    if key not in ALL_PRACTICES:
        raise ValueError(f"Unknown resource: {uri_str}")
    return json.dumps(ALL_PRACTICES[key], indent=2)


# ═══════════════════════════════════════════════
# TOOLS  – callable functions
# ═══════════════════════════════════════════════
@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        # ── Step 1: resolve topic
        types.Tool(
            name="resolve_topic",
            description=(
                "Resolve a natural-language BigQuery topic or question to the most "
                "relevant best-practice IDs. Use this FIRST to discover which practice "
                "IDs apply, then call get_practices for the full content. "
                "Returns a ranked list with relevance scores."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language topic or question, e.g. 'reduce query cost' or 'PII column security'.",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default 5, max 20).",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        ),
        # ── Step 2: fetch practices
        types.Tool(
            name="get_practices",
            description=(
                "Retrieve focused BigQuery best-practice content for a topic, "
                "constrained to a token budget. Assembles the most relevant practices "
                "in relevance order until the budget is exhausted. "
                "Pair with resolve_topic for the full two-step workflow."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic or question to retrieve practices for.",
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Approximate token budget for the returned content (default 3000).",
                        "default": 3000,
                    },
                    "practice_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: fetch specific practice IDs instead of resolving by topic.",
                    },
                },
                "required": ["topic"],
            },
        ),
        # ── Existing tools
        types.Tool(
            name="get_best_practices",
            description=(
                "Retrieve ALL practices for a category. "
                "Categories: query_optimization, schema_design, cost_management, "
                "security, materialized_views, monitoring."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": list(ALL_PRACTICES.keys()),
                        "description": "The best-practice category to retrieve.",
                    }
                },
                "required": ["category"],
            },
        ),
        types.Tool(
            name="search_practices",
            description="Full-text keyword search across all BigQuery best practices.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Keyword(s) to search for.",
                    }
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_practice_detail",
            description="Get full detail for a single best-practice rule by its ID (e.g. 'QO-001').",
            inputSchema={
                "type": "object",
                "properties": {
                    "practice_id": {
                        "type": "string",
                        "description": "The practice ID, e.g. 'QO-001'.",
                    }
                },
                "required": ["practice_id"],
            },
        ),
        types.Tool(
            name="review_query",
            description=(
                "Analyse a SQL query and surface relevant BigQuery best-practice "
                "warnings and suggestions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The BigQuery SQL query to review.",
                    }
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="list_all_practice_ids",
            description="Return a compact list of every practice ID + title across all categories.",
            inputSchema={"type": "object", "properties": {}},
        ),

        # ── GCP-powered tools (require credentials)
        types.Tool(
            name="check_gcp_connection",
            description=(
                "Verify that GCP credentials are configured and working. "
                "Run this first before using any other GCP tools."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="dry_run_query",
            description=(
                "Dry-run a BigQuery SQL query to estimate bytes processed and cost "
                "without actually executing it. No data is returned, no charges incurred."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The BigQuery SQL query to dry-run."},
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="explore_schema",
            description=(
                "Browse GCP project structure. "
                "No args → list datasets. "
                "dataset_id → list tables. "
                "dataset_id + table_id → show columns, partition, and cluster info."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string", "description": "Dataset to inspect (optional)."},
                    "table_id": {"type": "string", "description": "Table to inspect (optional, requires dataset_id)."},
                    "project_id": {"type": "string", "description": "GCP project ID (defaults to authenticated project)."},
                },
            },
        ),
        types.Tool(
            name="get_table_info",
            description=(
                "Get metadata for a BigQuery table: row count, size, partitioning, "
                "clustering, last modified time, and estimated full-scan cost."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {
                        "type": "string",
                        "description": "Table reference: project.dataset.table or dataset.table",
                    },
                },
                "required": ["table_ref"],
            },
        ),
        types.Tool(
            name="execute_query",
            description=(
                "Execute a BigQuery SQL query and return results as a table. "
                "Includes a safety cap on bytes billed (default 1 GB)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SQL query to execute."},
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum rows to return (default 100).",
                        "default": 100,
                    },
                    "max_bytes_billed": {
                        "type": "integer",
                        "description": "Safety cap on bytes billed in bytes (default 1 GB = 1000000000).",
                        "default": 1000000000,
                    },
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="query_history",
            description=(
                "Analyse recent BigQuery query history from INFORMATION_SCHEMA. "
                "Returns top users by estimated cost, job counts, and avg/max durations."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Lookback window in days (default 7).",
                        "default": 7,
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of results to show (default 10).",
                        "default": 10,
                    },
                    "region": {
                        "type": "string",
                        "description": "BigQuery region slug (default 'us').",
                        "default": "us",
                    },
                },
            },
        ),
        types.Tool(
            name="get_cost_attribution",
            description=(
                "Break down BigQuery spend by user or label over a time window. "
                "Useful for identifying who or what is driving costs."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Lookback window in days (default 30).",
                        "default": 30,
                    },
                    "group_by": {
                        "type": "string",
                        "enum": ["user", "label"],
                        "description": "Group costs by 'user' or 'label' (default 'user').",
                        "default": "user",
                    },
                    "region": {
                        "type": "string",
                        "description": "BigQuery region slug (default 'us').",
                        "default": "us",
                    },
                },
            },
        ),
        types.Tool(
            name="profile_table",
            description=(
                "Generate column-level statistics for a BigQuery table: "
                "non-null counts, distinct counts, min/max/avg for numeric columns, "
                "min/max string lengths for string columns. Uses TABLESAMPLE for speed."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {
                        "type": "string",
                        "description": "Table reference: project.dataset.table or dataset.table",
                    },
                    "sample_percent": {
                        "type": "integer",
                        "description": "Percentage of rows to sample (default 5, range 1-100).",
                        "default": 5,
                    },
                },
                "required": ["table_ref"],
            },
        ),
        types.Tool(
            name="list_jobs",
            description="List recent or currently running BigQuery jobs in the authenticated project.",
            inputSchema={
                "type": "object",
                "properties": {
                    "state": {
                        "type": "string",
                        "enum": ["RUNNING", "DONE", "PENDING"],
                        "description": "Filter by job state (default 'RUNNING').",
                        "default": "RUNNING",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of jobs to return (default 20).",
                        "default": 20,
                    },
                },
            },
        ),
        types.Tool(
            name="cancel_job",
            description="Cancel a running BigQuery job by its job ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "The BigQuery job ID to cancel."},
                    "location": {
                        "type": "string",
                        "description": "Job location (default 'US').",
                        "default": "US",
                    },
                },
                "required": ["job_id"],
            },
        ),
        types.Tool(
            name="estimate_query_cost",
            description=(
                "Dry-run a query and return a human-friendly cost estimate with tier label "
                "(negligible / low / moderate / high) and cost-reduction tips when the estimate is high."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SQL query to estimate."},
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="get_expensive_queries",
            description=(
                "Surface the top N most expensive queries from INFORMATION_SCHEMA, "
                "including their SQL snippets, cost, bytes processed, and user."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Lookback window in days (default 7).", "default": 7},
                    "top_n": {"type": "integer", "description": "Number of queries to return (default 10).", "default": 10},
                    "region": {"type": "string", "description": "BQ region slug (default 'us').", "default": "us"},
                },
            },
        ),
        types.Tool(
            name="get_slot_utilization",
            description=(
                "Show slot-hours consumed per reservation over a time window. "
                "Useful for capacity planning and understanding flat-rate vs on-demand usage."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Lookback window in days (default 7).", "default": 7},
                    "region": {"type": "string", "description": "BQ region slug (default 'us').", "default": "us"},
                },
            },
        ),
        types.Tool(
            name="check_data_freshness",
            description=(
                "Report how old a table's data is and flag it as STALE if it exceeds a threshold. "
                "Useful for monitoring ingestion pipelines."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {"type": "string", "description": "Table reference: project.dataset.table or dataset.table"},
                    "stale_hours": {
                        "type": "integer",
                        "description": "Hours after which the table is considered stale (default 24).",
                        "default": 24,
                    },
                },
                "required": ["table_ref"],
            },
        ),
        types.Tool(
            name="detect_schema_drift",
            description=(
                "Compare an expected schema (JSON array) against the actual BigQuery table schema. "
                "Reports missing columns, extra columns, and type mismatches."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {"type": "string", "description": "Table to inspect."},
                    "expected_schema_json": {
                        "type": "string",
                        "description": (
                            'JSON array of expected columns, e.g. '
                            '[{"name": "user_id", "type": "INT64"}, {"name": "event_date", "type": "DATE"}]'
                        ),
                    },
                },
                "required": ["table_ref", "expected_schema_json"],
            },
        ),
        types.Tool(
            name="suggest_schema_improvements",
            description=(
                "Analyse a table's schema against QostEx best practices and return "
                "specific improvement suggestions (partitioning, clustering, data types, nesting)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {"type": "string", "description": "Table reference: project.dataset.table or dataset.table"},
                },
                "required": ["table_ref"],
            },
        ),
        types.Tool(
            name="compare_tables",
            description=(
                "Diff schemas between two BigQuery tables — reports type mismatches, "
                "missing/extra columns, and partition/cluster differences."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_a": {"type": "string", "description": "First table reference."},
                    "table_b": {"type": "string", "description": "Second table reference."},
                },
                "required": ["table_a", "table_b"],
            },
        ),
        types.Tool(
            name="list_materialized_views",
            description="List all materialized views in a dataset with refresh status, age, and query definition.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string", "description": "Dataset to inspect."},
                    "project_id": {"type": "string", "description": "GCP project (defaults to authenticated project)."},
                },
                "required": ["dataset_id"],
            },
        ),
        types.Tool(
            name="explain_query_plan",
            description=(
                "Parse the execution plan of a completed BigQuery job and surface "
                "stage-by-stage statistics, bottlenecks, and filtering efficiency."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "The BigQuery job ID to explain."},
                    "location": {"type": "string", "description": "Job location (default 'US').", "default": "US"},
                },
                "required": ["job_id"],
            },
        ),
        types.Tool(
            name="detect_zombie_queries",
            description=(
                "Find recurring unlabeled queries ('zombie jobs') — scheduled or automated queries "
                "with no owner labels that accumulate cost without accountability. "
                "Returns query snippets, run counts, and total estimated cost."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Lookback window in days (default 30).", "default": 30},
                    "min_runs": {"type": "integer", "description": "Minimum runs to qualify as recurring (default 5).", "default": 5},
                    "region": {"type": "string", "description": "BQ region slug (default 'us').", "default": "us"},
                },
            },
        ),
        types.Tool(
            name="map_table_lineage",
            description=(
                "Build a dependency graph for a table by parsing SQL from job history. "
                "Returns upstream tables (feeds into this table) and downstream tables (read from this table). "
                "No Dataplex or dbt required."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_ref": {"type": "string", "description": "Table reference: project.dataset.table or dataset.table"},
                    "days": {"type": "integer", "description": "Lookback window in days (default 30).", "default": 30},
                    "region": {"type": "string", "description": "BQ region slug (default 'us').", "default": "us"},
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "both"],
                        "description": "Which direction to map (default 'both').",
                        "default": "both",
                    },
                },
                "required": ["table_ref"],
            },
        ),
        types.Tool(
            name="detect_performance_regression",
            description=(
                "Compare query performance between a recent window and a baseline window. "
                "Flags queries where bytes processed or duration have increased by 30%+. "
                "Answers: 'this query was fast last week — what changed?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days_recent": {"type": "integer", "description": "Size of the recent window in days (default 7).", "default": 7},
                    "days_baseline": {"type": "integer", "description": "Size of the baseline window in days (default 7).", "default": 7},
                    "min_runs": {"type": "integer", "description": "Minimum runs required in each window (default 3).", "default": 3},
                    "region": {"type": "string", "description": "BQ region slug (default 'us').", "default": "us"},
                },
            },
        ),
        types.Tool(
            name="review_query_with_schema",
            description=(
                "Schema-aware SQL review. Extracts tables from the query, fetches their actual "
                "partition and clustering columns from BigQuery, and checks whether the WHERE clause "
                "filters on the REAL partition column — not just any date expression. "
                "Use alongside review_query for complete coverage."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The BigQuery SQL to review."},
                    "project_id": {"type": "string", "description": "GCP project (defaults to authenticated project)."},
                },
                "required": ["sql"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    # ── Best-practice tools (no GCP required)
    if name == "resolve_topic":
        return _resolve_topic(arguments["query"], arguments.get("top_k", 5))
    elif name == "get_practices":
        return _get_practices(
            arguments["topic"],
            arguments.get("max_tokens", 3000),
            arguments.get("practice_ids"),
        )
    elif name == "get_best_practices":
        return _get_best_practices(arguments["category"])
    elif name == "search_practices":
        return _search_practices(arguments["query"])
    elif name == "get_practice_detail":
        return _get_practice_detail(arguments["practice_id"])
    elif name == "review_query":
        return _review_query(arguments["sql"])
    elif name == "list_all_practice_ids":
        return _list_all_practice_ids()

    # ── GCP-powered tools (require credentials)
    elif name == "check_gcp_connection":
        return await gcp_tools.check_gcp_connection()
    elif name == "dry_run_query":
        return await gcp_tools.dry_run_query(arguments["sql"])
    elif name == "explore_schema":
        return await gcp_tools.explore_schema(
            dataset_id=arguments.get("dataset_id"),
            table_id=arguments.get("table_id"),
            project_id=arguments.get("project_id"),
        )
    elif name == "get_table_info":
        return await gcp_tools.get_table_info(arguments["table_ref"])
    elif name == "execute_query":
        return await gcp_tools.execute_query(
            sql=arguments["sql"],
            max_rows=arguments.get("max_rows", 100),
            max_bytes_billed=arguments.get("max_bytes_billed", 1_000_000_000),
        )
    elif name == "query_history":
        return await gcp_tools.query_history(
            days=arguments.get("days", 7),
            top_n=arguments.get("top_n", 10),
            region=arguments.get("region", "us"),
        )
    elif name == "get_cost_attribution":
        return await gcp_tools.get_cost_attribution(
            days=arguments.get("days", 30),
            group_by=arguments.get("group_by", "user"),
            region=arguments.get("region", "us"),
        )
    elif name == "profile_table":
        return await gcp_tools.profile_table(
            table_ref=arguments["table_ref"],
            sample_percent=arguments.get("sample_percent", 5),
        )
    elif name == "list_jobs":
        return await gcp_tools.list_jobs(
            state=arguments.get("state", "RUNNING"),
            max_results=arguments.get("max_results", 20),
        )
    elif name == "cancel_job":
        return await gcp_tools.cancel_job(
            job_id=arguments["job_id"],
            location=arguments.get("location", "US"),
        )
    elif name == "estimate_query_cost":
        return await gcp_tools.estimate_query_cost(arguments["sql"])
    elif name == "get_expensive_queries":
        return await gcp_tools.get_expensive_queries(
            days=arguments.get("days", 7),
            top_n=arguments.get("top_n", 10),
            region=arguments.get("region", "us"),
        )
    elif name == "get_slot_utilization":
        return await gcp_tools.get_slot_utilization(
            days=arguments.get("days", 7),
            region=arguments.get("region", "us"),
        )
    elif name == "check_data_freshness":
        return await gcp_tools.check_data_freshness(
            table_ref=arguments["table_ref"],
            stale_hours=arguments.get("stale_hours", 24),
        )
    elif name == "detect_schema_drift":
        return await gcp_tools.detect_schema_drift(
            table_ref=arguments["table_ref"],
            expected_schema_json=arguments["expected_schema_json"],
        )
    elif name == "suggest_schema_improvements":
        return await gcp_tools.suggest_schema_improvements(arguments["table_ref"])
    elif name == "compare_tables":
        return await gcp_tools.compare_tables(arguments["table_a"], arguments["table_b"])
    elif name == "list_materialized_views":
        return await gcp_tools.list_materialized_views(
            dataset_id=arguments["dataset_id"],
            project_id=arguments.get("project_id"),
        )
    elif name == "explain_query_plan":
        return await gcp_tools.explain_query_plan(
            job_id=arguments["job_id"],
            location=arguments.get("location", "US"),
        )
    elif name == "detect_zombie_queries":
        return await gcp_tools.detect_zombie_queries(
            days=arguments.get("days", 30),
            min_runs=arguments.get("min_runs", 5),
            region=arguments.get("region", "us"),
        )
    elif name == "map_table_lineage":
        return await gcp_tools.map_table_lineage(
            table_ref=arguments["table_ref"],
            days=arguments.get("days", 30),
            region=arguments.get("region", "us"),
            direction=arguments.get("direction", "both"),
        )
    elif name == "detect_performance_regression":
        return await gcp_tools.detect_performance_regression(
            days_recent=arguments.get("days_recent", 7),
            days_baseline=arguments.get("days_baseline", 7),
            min_runs=arguments.get("min_runs", 3),
            region=arguments.get("region", "us"),
        )
    elif name == "review_query_with_schema":
        return await gcp_tools.review_query_with_schema(
            sql=arguments["sql"],
            project_id=arguments.get("project_id"),
        )

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


# ─────────────────────────────────────────────
# Helper: keyword scoring
# ─────────────────────────────────────────────
def _score_practice(keywords: set[str], practice: dict) -> int:
    """Score a practice dict against a set of keywords."""
    score = 0

    def _words(text: str) -> set[str]:
        return set(re.sub(r"[^a-z0-9\s]", " ", text.lower()).split())

    # Title: weight 4
    score += len(keywords & _words(practice["title"])) * 4
    # Description: weight 2
    score += len(keywords & _words(practice["description"])) * 2
    # Do / dont: weight 1 each
    for item in practice.get("do", []) + practice.get("dont", []):
        score += len(keywords & _words(item)) * 1
    # Impact tag: weight 3
    score += len(keywords & _words(practice.get("impact", ""))) * 3
    # Severity: weight 1
    score += len(keywords & _words(practice.get("severity", ""))) * 1

    return score


def _extract_keywords(query: str) -> set[str]:
    tokens = set(re.sub(r"[^a-z0-9\s]", " ", query.lower()).split())
    return tokens - _STOP_WORDS


# ─────────────────────────────────────────────
# Tool: resolve_topic
# ─────────────────────────────────────────────
def _resolve_topic(query: str, top_k: int = 5) -> list[types.TextContent]:
    top_k = min(max(top_k, 1), 20)
    keywords = _extract_keywords(query)

    if not keywords:
        return [types.TextContent(type="text", text="Query too generic – try adding more specific terms.")]

    scored: list[dict] = []
    for category, data in ALL_PRACTICES.items():
        for p in data["practices"]:
            score = _score_practice(keywords, p)
            if score > 0:
                scored.append({
                    "id": p["id"],
                    "title": p["title"],
                    "category": category,
                    "severity": p["severity"],
                    "impact": p["impact"],
                    "relevance_score": score,
                })

    scored.sort(key=lambda x: x["relevance_score"], reverse=True)
    results = scored[:top_k]

    if not results:
        return [types.TextContent(
            type="text",
            text=f"No practices matched '{query}'. Try broader terms or use list_all_practice_ids."
        )]

    lines = [f"# Topic Resolution: '{query}'\n"]
    lines.append(f"Found {len(results)} relevant practice(s) (of {len(scored)} total matches):\n")
    for r in results:
        lines.append(
            f"- **{r['id']}** – {r['title']}\n"
            f"  Category: `{r['category']}` | Severity: {r['severity']} | Impact: {r['impact']}\n"
            f"  Relevance score: {r['relevance_score']}"
        )
    lines.append(
        f"\n**Next step:** call `get_practices(topic=\"{query}\")` "
        "to retrieve the full content within a token budget."
    )
    return [types.TextContent(type="text", text="\n".join(lines))]


# ─────────────────────────────────────────────
# Tool: get_practices  (token-budgeted)
# ─────────────────────────────────────────────
def _render_practice_block(practice: dict, category: str) -> str:
    """Render a single practice to a markdown string."""
    lines = [
        f"## [{practice['id']}] {practice['title']}",
        f"**Category**: {category}  |  **Severity**: {practice['severity']}  |  **Impact**: {practice['impact']}",
        f"\n{practice['description']}\n",
        "**Do:**",
    ]
    for item in practice["do"]:
        lines.append(f"  ✅ {item}")
    lines.append("\n**Avoid:**")
    for item in practice["dont"]:
        lines.append(f"  ❌ {item}")
    if practice.get("example"):
        lines.append(f"\n**Example:**\n```sql\n{practice['example']}\n```")
    lines.append("")
    return "\n".join(lines)


def _get_practices(
    topic: str,
    max_tokens: int = 3000,
    practice_ids: list[str] | None = None,
) -> list[types.TextContent]:
    chars_budget = max_tokens * 4  # rough ~4 chars / token

    if practice_ids:
        # Explicit IDs requested
        ordered: list[tuple[str, dict]] = []
        for pid in practice_ids:
            entry = _PRACTICE_INDEX.get(pid.upper())
            if entry:
                ordered.append(entry)
    else:
        # Resolve by topic relevance
        keywords = _extract_keywords(topic)
        scored: list[tuple[int, str, dict]] = []
        for category, data in ALL_PRACTICES.items():
            for p in data["practices"]:
                s = _score_practice(keywords, p)
                if s > 0:
                    scored.append((s, category, p))
        scored.sort(key=lambda x: x[0], reverse=True)
        ordered = [(cat, p) for _, cat, p in scored]

    if not ordered:
        return [types.TextContent(
            type="text",
            text=f"No practices found for topic: '{topic}'. Try resolve_topic first."
        )]

    header = f"# BigQuery Best Practices: {topic}\n\n"
    body_parts: list[str] = []
    chars_used = len(header)

    for category, practice in ordered:
        block = _render_practice_block(practice, category)
        if chars_used + len(block) > chars_budget:
            if not body_parts:
                # Always include at least one result
                body_parts.append(block)
            break
        body_parts.append(block)
        chars_used += len(block)

    included = len(body_parts)
    remaining = len(ordered) - included
    footer_parts = [f"\n---\n_Showing {included} of {len(ordered)} matching practice(s)._"]
    if remaining > 0:
        footer_parts.append(
            f"_{remaining} more practice(s) available – increase max_tokens or use resolve_topic to narrow the scope._"
        )
    footer_parts.append(f"_Token budget: ~{chars_used // 4} / {max_tokens}_")

    content = header + "\n".join(body_parts) + "\n".join(footer_parts)
    return [types.TextContent(type="text", text=content)]


# ─────────────────────────────────────────────
# Tool: get_best_practices
# ─────────────────────────────────────────────
def _get_best_practices(category: str) -> list[types.TextContent]:
    data = ALL_PRACTICES[category]
    lines = [f"# {data['title']}", f"\n{data['description']}\n"]
    for p in data["practices"]:
        lines.append(f"## [{p['id']}] {p['title']}")
        lines.append(f"**Severity**: {p['severity']}  |  **Impact**: {p['impact']}")
        lines.append(f"\n{p['description']}\n")
        lines.append("**Do this:**")
        for do in p["do"]:
            lines.append(f"  ✅ {do}")
        lines.append("\n**Avoid:**")
        for dont in p["dont"]:
            lines.append(f"  ❌ {dont}")
        if p.get("example"):
            lines.append(f"\n**Example:**\n```sql\n{p['example']}\n```")
        lines.append("")
    return [types.TextContent(type="text", text="\n".join(lines))]


# ─────────────────────────────────────────────
# Tool: search_practices
# ─────────────────────────────────────────────
def _search_practices(query: str) -> list[types.TextContent]:
    q = query.lower()
    results = []
    for category, data in ALL_PRACTICES.items():
        for p in data["practices"]:
            haystack = (
                p["title"] + p["description"] + " ".join(p["do"] + p["dont"])
            ).lower()
            if q in haystack:
                results.append(
                    f"[{p['id']}] {p['title']}  (category: {category})\n"
                    f"  → {p['description'][:120]}…"
                )
    if not results:
        return [types.TextContent(type="text", text=f"No practices found for '{query}'.")]
    output = f"Found {len(results)} practice(s) matching '{query}':\n\n" + "\n\n".join(results)
    return [types.TextContent(type="text", text=output)]


# ─────────────────────────────────────────────
# Tool: get_practice_detail
# ─────────────────────────────────────────────
def _get_practice_detail(practice_id: str) -> list[types.TextContent]:
    entry = _PRACTICE_INDEX.get(practice_id.upper())
    if entry:
        _, practice = entry
        return [types.TextContent(type="text", text=json.dumps(practice, indent=2))]
    return [types.TextContent(type="text", text=f"Practice ID '{practice_id}' not found.")]


# ─────────────────────────────────────────────
# Tool: list_all_practice_ids
# ─────────────────────────────────────────────
def _list_all_practice_ids() -> list[types.TextContent]:
    total = sum(len(d["practices"]) for d in ALL_PRACTICES.values())
    lines = [f"# All QostEx Practice IDs  ({total} total)\n"]
    for category, data in ALL_PRACTICES.items():
        lines.append(f"## {data['title']}")
        for p in data["practices"]:
            lines.append(f"  {p['id']:10s} – {p['title']}")
        lines.append("")
    return [types.TextContent(type="text", text="\n".join(lines))]


# ─────────────────────────────────────────────
# Tool: review_query
# ─────────────────────────────────────────────
def _has_partition_filter(where_clause: str) -> bool:
    """
    Return True if the WHERE clause contains a filter on a known partition pseudo-column
    OR an explicit date/timestamp comparison that would prune DATE/TIMESTAMP partitions.
    Uses comment-stripped, normalised SQL so inline comments can't fool the check.
    """
    # Tier 1: explicit BigQuery partition pseudo-columns — definitive
    pseudo = ["_partitiontime", "_partitiondate"]
    if any(p in where_clause for p in pseudo):
        return True

    # Tier 2: date/time function calls applied to a column (likely partition key)
    # e.g. DATE(col) = ..., TIMESTAMP_TRUNC(col, DAY) >= ..., DATETIME_TRUNC(...)
    if re.search(
        r"\b(date|timestamp_trunc|datetime_trunc)\s*\(\s*\w",
        where_clause,
    ):
        return True

    # Tier 3: direct comparison with a date literal or CURRENT_DATE/TIMESTAMP
    # e.g. col >= '2024-01-01', col BETWEEN ..., col >= CURRENT_DATE()
    if re.search(
        r"\w+\s*(>=|<=|=|between)\s*['\"](20\d{2}|19\d{2})",  # date literal
        where_clause,
    ):
        return True
    if re.search(
        r"\w+\s*(>=|<=|=)\s*(current_date|current_timestamp)\b",
        where_clause,
    ):
        return True

    return False


def _detect_join_order_issue(clean_sql: str) -> str | None:
    """Return a description string if a potential JOIN order issue is detected, else None."""
    if re.search(r"from\s*\(select", clean_sql):
        if re.search(
            r"\)\s*(as\s+\w+\s+)?(inner\s+join|left\s+join|right\s+join|\bjoin\b)",
            clean_sql,
        ):
            return (
                "Derived table (subquery) is on the LEFT of a JOIN. "
                "In BigQuery hash joins the left side is broadcast; place the largest plain table first "
                "and the smaller derived/filtered result on the right."
            )
    if re.search(r"\bright\s+(outer\s+)?join\b", clean_sql):
        return (
            "RIGHT JOIN detected — rewriting as a LEFT JOIN by swapping table order is more readable "
            "and makes JOIN intent explicit."
        )
    return None


def _review_query(sql: str) -> list[types.TextContent]:
    # Work on comment-stripped, normalised SQL for all checks
    clean = sql_parser.clean(sql)
    s = clean.lower()
    findings: list[dict] = []

    # 1. SELECT *
    if re.search(r"\bselect\s+\*", s):
        findings.append({
            "pid": "QO-001",
            "msg": "SELECT * detected — specify only the columns you need to reduce bytes scanned.",
            "fix": re.sub(r"(?i)\bselect\s+\*", "SELECT col1, col2, col3  -- list only needed columns", sql, count=1),
        })

    # 2. CROSS JOIN
    if re.search(r"\bcross\s+join\b", s):
        findings.append({
            "pid": "QO-003",
            "msg": "CROSS JOIN detected — produces a cartesian product; extremely expensive on large tables.",
            "fix": re.sub(r"(?i)\bcross\s+join\b", "INNER JOIN", sql, count=1)
                   + "\n-- Add ON clause: ON table1.key = table2.key",
        })

    # 3. ORDER BY without LIMIT
    if re.search(r"\border\s+by\b", s) and not re.search(r"\blimit\b", s):
        findings.append({
            "pid": "QO-004",
            "msg": "ORDER BY without LIMIT — sorting the full result set wastes resources.",
            "fix": sql.rstrip().rstrip(";") + "\nLIMIT 1000;",
        })

    # 4. Cache-busting non-deterministic functions
    if re.search(r"\b(now|current_timestamp|current_date)\s*\(\s*\)", s):
        findings.append({
            "pid": "CO-002",
            "msg": "Non-deterministic function (NOW / CURRENT_TIMESTAMP) — prevents result-cache hits.",
            "fix": (
                "-- Use a query parameter so the same value is reused across runs:\n"
                "-- Replace:  WHERE ts >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY\n"
                "-- With:     WHERE DATE(ts) = @run_date  -- pass @run_date as a query parameter"
            ),
        })

    # 5. Partition pruning — uses comment-stripped WHERE clause
    if re.search(r"\bfrom\b", s):
        where_clause = sql_parser.get_where_clause(clean)
        if not where_clause:
            findings.append({
                "pid": "QO-002",
                "msg": "No WHERE clause — full table scan will occur on every partition.",
                "fix": (
                    sql.rstrip().rstrip(";")
                    + "\nWHERE _PARTITIONDATE = CURRENT_DATE()  -- adjust to your partition column"
                ),
            })
        elif not _has_partition_filter(where_clause):
            findings.append({
                "pid": "QO-002",
                "msg": (
                    "WHERE clause present but no recognisable partition filter detected. "
                    "If the target table is partitioned, add a filter on the partition column "
                    "to avoid a full scan. Use review_query_with_schema for a schema-aware check."
                ),
                "fix": (
                    "-- Add one of the following to your WHERE clause:\n"
                    "--   AND _PARTITIONDATE = CURRENT_DATE()\n"
                    "--   AND DATE(created_at) BETWEEN '2024-01-01' AND '2024-01-31'\n"
                    "--   AND event_ts >= TIMESTAMP('2024-01-01')"
                ),
            })

    # 6. NOT IN (subquery)
    if re.search(r"\bnot\s+in\s*\(\s*select\b", s):
        findings.append({
            "pid": "QO-005",
            "msg": "NOT IN (subquery) — prefer NOT EXISTS or LEFT JOIN / IS NULL.",
            "fix": (
                "-- Option A — NOT EXISTS (faster, handles NULLs correctly):\n"
                "-- WHERE NOT EXISTS (\n"
                "--   SELECT 1 FROM other_table\n"
                "--   WHERE other_table.id = main_table.id\n"
                "-- )\n\n"
                "-- Option B — LEFT JOIN / IS NULL:\n"
                "-- LEFT JOIN other_table ON main_table.id = other_table.id\n"
                "-- WHERE other_table.id IS NULL"
            ),
        })

    # 7. Deeply nested subqueries (count SELECT in clean SQL, not raw)
    if sql_parser.count_keyword(sql, "select") > 2:
        findings.append({
            "pid": "QO-004",
            "msg": "Multiple nested subqueries — CTEs (WITH clauses) improve readability and optimizer visibility.",
            "fix": (
                "WITH step1 AS (\n"
                "  -- innermost subquery here\n"
                "),\n"
                "step2 AS (\n"
                "  SELECT * FROM step1 WHERE ...\n"
                ")\n"
                "SELECT * FROM step2;"
            ),
        })

    # 8. HAVING without GROUP BY
    if re.search(r"\bhaving\b", s) and not re.search(r"\bgroup\s+by\b", s):
        findings.append({
            "pid": "QO-004",
            "msg": "HAVING without GROUP BY — likely a logic error. Use WHERE to filter individual rows.",
            "fix": re.sub(r"(?i)\bhaving\b", "WHERE", sql, count=1),
        })

    # 9. COUNT(DISTINCT ...) — handles arbitrary whitespace between tokens
    if re.search(r"\bcount\s*\(\s*distinct\b", s):
        findings.append({
            "pid": "QO-006",
            "msg": "COUNT(DISTINCT) — use APPROX_COUNT_DISTINCT() for large datasets (~1% error, dramatically faster).",
            "fix": re.sub(
                r"(?i)\bCOUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)",
                r"APPROX_COUNT_DISTINCT(\1)",
                sql,
            ),
        })

    # 10. JOIN order
    join_issue = _detect_join_order_issue(s)
    if join_issue:
        findings.append({
            "pid": "QO-003",
            "msg": f"JOIN order: {join_issue}",
            "fix": (
                "-- Place the largest table first (left side) in your JOIN:\n"
                "-- FROM large_table\n"
                "-- JOIN smaller_filtered_result ON large_table.id = smaller_filtered_result.id"
            ),
        })

    # ── Format output ──
    if not findings:
        return [types.TextContent(
            type="text",
            text=(
                "No obvious best-practice violations detected.\n\n"
                "Run review_query_with_schema (requires GCP) for a schema-aware check "
                "that verifies the actual partition column is being filtered."
            ),
        )]

    parts = [f"Found {len(findings)} potential issue(s):\n"]
    for i, f in enumerate(findings, 1):
        parts.append(f"### {i}. [{f['pid']}] {f['msg']}\n")
        parts.append(f"**Suggested fix:**\n```sql\n{f['fix']}\n```\n")
    parts.append(
        "---\nUse `get_practice_detail` with any ID above for full guidance, "
        "or `get_practices` with a relevant topic for broader context.\n"
        "For schema-aware partition checks, use `review_query_with_schema`."
    )
    return [types.TextContent(type="text", text="\n".join(parts))]


# ─────────────────────────────────────────────
# "use qostex" system prompt snippet
# ─────────────────────────────────────────────
_SYSTEM_PROMPT_SNIPPET = """\
# QostEx – BigQuery Best Practices (use qostex)

You have access to the QostEx MCP server which provides curated, structured
BigQuery best practices across 6 categories (query optimisation, schema design,
cost management, security, materialized views, monitoring).

## When to use QostEx

| User request                                    | Tool to call                         |
|-------------------------------------------------|--------------------------------------|
| Asks about a BigQuery topic                     | resolve_topic → get_practices        |
| Shares SQL for review                           | review_query                         |
| Wants all practices in one category             | get_best_practices                   |
| Searches by keyword                             | search_practices                     |
| Wants details on a specific rule (e.g. QO-002) | get_practice_detail                  |

## Recommended workflow

1. Call `resolve_topic(query="<user intent>")` → get ranked practice IDs
2. Call `get_practices(topic="<user intent>", max_tokens=3000)` → focused content
3. Use the returned practices to inform your response
4. **Always cite practice IDs** (e.g. QO-002, SD-001) when giving recommendations

## Token budget guidance

- Quick answer  : max_tokens=1500
- Normal answer : max_tokens=3000  (default)
- Deep dive     : max_tokens=6000
"""


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
