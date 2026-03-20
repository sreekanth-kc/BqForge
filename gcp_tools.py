"""
GCP-powered BqForge tools.

All public coroutines return list[types.TextContent] matching the MCP contract.
Blocking BigQuery SDK calls are dispatched to a thread pool via asyncio.to_thread.
"""

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timezone

from mcp import types

# Configurable via env var — set BQ_PRICE_PER_TB to override (e.g. for regional pricing)
_ON_DEMAND_PRICE_PER_TB = float(os.environ.get("BQ_PRICE_PER_TB", "6.25"))

_GCP_NOT_CONFIGURED = (
    "GCP credentials are not configured.\n\n"
    "Set one of:\n"
    "  • GCP_SERVICE_ACCOUNT_JSON  — paste your service-account JSON as an env var\n"
    "  • GOOGLE_APPLICATION_CREDENTIALS — path to a service-account key file\n"
    "  • Run `gcloud auth application-default login` for local ADC\n\n"
    "Then call check_gcp_connection to verify."
)


def _text(s: str) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=s)]


def _client():
    from gcp_client import get_client
    return get_client()


async def _run(fn, *args, **kwargs):
    """Run a blocking callable in a thread pool."""
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _retry_run(fn, retries: int = 3, delay: float = 2.0):
    """Run a blocking callable with exponential-backoff retries (for INFORMATION_SCHEMA queries)."""
    import time
    last_exc = None
    for attempt in range(retries):
        try:
            return await asyncio.to_thread(fn)
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                await asyncio.sleep(delay * (2 ** attempt))
    raise last_exc


# ─────────────────────────────────────────────
# check_gcp_connection
# ─────────────────────────────────────────────
async def check_gcp_connection() -> list[types.TextContent]:
    try:
        client = _client()
        datasets = await _run(lambda: list(client.list_datasets(max_results=3)))
        project = client.project
        names = [d.dataset_id for d in datasets]
        preview = ", ".join(names) + ("…" if len(names) == 3 else "")
        return _text(
            f"Connected to GCP project: `{project}`\n"
            f"Sample datasets: {preview or '(none found)'}"
        )
    except Exception as e:
        return _text(f"GCP connection failed: {e}\n\n{_GCP_NOT_CONFIGURED}")


# ─────────────────────────────────────────────
# dry_run_query
# ─────────────────────────────────────────────
async def dry_run_query(sql: str) -> list[types.TextContent]:
    try:
        from google.cloud import bigquery as bq
        client = _client()
        config = bq.QueryJobConfig(dry_run=True, use_query_cache=False)

        job = await _run(lambda: client.query(sql, job_config=config))
        bytes_processed = job.total_bytes_processed
        gb = bytes_processed / 1e9
        cost_usd = (bytes_processed / 1e12) * _ON_DEMAND_PRICE_PER_TB

        lines = [
            "## Dry Run Result",
            f"Bytes processed : {bytes_processed:,} bytes  ({gb:.3f} GB)",
            f"Estimated cost  : ${cost_usd:.4f} USD  (on-demand @ ${_ON_DEMAND_PRICE_PER_TB}/TB)",
        ]

        if bytes_processed == 0:
            lines.append("\nNo data scanned — query may use only literals or cached results.")
        elif cost_usd > 10:
            lines.append(
                f"\nWARNING: Estimated cost ${cost_usd:.2f} is high. "
                "Consider adding partition filters or selecting fewer columns."
            )
        elif cost_usd < 0.001:
            lines.append("\nCost is negligible (< $0.001).")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Dry run failed: {e}")


# ─────────────────────────────────────────────
# explore_schema
# ─────────────────────────────────────────────
async def explore_schema(
    dataset_id: str | None = None,
    table_id: str | None = None,
    project_id: str | None = None,
) -> list[types.TextContent]:
    try:
        client = _client()
        project = project_id or client.project

        if dataset_id is None:
            datasets = await _run(lambda: list(client.list_datasets(project=project)))
            if not datasets:
                return _text(f"No datasets found in project `{project}`.")
            lines = [f"# Datasets in `{project}`\n"]
            for d in datasets:
                lines.append(f"  {d.dataset_id}")
            return _text("\n".join(lines))

        if table_id is None:
            tables = await _run(lambda: list(client.list_tables(f"{project}.{dataset_id}")))
            if not tables:
                return _text(f"No tables in `{project}.{dataset_id}`.")
            lines = [f"# Tables in `{project}.{dataset_id}`\n"]
            for t in tables:
                lines.append(f"  {t.table_id:<45} [{t.table_type}]")
            return _text("\n".join(lines))

        table_ref = f"{project}.{dataset_id}.{table_id}"
        table = await _run(lambda: client.get_table(table_ref))
        lines = [
            f"# Schema: `{table_ref}`\n",
            f"{'Column':<35} {'Type':<20} {'Mode':<10} Description",
            "-" * 90,
        ]
        for field in table.schema:
            lines.append(
                f"{field.name:<35} {field.field_type:<20} {field.mode:<10} "
                f"{(field.description or '')[:50]}"
            )

        if table.time_partitioning:
            col = table.time_partitioning.field or "_PARTITIONTIME"
            lines.append(f"\nPartitioned by : {col} ({table.time_partitioning.type_})")
        if table.clustering_fields:
            lines.append(f"Clustered by   : {', '.join(table.clustering_fields)}")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Schema exploration failed: {e}")


# ─────────────────────────────────────────────
# get_table_info
# ─────────────────────────────────────────────
async def get_table_info(table_ref: str) -> list[types.TextContent]:
    """table_ref can be project.dataset.table or dataset.table"""
    try:
        client = _client()
        if table_ref.count(".") == 1:
            table_ref = f"{client.project}.{table_ref}"

        table = await _run(lambda: client.get_table(table_ref))

        size_gb = (table.num_bytes or 0) / 1e9
        size_tb = (table.num_bytes or 0) / 1e12
        rows = table.num_rows or 0
        modified = table.modified.strftime("%Y-%m-%d %H:%M UTC") if table.modified else "N/A"
        created = table.created.strftime("%Y-%m-%d %H:%M UTC") if table.created else "N/A"

        freshness = ""
        if table.modified:
            age = datetime.now(timezone.utc) - table.modified
            freshness = (
                f"(updated {age.seconds // 3600}h ago)"
                if age.days == 0
                else f"(updated {age.days}d ago)"
            )

        lines = [
            f"# Table Info: `{table_ref}`\n",
            f"Type          : {table.table_type}",
            f"Rows          : {rows:,}",
            f"Size          : {size_gb:.3f} GB",
            f"Created       : {created}",
            f"Last modified : {modified}  {freshness}",
        ]

        if table.description:
            lines.append(f"Description   : {table.description}")

        if table.time_partitioning:
            col = table.time_partitioning.field or "_PARTITIONTIME"
            lines.append(f"\nPartition by  : {col} ({table.time_partitioning.type_})")
            if table.time_partitioning.expiration_ms:
                exp_days = table.time_partitioning.expiration_ms // (1000 * 60 * 60 * 24)
                lines.append(f"Partition TTL : {exp_days} days")
        else:
            lines.append("\nPartition by  : (none) — consider partitioning large tables")

        if table.clustering_fields:
            lines.append(f"Clustered by  : {', '.join(table.clustering_fields)}")
        else:
            lines.append("Clustered by  : (none)")

        if table.num_bytes:
            est_cost = size_tb * _ON_DEMAND_PRICE_PER_TB
            lines.append(f"\nFull scan cost: ${est_cost:.4f} USD (no partition filter)")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Failed to get table info: {e}")


# ─────────────────────────────────────────────
# execute_query
# ─────────────────────────────────────────────
async def execute_query(
    sql: str,
    max_rows: int = 100,
    max_bytes_billed: int = 1_000_000_000,  # 1 GB safety cap
) -> list[types.TextContent]:
    try:
        from google.cloud import bigquery as bq
        client = _client()
        config = bq.QueryJobConfig(maximum_bytes_billed=max_bytes_billed)

        def _do():
            job = client.query(sql, job_config=config)
            results = job.result(max_results=max_rows)
            rows = list(results)
            schema = results.schema
            return rows, schema, job.total_bytes_processed

        rows, schema, bytes_processed = await _run(_do)

        if not rows:
            return _text("Query returned 0 rows.")

        headers = [f.name for f in schema]
        cost = ((bytes_processed or 0) / 1e12) * _ON_DEMAND_PRICE_PER_TB

        lines = [
            f"Returned {len(rows)} row(s) | "
            f"Scanned {(bytes_processed or 0):,} bytes | "
            f"Cost: ${cost:.4f}\n",
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]

        for row in rows:
            values = [str(v) if v is not None else "NULL" for v in row.values()]
            lines.append("| " + " | ".join(values) + " |")

        if len(rows) == max_rows:
            lines.append(f"\n_Results capped at {max_rows} rows. Pass a higher max_rows to see more._")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Query execution failed: {e}")


# ─────────────────────────────────────────────
# query_history
# ─────────────────────────────────────────────
async def query_history(
    days: int = 7,
    top_n: int = 10,
    region: str = "us",
) -> list[types.TextContent]:
    try:
        client = _client()
        project = client.project

        sql = f"""
        SELECT
            user_email,
            COUNT(*) AS job_count,
            SUM(total_bytes_processed) AS total_bytes,
            ROUND(SUM(total_bytes_processed) / 1e12 * {_ON_DEMAND_PRICE_PER_TB}, 4) AS est_cost_usd,
            AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS avg_duration_sec,
            MAX(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS max_duration_sec
        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND state = 'DONE'
            AND error_result IS NULL
            AND job_type = 'QUERY'
        GROUP BY user_email
        ORDER BY est_cost_usd DESC
        LIMIT {top_n}
        """

        rows = await _retry_run(lambda: list(client.query(sql).result()))

        if not rows:
            return _text(f"No completed query jobs in the last {days} days.")

        lines = [f"# Query History — Last {days} Days (top {top_n} by cost)\n"]
        lines.append(
            f"{'User':<40} {'Jobs':>6} {'GB':>10} {'Cost (USD)':>12} {'Avg(s)':>8} {'Max(s)':>8}"
        )
        lines.append("-" * 90)

        total_cost = 0.0
        for row in rows:
            gb = (row.total_bytes or 0) / 1e9
            cost = float(row.est_cost_usd or 0)
            total_cost += cost
            lines.append(
                f"{row.user_email:<40} {row.job_count:>6} {gb:>10.2f} "
                f"${cost:>11.4f} {(row.avg_duration_sec or 0):>8.1f} {(row.max_duration_sec or 0):>8.1f}"
            )

        lines.append("-" * 90)
        lines.append(f"Total estimated cost (shown): ${total_cost:.4f} USD")
        lines.append("\n_Costs are estimates based on on-demand pricing ($6.25/TB)._")
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Query history failed: {e}")


# ─────────────────────────────────────────────
# get_cost_attribution
# ─────────────────────────────────────────────
async def get_cost_attribution(
    days: int = 30,
    group_by: str = "user",  # user | label
    region: str = "us",
) -> list[types.TextContent]:
    try:
        client = _client()
        project = client.project

        if group_by == "label":
            sql = f"""
            SELECT
                label.key AS label_key,
                label.value AS label_value,
                COUNT(*) AS job_count,
                SUM(total_bytes_processed) AS total_bytes,
                ROUND(SUM(total_bytes_processed) / 1e12 * {_ON_DEMAND_PRICE_PER_TB}, 4) AS est_cost_usd
            FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
                UNNEST(labels) AS label
            WHERE
                creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND state = 'DONE'
                AND job_type = 'QUERY'
                AND ARRAY_LENGTH(labels) > 0
            GROUP BY label_key, label_value
            ORDER BY est_cost_usd DESC
            LIMIT 25
            """
            label_mode = True
        else:
            sql = f"""
            SELECT
                user_email,
                COUNT(*) AS job_count,
                SUM(total_bytes_processed) AS total_bytes,
                ROUND(SUM(total_bytes_processed) / 1e12 * {_ON_DEMAND_PRICE_PER_TB}, 4) AS est_cost_usd
            FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE
                creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND state = 'DONE'
                AND job_type = 'QUERY'
            GROUP BY user_email
            ORDER BY est_cost_usd DESC
            LIMIT 25
            """
            label_mode = False

        rows = await _retry_run(lambda: list(client.query(sql).result()))

        if not rows:
            return _text(f"No billing data in the last {days} days.")

        lines = [f"# Cost Attribution — Last {days} Days (by {group_by})\n"]
        total_cost = 0.0
        for row in rows:
            cost = float(row.est_cost_usd or 0)
            gb = (row.total_bytes or 0) / 1e9
            total_cost += cost
            key = f"{row.label_key}={row.label_value}" if label_mode else row.user_email
            lines.append(f"  {key:<50} {gb:>10.2f} GB   ${cost:>10.4f}")

        lines.append(f"\nTotal: ${total_cost:.4f} USD  (on-demand estimate)")
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Cost attribution failed: {e}")


# ─────────────────────────────────────────────
# profile_table
# ─────────────────────────────────────────────
async def profile_table(
    table_ref: str,
    sample_percent: int = 5,
) -> list[types.TextContent]:
    try:
        client = _client()
        if table_ref.count(".") == 1:
            table_ref = f"{client.project}.{table_ref}"

        table = await _run(lambda: client.get_table(table_ref))
        schema = table.schema

        parts = []
        for field in schema[:20]:
            col = f"`{field.name}`"
            ftype = field.field_type
            if ftype in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                parts.append(
                    f"COUNT({col}) AS `{field.name}__count`, "
                    f"COUNT(DISTINCT {col}) AS `{field.name}__distinct`, "
                    f"MIN({col}) AS `{field.name}__min`, "
                    f"MAX({col}) AS `{field.name}__max`, "
                    f"AVG(CAST({col} AS FLOAT64)) AS `{field.name}__avg`"
                )
            elif ftype in ("STRING", "BYTES"):
                parts.append(
                    f"COUNT({col}) AS `{field.name}__count`, "
                    f"COUNT(DISTINCT {col}) AS `{field.name}__distinct`, "
                    f"MIN(LENGTH(CAST({col} AS STRING))) AS `{field.name}__min_len`, "
                    f"MAX(LENGTH(CAST({col} AS STRING))) AS `{field.name}__max_len`"
                )
            else:
                parts.append(f"COUNT({col}) AS `{field.name}__count`")

        if not parts:
            return _text("No columns to profile.")

        pct = max(1, min(sample_percent, 100))
        sql = (
            f"SELECT {', '.join(parts)} "
            f"FROM `{table_ref}` TABLESAMPLE SYSTEM ({pct} PERCENT)"
        )

        rows = await _retry_run(lambda: list(client.query(sql).result()))
        if not rows:
            return _text("Table appears to be empty.")

        row = dict(rows[0])
        lines = [
            f"# Table Profile: `{table_ref}` (~{pct}% sample)\n",
            f"{'Column':<35} {'Type':<12} {'Non-null':>9} {'Distinct':>9} {'Min':>16} {'Max':>16} {'Avg':>12}",
            "-" * 115,
        ]

        for field in schema[:20]:
            n = field.name
            ftype = field.field_type
            count = row.get(f"{n}__count", "N/A")
            distinct = row.get(f"{n}__distinct", "N/A")

            if ftype in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                mn = row.get(f"{n}__min", "N/A")
                mx = row.get(f"{n}__max", "N/A")
                avg = row.get(f"{n}__avg")
                avg_str = f"{avg:.2f}" if avg is not None else "N/A"
                lines.append(
                    f"{n:<35} {ftype:<12} {str(count):>9} {str(distinct):>9} "
                    f"{str(mn):>16} {str(mx):>16} {avg_str:>12}"
                )
            elif ftype in ("STRING", "BYTES"):
                min_len = row.get(f"{n}__min_len", "N/A")
                max_len = row.get(f"{n}__max_len", "N/A")
                lines.append(
                    f"{n:<35} {ftype:<12} {str(count):>9} {str(distinct):>9} "
                    f"{'len:'+str(min_len):>16} {'len:'+str(max_len):>16} {'':>12}"
                )
            else:
                lines.append(f"{n:<35} {ftype:<12} {str(count):>9}")

        if len(schema) > 20:
            lines.append(f"\n_(Showing first 20 of {len(schema)} columns)_")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Table profiling failed: {e}")


# ─────────────────────────────────────────────
# list_jobs
# ─────────────────────────────────────────────
async def list_jobs(
    state: str = "RUNNING",
    max_results: int = 20,
) -> list[types.TextContent]:
    try:
        client = _client()
        state_upper = state.upper()

        jobs = await _run(
            lambda: list(client.list_jobs(state_filter=state_upper, max_results=max_results))
        )

        if not jobs:
            return _text(f"No {state_upper} jobs found.")

        lines = [
            f"# {state_upper} Jobs ({len(jobs)} shown)\n",
            f"{'Job ID':<50} {'Type':<12} {'User':<35} {'Created':<20}",
            "-" * 125,
        ]
        for job in jobs:
            created = job.created.strftime("%Y-%m-%d %H:%M") if job.created else "N/A"
            lines.append(
                f"{job.job_id:<50} {job.job_type:<12} "
                f"{(job.user_email or 'N/A'):<35} {created:<20}"
            )

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"List jobs failed: {e}")


# ─────────────────────────────────────────────
# cancel_job
# ─────────────────────────────────────────────
async def cancel_job(job_id: str, location: str = "US") -> list[types.TextContent]:
    try:
        client = _client()
        job = await _run(lambda: client.cancel_job(job_id, location=location))
        return _text(f"Cancel request sent for job `{job_id}`.\nCurrent state: {job.state}")
    except Exception as e:
        return _text(f"Cancel job failed: {e}")


# ─────────────────────────────────────────────
# estimate_query_cost
# ─────────────────────────────────────────────
async def estimate_query_cost(sql: str, region: str = "US") -> list[types.TextContent]:
    """Human-friendly cost estimate wrapping dry_run_query."""
    try:
        from google.cloud import bigquery as bq
        client = _client()
        config = bq.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = await _run(lambda: client.query(sql, job_config=config))
        b = job.total_bytes_processed
        gb = b / 1e9
        tb = b / 1e12
        cost = tb * _ON_DEMAND_PRICE_PER_TB

        if cost == 0:
            tier = "free (no data scanned)"
        elif cost < 0.01:
            tier = "negligible (< $0.01)"
        elif cost < 1:
            tier = "low (< $1)"
        elif cost < 10:
            tier = "moderate ($1–$10)"
        elif cost < 100:
            tier = "high ($10–$100) — consider adding partition filters"
        else:
            tier = f"very high (${cost:,.2f}) — review query before running"

        lines = [
            "## Query Cost Estimate\n",
            f"Data scanned  : {b:,} bytes  ({gb:.3f} GB)",
            f"Estimated cost: **${cost:.4f} USD**",
            f"Cost tier     : {tier}",
            "",
            "_Pricing: on-demand @ $6.25/TB. Flat-rate / flex slots = $0 per query._",
        ]
        if cost > 5:
            lines += [
                "",
                "**Tips to reduce cost:**",
                "- Add a partition filter (`WHERE _PARTITIONDATE = ...`)",
                "- Replace `SELECT *` with specific columns",
                "- Use a WHERE clause to limit rows scanned",
            ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Cost estimation failed: {e}")


# ─────────────────────────────────────────────
# get_expensive_queries
# ─────────────────────────────────────────────
async def get_expensive_queries(
    days: int = 7,
    top_n: int = 10,
    region: str = "us",
) -> list[types.TextContent]:
    """Surface the top N most expensive queries with their SQL snippets."""
    try:
        client = _client()
        project = client.project
        sql = f"""
        SELECT
            job_id,
            user_email,
            ROUND(total_bytes_processed / 1e12 * {_ON_DEMAND_PRICE_PER_TB}, 4) AS est_cost_usd,
            ROUND(total_bytes_processed / 1e9, 2) AS gb_processed,
            TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_sec,
            creation_time,
            SUBSTR(query, 1, 500) AS query_snippet
        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND state = 'DONE'
            AND job_type = 'QUERY'
            AND total_bytes_processed IS NOT NULL
        ORDER BY total_bytes_processed DESC
        LIMIT {top_n}
        """
        rows = await _retry_run(lambda: list(client.query(sql).result()))
        if not rows:
            return _text(f"No completed queries found in the last {days} days.")

        lines = [f"# Top {top_n} Most Expensive Queries — Last {days} Days\n"]
        for i, row in enumerate(rows, 1):
            created = row.creation_time.strftime("%Y-%m-%d %H:%M") if row.creation_time else "N/A"
            lines += [
                f"## {i}. ${row.est_cost_usd:.4f} USD  |  {row.gb_processed:.2f} GB  |  {row.duration_sec}s  |  {created}",
                f"**User:** {row.user_email}  |  **Job:** `{row.job_id}`",
                f"```sql\n{row.query_snippet}\n```",
                "",
            ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"get_expensive_queries failed: {e}")


# ─────────────────────────────────────────────
# get_slot_utilization
# ─────────────────────────────────────────────
async def get_slot_utilization(
    days: int = 7,
    region: str = "us",
) -> list[types.TextContent]:
    """Slot hours consumed by reservation over the time window."""
    try:
        client = _client()
        project = client.project
        sql = f"""
        SELECT
            reservation_id,
            COUNT(*) AS job_count,
            SUM(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS total_duration_sec,
            ROUND(SUM(total_slot_ms) / 3600000.0, 2) AS slot_hours
        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND state = 'DONE'
            AND total_slot_ms IS NOT NULL
        GROUP BY reservation_id
        ORDER BY slot_hours DESC
        """
        rows = await _retry_run(lambda: list(client.query(sql).result()))
        if not rows:
            return _text(f"No slot data found in the last {days} days.")

        lines = [
            f"# Slot Utilization — Last {days} Days\n",
            f"{'Reservation':<45} {'Jobs':>6} {'Slot-Hours':>12} {'Total Duration (h)':>20}",
            "-" * 90,
        ]
        for row in rows:
            res = row.reservation_id or "(on-demand)"
            dur_h = (row.total_duration_sec or 0) / 3600
            lines.append(
                f"{res:<45} {row.job_count:>6} {row.slot_hours:>12.2f} {dur_h:>20.1f}"
            )
        lines.append("\n_Slot-hours = total_slot_ms / 3,600,000. On-demand jobs show as '(on-demand)'._")
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Slot utilization query failed: {e}")


# ─────────────────────────────────────────────
# check_data_freshness
# ─────────────────────────────────────────────
async def check_data_freshness(
    table_ref: str,
    stale_hours: int = 24,
) -> list[types.TextContent]:
    """Report how fresh a table is and flag if stale beyond threshold."""
    try:
        client = _client()
        if table_ref.count(".") == 1:
            table_ref = f"{client.project}.{table_ref}"

        table = await _run(lambda: client.get_table(table_ref))
        modified = table.modified
        if not modified:
            return _text(f"Cannot determine freshness for `{table_ref}` — no modification timestamp available.")

        age = datetime.now(timezone.utc) - modified
        age_hours = age.total_seconds() / 3600
        age_str = (
            f"{int(age.total_seconds() // 60)} minutes"
            if age_hours < 1
            else f"{age_hours:.1f} hours"
            if age_hours < 48
            else f"{age.days} days"
        )

        is_stale = age_hours > stale_hours
        status = "STALE" if is_stale else "FRESH"
        icon = "WARNING" if is_stale else "OK"

        lines = [
            f"# Data Freshness: `{table_ref}`\n",
            f"Status        : [{icon}] {status}",
            f"Last modified : {modified.strftime('%Y-%m-%d %H:%M UTC')}",
            f"Age           : {age_str}",
            f"Threshold     : {stale_hours}h",
            f"Rows          : {(table.num_rows or 0):,}",
            f"Size          : {(table.num_bytes or 0) / 1e9:.3f} GB",
        ]

        if table.time_partitioning:
            lines.append(
                f"Partitioned by: {table.time_partitioning.field or '_PARTITIONTIME'} "
                f"({table.time_partitioning.type_})"
            )

        if is_stale:
            lines += [
                "",
                f"Table has not been updated in {age_str} — exceeds the {stale_hours}h threshold.",
                "Check your ingestion pipeline or scheduled query for failures.",
            ]

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Data freshness check failed: {e}")


# ─────────────────────────────────────────────
# detect_schema_drift
# ─────────────────────────────────────────────
async def detect_schema_drift(
    table_ref: str,
    expected_schema_json: str,
) -> list[types.TextContent]:
    """Compare expected schema (JSON array of {name, type}) against actual BigQuery schema."""
    import json
    try:
        client = _client()
        if table_ref.count(".") == 1:
            table_ref = f"{client.project}.{table_ref}"

        table = await _run(lambda: client.get_table(table_ref))
        actual = {f.name.lower(): f.field_type.upper() for f in table.schema}

        try:
            expected_raw = json.loads(expected_schema_json)
        except json.JSONDecodeError as e:
            return _text(f"Invalid expected_schema_json: {e}")

        expected = {col["name"].lower(): col["type"].upper() for col in expected_raw}

        missing = {k: v for k, v in expected.items() if k not in actual}
        extra = {k: v for k, v in actual.items() if k not in expected}
        type_mismatch = {
            k: {"expected": expected[k], "actual": actual[k]}
            for k in expected if k in actual and expected[k] != actual[k]
        }

        if not missing and not extra and not type_mismatch:
            return _text(f"No schema drift detected for `{table_ref}` — schemas match.")

        lines = [f"# Schema Drift Report: `{table_ref}`\n"]
        if missing:
            lines.append(f"## Missing columns ({len(missing)}) — in expected but not in table")
            for col, typ in missing.items():
                lines.append(f"  - `{col}` ({typ})")
        if extra:
            lines.append(f"\n## Extra columns ({len(extra)}) — in table but not in expected schema")
            for col, typ in extra.items():
                lines.append(f"  + `{col}` ({typ})")
        if type_mismatch:
            lines.append(f"\n## Type mismatches ({len(type_mismatch)})")
            for col, diff in type_mismatch.items():
                lines.append(f"  `{col}`: expected {diff['expected']}, got {diff['actual']}")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Schema drift detection failed: {e}")


# ─────────────────────────────────────────────
# suggest_schema_improvements
# ─────────────────────────────────────────────
async def suggest_schema_improvements(table_ref: str) -> list[types.TextContent]:
    """Pull a table's schema and cross-reference against schema design best practices."""
    try:
        client = _client()
        if table_ref.count(".") == 1:
            table_ref = f"{client.project}.{table_ref}"

        table = await _run(lambda: client.get_table(table_ref))
        schema = table.schema
        suggestions: list[str] = []

        # No partitioning
        if not table.time_partitioning and not table.range_partitioning:
            size_gb = (table.num_bytes or 0) / 1e9
            if size_gb > 1:
                suggestions.append(
                    f"[PT-001] Table is {size_gb:.1f} GB with no partitioning. "
                    "Add DATE/TIMESTAMP partitioning to reduce scan cost."
                )

        # No clustering
        if not table.clustering_fields:
            suggestions.append(
                "[SD-002] No clustering defined. If queries frequently filter or order by specific columns, "
                "add up to 4 cluster columns for better pruning."
            )

        # Columns whose name suggests a date but type is STRING
        date_name_hints = {"date", "day", "dt", "created", "updated", "timestamp", "ts", "time"}
        for field in schema:
            name_lower = field.name.lower()
            if field.field_type == "STRING" and any(h in name_lower for h in date_name_hints):
                suggestions.append(
                    f"[SD-003] Column `{field.name}` is STRING but name suggests a date/time. "
                    "Consider using DATE, DATETIME, or TIMESTAMP for correct partition pruning and sorting."
                )

        # NULLABLE columns whose name looks like a primary key
        pk_hints = {"id", "_id", "key", "_key", "pk"}
        for field in schema:
            if field.mode == "NULLABLE" and any(field.name.lower().endswith(h) for h in pk_hints):
                suggestions.append(
                    f"[SD-004] Column `{field.name}` appears to be an ID/key but is NULLABLE. "
                    "If this is a primary key, set mode to REQUIRED."
                )

        # Very wide table
        if len(schema) > 50:
            record_cols = [f for f in schema if f.field_type == "RECORD"]
            if not record_cols:
                suggestions.append(
                    f"[SD-005] Table has {len(schema)} columns with no nested RECORD fields. "
                    "Consider grouping related columns into STRUCT/RECORD types for clarity and compression."
                )

        # Repeated fields not using ARRAY type
        repeated = [f for f in schema if f.mode == "REPEATED"]
        if not repeated and len(schema) > 20:
            suggestions.append(
                "[SD-006] No REPEATED (ARRAY) columns found. "
                "If any columns represent one-to-many relationships, consider nesting them as ARRAY<STRUCT> "
                "to avoid expensive JOINs."
            )

        if not suggestions:
            return _text(
                f"No schema improvement suggestions for `{table_ref}`.\n"
                "Partitioning, clustering, and column types all look reasonable."
            )

        lines = [f"# Schema Improvement Suggestions: `{table_ref}`\n"]
        for i, s in enumerate(suggestions, 1):
            lines.append(f"{i}. {s}")
        lines.append(
            "\nUse `get_practice_detail` with any practice ID (e.g. PT-001) for full guidance."
        )
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Schema improvement analysis failed: {e}")


# ─────────────────────────────────────────────
# compare_tables
# ─────────────────────────────────────────────
async def compare_tables(table_a: str, table_b: str) -> list[types.TextContent]:
    """Diff schemas between two BigQuery tables."""
    try:
        client = _client()

        def _norm(ref: str) -> str:
            return f"{client.project}.{ref}" if ref.count(".") == 1 else ref

        t_a, t_b = await asyncio.gather(
            _run(lambda: client.get_table(_norm(table_a))),
            _run(lambda: client.get_table(_norm(table_b))),
        )

        schema_a = {f.name.lower(): f for f in t_a.schema}
        schema_b = {f.name.lower(): f for f in t_b.schema}

        only_a = {k for k in schema_a if k not in schema_b}
        only_b = {k for k in schema_b if k not in schema_a}
        both = {k for k in schema_a if k in schema_b}
        type_diff = {
            k for k in both
            if schema_a[k].field_type != schema_b[k].field_type
            or schema_a[k].mode != schema_b[k].mode
        }
        identical = both - type_diff

        lines = [
            f"# Schema Comparison\n",
            f"Table A: `{_norm(table_a)}`  ({len(t_a.schema)} columns, {(t_a.num_bytes or 0)/1e9:.2f} GB)",
            f"Table B: `{_norm(table_b)}`  ({len(t_b.schema)} columns, {(t_b.num_bytes or 0)/1e9:.2f} GB)\n",
            f"Identical columns : {len(identical)}",
            f"Type/mode diffs   : {len(type_diff)}",
            f"Only in A         : {len(only_a)}",
            f"Only in B         : {len(only_b)}",
        ]

        if type_diff:
            lines.append("\n## Type / Mode Differences")
            for k in sorted(type_diff):
                fa, fb = schema_a[k], schema_b[k]
                lines.append(
                    f"  `{k}`:  A={fa.field_type}/{fa.mode}  →  B={fb.field_type}/{fb.mode}"
                )

        if only_a:
            lines.append(f"\n## Only in A ({len(only_a)})")
            for k in sorted(only_a):
                f = schema_a[k]
                lines.append(f"  - `{k}` ({f.field_type}, {f.mode})")

        if only_b:
            lines.append(f"\n## Only in B ({len(only_b)})")
            for k in sorted(only_b):
                f = schema_b[k]
                lines.append(f"  + `{k}` ({f.field_type}, {f.mode})")

        # Partition / cluster diff
        part_a = table_a and (t_a.time_partitioning.field if t_a.time_partitioning else None)
        part_b = table_b and (t_b.time_partitioning.field if t_b.time_partitioning else None)
        if part_a != part_b:
            lines.append(f"\n## Partition column differs: A={part_a or '(none)'}  B={part_b or '(none)'}")

        clust_a = t_a.clustering_fields or []
        clust_b = t_b.clustering_fields or []
        if clust_a != clust_b:
            lines.append(
                f"\n## Clustering differs:\n  A: {clust_a or '(none)'}  B: {clust_b or '(none)'}"
            )

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Table comparison failed: {e}")


# ─────────────────────────────────────────────
# list_materialized_views
# ─────────────────────────────────────────────
async def list_materialized_views(dataset_id: str, project_id: str | None = None) -> list[types.TextContent]:
    """List all materialized views in a dataset with refresh status."""
    try:
        client = _client()
        project = project_id or client.project

        tables = await _run(lambda: list(client.list_tables(f"{project}.{dataset_id}")))
        mvs = [t for t in tables if t.table_type == "MATERIALIZED_VIEW"]

        if not mvs:
            return _text(f"No materialized views found in `{project}.{dataset_id}`.")

        # Get full details for each MV
        async def _get(t):
            return await _run(lambda: client.get_table(t.reference))

        full = await asyncio.gather(*[_get(t) for t in mvs])

        lines = [f"# Materialized Views in `{project}.{dataset_id}`\n"]
        for t in full:
            mv_def = getattr(t, "mview_query", None) or "(query not available)"
            last_refresh = getattr(t, "mview_last_refresh_time", None)
            enable_refresh = getattr(t, "mview_enable_refresh", None)
            refresh_interval = getattr(t, "mview_refresh_interval", None)

            refresh_str = last_refresh.strftime("%Y-%m-%d %H:%M UTC") if last_refresh else "Never"
            age_str = ""
            if last_refresh:
                age_h = (datetime.now(timezone.utc) - last_refresh).total_seconds() / 3600
                age_str = f"({age_h:.1f}h ago)"

            lines += [
                f"## {t.table_id}",
                f"Last refreshed : {refresh_str} {age_str}",
                f"Auto-refresh   : {enable_refresh}",
                f"Refresh interval: {refresh_interval or 'N/A'}",
                f"Size           : {(t.num_bytes or 0) / 1e9:.3f} GB",
                f"```sql\n{str(mv_def)[:400]}\n```",
                "",
            ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"List materialized views failed: {e}")


# ─────────────────────────────────────────────
# explain_query_plan
# ─────────────────────────────────────────────
async def explain_query_plan(job_id: str, location: str = "US") -> list[types.TextContent]:
    """Parse the query execution plan from a completed job and surface bottlenecks."""
    try:
        client = _client()
        job = await _run(lambda: client.get_job(job_id, location=location))

        if not hasattr(job, "query_plan") or not job.query_plan:
            return _text(
                f"No query plan available for job `{job_id}`. "
                "The job may still be running, or may not be a query job."
            )

        plan = job.query_plan
        total_compute_ms = sum(
            (stage.compute_ms_avg or 0) for stage in plan
        )

        lines = [
            f"# Query Execution Plan: `{job_id}`\n",
            f"Total stages : {len(plan)}",
            f"Total compute: ~{total_compute_ms:,} ms\n",
            f"{'Stage':<30} {'Status':<12} {'Records In':>14} {'Records Out':>14} {'Compute (ms)':>14} {'Inputs':>8}",
            "-" * 100,
        ]

        bottlenecks = []
        for stage in plan:
            rec_in = stage.records_read or 0
            rec_out = stage.records_written or 0
            compute = stage.compute_ms_avg or 0
            inputs = stage.parallel_inputs or 0

            lines.append(
                f"{stage.name:<30} {stage.status:<12} {rec_in:>14,} {rec_out:>14,} {compute:>14,} {inputs:>8}"
            )

            # Flag bottlenecks
            if total_compute_ms > 0 and compute / total_compute_ms > 0.5:
                bottlenecks.append(
                    f"Stage `{stage.name}` consumes {compute/total_compute_ms*100:.0f}% of total compute time."
                )
            if rec_in > 0 and rec_out / rec_in < 0.01:
                bottlenecks.append(
                    f"Stage `{stage.name}` filters out 99%+ of records ({rec_in:,} → {rec_out:,}). "
                    "Moving this filter earlier (or using partition/cluster pruning) could reduce cost."
                )

            # Show meaningful steps
            if stage.steps:
                step_summary = ", ".join(
                    s.kind for s in stage.steps[:6] if s.kind
                )
                if step_summary:
                    lines.append(f"  Steps: {step_summary}")

        if bottlenecks:
            lines.append("\n## Bottlenecks Detected")
            for b in bottlenecks:
                lines.append(f"  - {b}")

        billed_gb = (getattr(job, "total_bytes_billed", None) or 0) / 1e9
        processed_gb = (getattr(job, "total_bytes_processed", None) or 0) / 1e9
        if processed_gb:
            lines.append(f"\n**Bytes processed:** {processed_gb:.3f} GB  |  **Bytes billed:** {billed_gb:.3f} GB")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"explain_query_plan failed: {e}")


# ─────────────────────────────────────────────
# detect_zombie_queries
# ─────────────────────────────────────────────
def _normalize_sql(sql: str) -> str:
    """Strip literals and whitespace for stable query fingerprinting."""
    s = sql.lower()
    s = re.sub(r"'[^']*'", "'?'", s)       # string literals
    s = re.sub(r"\b\d+\b", "?", s)          # numeric literals
    s = re.sub(r"--[^\n]*", "", s)           # line comments
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)  # block comments
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def detect_zombie_queries(
    days: int = 30,
    min_runs: int = 5,
    region: str = "us",
) -> list[types.TextContent]:
    """
    Find recurring queries with no owner labels — 'zombie' scheduled queries
    that run automatically, accumulate cost, and have no clear accountability.
    """
    try:
        client = _client()
        project = client.project

        sql = f"""
        SELECT
            SUBSTR(query, 1, 600) AS query_snippet,
            user_email,
            COUNT(*) AS run_count,
            ROUND(SUM(total_bytes_processed) / 1e12 * {_ON_DEMAND_PRICE_PER_TB}, 4) AS total_cost_usd,
            ROUND(AVG(total_bytes_processed) / 1e9, 2) AS avg_gb_per_run,
            MIN(creation_time) AS first_seen,
            MAX(creation_time) AS last_seen,
            ARRAY_LENGTH(labels) AS label_count
        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND state = 'DONE'
            AND job_type = 'QUERY'
            AND error_result IS NULL
            AND query IS NOT NULL
            AND ARRAY_LENGTH(labels) = 0
        GROUP BY query_snippet, user_email, label_count
        HAVING run_count >= {min_runs}
        ORDER BY total_cost_usd DESC
        LIMIT 25
        """

        rows = await _retry_run(lambda: list(client.query(sql).result()))

        if not rows:
            return _text(
                f"No zombie queries detected in the last {days} days "
                f"(no unlabeled queries ran {min_runs}+ times)."
            )

        total_waste = sum(float(r.total_cost_usd or 0) for r in rows)

        lines = [
            f"# Zombie Query Report — Last {days} Days\n",
            f"Found **{len(rows)}** recurring unlabeled queries.",
            f"Total estimated cost: **${total_waste:.2f} USD**\n",
            f"{'Runs':>5} {'Cost (USD)':>12} {'Avg GB':>8} {'User':<35} {'Last Seen':<18}",
            "-" * 85,
        ]

        for row in rows:
            last = row.last_seen.strftime("%Y-%m-%d %H:%M") if row.last_seen else "N/A"
            lines.append(
                f"{row.run_count:>5} ${float(row.total_cost_usd or 0):>11.4f} "
                f"{float(row.avg_gb_per_run or 0):>8.2f} {row.user_email:<35} {last:<18}"
            )
            lines.append(f"  ```sql\n  {row.query_snippet[:200].strip()}\n  ```")
            lines.append("")

        lines += [
            "---",
            "**Recommended actions:**",
            "- Add labels (`owner`, `pipeline`, `team`) to each query — see practice SQ-001.",
            "- Decommission queries that are no longer needed.",
            "- Move high-cost recurring queries to Dataform for proper lifecycle management.",
        ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Zombie query detection failed: {e}")


# ─────────────────────────────────────────────
# map_table_lineage
# ─────────────────────────────────────────────
async def map_table_lineage(
    table_ref: str,
    days: int = 30,
    region: str = "us",
    direction: str = "both",  # upstream | downstream | both
) -> list[types.TextContent]:
    """
    Parse SQL from job history to build a dependency graph for a table.
    upstream  = what tables does this table read from?
    downstream = what tables read from this table?
    """
    try:
        client = _client()
        project = client.project

        if table_ref.count(".") == 1:
            table_ref = f"{project}.{table_ref}"

        # Normalise for matching: strip project prefix for pattern matching
        parts = table_ref.split(".")
        table_short = ".".join(parts[-2:])   # dataset.table
        table_full = table_ref

        sql = f"""
        SELECT
            job_id,
            user_email,
            creation_time,
            SUBSTR(query, 1, 2000) AS query_text,
            ARRAY(
              SELECT CONCAT(t.project_id, '.', t.dataset_id, '.', t.table_id)
              FROM UNNEST(referenced_tables) AS t
            ) AS source_tables,
            destination_table
        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND state = 'DONE'
            AND job_type = 'QUERY'
            AND query IS NOT NULL
            AND (
                query LIKE '%{table_short}%'
                OR CAST(destination_table AS STRING) LIKE '%{table_short}%'
            )
        LIMIT 200
        """

        rows = await _retry_run(lambda: list(client.query(sql).result()))

        if not rows:
            return _text(
                f"No lineage data found for `{table_ref}` in the last {days} days.\n"
                "The table may not have been referenced in any recent jobs."
            )

        upstream: dict[str, int] = {}    # tables that feed INTO target
        downstream: dict[str, int] = {}  # tables that are written FROM target

        for row in rows:
            sources = list(row.source_tables or [])
            dest = row.destination_table

            # Normalise destination
            dest_str = ""
            if dest and hasattr(dest, "project_id"):
                dest_str = f"{dest.project_id}.{dest.dataset_id}.{dest.table_id}"

            target_is_source = any(table_short in s for s in sources) or table_full in sources
            target_is_dest = table_short in dest_str or table_full in dest_str

            if target_is_dest:
                # Something writes TO our table — sources are upstream
                for s in sources:
                    if s and table_short not in s:
                        upstream[s] = upstream.get(s, 0) + 1

            if target_is_source:
                # Our table is read, destination is downstream
                if dest_str and table_short not in dest_str:
                    downstream[dest_str] = downstream.get(dest_str, 0) + 1

        lines = [f"# Table Lineage: `{table_ref}`\n", f"Analysis window: last {days} days\n"]

        if direction in ("upstream", "both") and upstream:
            lines.append(f"## Upstream (feeds into `{table_short}`)")
            lines.append(f"{'Table':<65} {'Job count':>10}")
            lines.append("-" * 78)
            for t, cnt in sorted(upstream.items(), key=lambda x: -x[1]):
                lines.append(f"  {t:<65} {cnt:>10}")
            lines.append("")

        if direction in ("downstream", "both") and downstream:
            lines.append(f"## Downstream (reads from `{table_short}`)")
            lines.append(f"{'Table':<65} {'Job count':>10}")
            lines.append("-" * 78)
            for t, cnt in sorted(downstream.items(), key=lambda x: -x[1]):
                lines.append(f"  {t:<65} {cnt:>10}")
            lines.append("")

        if not upstream and not downstream:
            lines.append(
                f"No upstream or downstream dependencies found for `{table_ref}` in the last {days} days.\n"
                "The table may be a root source or may not have been used recently."
            )

        lines.append(
            "_Lineage is derived from INFORMATION_SCHEMA.JOBS SQL text and referenced_tables. "
            "It reflects query-time dependencies, not physical data movement._"
        )
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Table lineage mapping failed: {e}")


# ─────────────────────────────────────────────
# detect_performance_regression
# ─────────────────────────────────────────────
async def detect_performance_regression(
    days_recent: int = 7,
    days_baseline: int = 7,
    min_runs: int = 3,
    region: str = "us",
) -> list[types.TextContent]:
    """
    Compare query performance between a recent window and a baseline window.
    Flags queries where bytes processed or duration have significantly increased.
    """
    try:
        client = _client()
        project = client.project

        # Fetch both windows in one query, split by period
        sql = f"""
        WITH windowed AS (
            SELECT
                MD5(REGEXP_REPLACE(LOWER(query), r'\\s+', ' ')) AS query_hash,
                SUBSTR(query, 1, 300) AS query_snippet,
                CASE
                    WHEN creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_recent} DAY)
                    THEN 'recent'
                    ELSE 'baseline'
                END AS period,
                total_bytes_processed,
                TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
                total_slot_ms
            FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE
                creation_time >= TIMESTAMP_SUB(
                    CURRENT_TIMESTAMP(),
                    INTERVAL {days_recent + days_baseline} DAY
                )
                AND state = 'DONE'
                AND job_type = 'QUERY'
                AND error_result IS NULL
                AND total_bytes_processed IS NOT NULL
                AND query NOT LIKE '%INFORMATION_SCHEMA%'
        ),
        aggregated AS (
            SELECT
                query_hash,
                ANY_VALUE(query_snippet) AS query_snippet,
                period,
                COUNT(*) AS run_count,
                AVG(total_bytes_processed) AS avg_bytes,
                AVG(duration_ms) AS avg_duration_ms,
                AVG(total_slot_ms) AS avg_slot_ms
            FROM windowed
            GROUP BY query_hash, period
            HAVING run_count >= {min_runs}
        ),
        compared AS (
            SELECT
                r.query_hash,
                r.query_snippet,
                r.run_count AS recent_runs,
                b.run_count AS baseline_runs,
                r.avg_bytes AS recent_bytes,
                b.avg_bytes AS baseline_bytes,
                r.avg_duration_ms AS recent_dur_ms,
                b.avg_duration_ms AS baseline_dur_ms,
                SAFE_DIVIDE(r.avg_bytes, b.avg_bytes) AS bytes_ratio,
                SAFE_DIVIDE(r.avg_duration_ms, b.avg_duration_ms) AS dur_ratio
            FROM aggregated r
            JOIN aggregated b USING (query_hash)
            WHERE r.period = 'recent' AND b.period = 'baseline'
        )
        SELECT *
        FROM compared
        WHERE bytes_ratio > 1.3 OR dur_ratio > 1.3  -- flag 30%+ regression
        ORDER BY bytes_ratio DESC
        LIMIT 20
        """

        rows = await _retry_run(lambda: list(client.query(sql).result()))

        if not rows:
            return _text(
                f"No performance regressions detected.\n"
                f"Compared last {days_recent}d vs prior {days_baseline}d "
                f"(minimum {min_runs} runs required in each window)."
            )

        lines = [
            f"# Performance Regression Report\n",
            f"Recent window : last {days_recent} days",
            f"Baseline      : {days_recent}–{days_recent + days_baseline} days ago",
            f"Min runs      : {min_runs} per window\n",
            f"Found **{len(rows)}** regressing queries.\n",
        ]

        for i, row in enumerate(rows, 1):
            bytes_pct = (float(row.bytes_ratio or 1) - 1) * 100
            dur_pct = (float(row.dur_ratio or 1) - 1) * 100
            recent_gb = float(row.recent_bytes or 0) / 1e9
            baseline_gb = float(row.baseline_bytes or 0) / 1e9
            recent_s = float(row.recent_dur_ms or 0) / 1000
            baseline_s = float(row.baseline_dur_ms or 0) / 1000

            severity = "CRITICAL" if bytes_pct > 100 or dur_pct > 100 else "WARNING"

            lines += [
                f"## {i}. [{severity}] Bytes +{bytes_pct:.0f}%  |  Duration +{dur_pct:.0f}%",
                f"Recent: {recent_gb:.2f} GB / {recent_s:.1f}s  |  "
                f"Baseline: {baseline_gb:.2f} GB / {baseline_s:.1f}s",
                f"Runs: {row.recent_runs} recent vs {row.baseline_runs} baseline",
                f"```sql\n{row.query_snippet}\n```",
                "",
            ]

        lines += [
            "---",
            "**Common causes of regression:**",
            "- Table growth without corresponding partition filter",
            "- Schema change adding columns scanned by SELECT *",
            "- New JOIN to a large table added in a pipeline update",
            "- Partition filter removed or broken after a refactor",
        ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"Performance regression detection failed: {e}")


# ─────────────────────────────────────────────
# review_query_with_schema
# ─────────────────────────────────────────────
async def review_query_with_schema(
    sql: str,
    project_id: str | None = None,
) -> list[types.TextContent]:
    """
    Schema-aware SQL review. Extracts table references from the SQL,
    fetches their actual partition and clustering columns from BigQuery,
    then checks whether the WHERE clause actually filters on the real
    partition column — not just any date-like expression.
    """
    from sql_parser import extract_table_refs, get_where_clause, clean

    try:
        client = _client()
        project = project_id or client.project

        table_refs = extract_table_refs(sql)
        if not table_refs:
            return _text(
                "Could not extract table references from the SQL. "
                "Make sure the query contains a FROM clause with a table name."
            )

        schema_info: dict[str, dict] = {}
        for ref in table_refs:
            parts = ref.split(".")
            if len(parts) == 3:
                full_ref = ref
            elif len(parts) == 2:
                full_ref = f"{project}.{ref}"
            else:
                # Single-part — likely an alias or CTE name, skip
                continue
            try:
                table = await _run(lambda: client.get_table(full_ref))
                partition_col = None
                if table.time_partitioning:
                    partition_col = table.time_partitioning.field or "_PARTITIONTIME"
                schema_info[ref] = {
                    "full_ref": full_ref,
                    "partition_col": partition_col,
                    "clustering": table.clustering_fields or [],
                    "size_gb": (table.num_bytes or 0) / 1e9,
                    "require_partition_filter": getattr(
                        table, "require_partition_filter", False
                    ),
                }
            except Exception:
                pass  # table not accessible — skip silently

        if not schema_info:
            return _text(
                f"Could not fetch schema for any of the detected tables: {table_refs}. "
                "Ensure the authenticated service account has BigQuery Data Viewer access."
            )

        where_clause = get_where_clause(sql)
        findings: list[str] = []
        good: list[str] = []

        for ref, info in schema_info.items():
            pcol = info["partition_col"]
            clustering = info["clustering"]
            size_gb = info["size_gb"]
            full_ref = info["full_ref"]

            if pcol:
                pcol_lower = pcol.lower()
                # Check if actual partition column appears in WHERE
                # Handles: direct reference, DATE(col), TIMESTAMP_TRUNC(col, ...), col >= ...
                partition_patterns = [
                    pcol_lower,
                    f"date({pcol_lower})",
                    f"timestamp_trunc({pcol_lower}",
                    f"datetime_trunc({pcol_lower}",
                ]
                partition_filtered = any(p in where_clause for p in partition_patterns)
                # Also accept _PARTITIONTIME / _PARTITIONDATE pseudo-columns
                pseudo_filtered = "_partitiontime" in where_clause or "_partitiondate" in where_clause

                if partition_filtered or pseudo_filtered:
                    good.append(
                        f"  `{ref}` — partition filter on `{pcol}` detected. Partition pruning active."
                    )
                else:
                    est_full_scan = size_gb / 1e3 * _ON_DEMAND_PRICE_PER_TB  # cost if full scan
                    findings.append(
                        f"  `{ref}` — partitioned by `{pcol}` but no filter on this column found in WHERE. "
                        f"Full scan cost: ~${est_full_scan:.4f} USD ({size_gb:.1f} GB). "
                        f"Fix: add `AND {pcol} >= TIMESTAMP('2024-01-01')` "
                        f"(or `AND DATE({pcol}) = CURRENT_DATE()`) to your WHERE clause."
                    )
            else:
                if size_gb > 1:
                    findings.append(
                        f"  `{ref}` — {size_gb:.1f} GB table has no partitioning. "
                        "Consider adding DATE/TIMESTAMP partitioning to reduce scan cost (PT-001)."
                    )

            if clustering and where_clause:
                missing_cluster = [
                    c for c in clustering if c.lower() not in where_clause
                ]
                if missing_cluster:
                    findings.append(
                        f"  `{ref}` — clustered by [{', '.join(clustering)}] but "
                        f"[{', '.join(missing_cluster)}] not in WHERE. "
                        "Filtering on cluster columns improves pruning within partitions."
                    )
                else:
                    good.append(
                        f"  `{ref}` — all cluster columns [{', '.join(clustering)}] are filtered."
                    )

        lines = ["# Schema-Aware Query Review\n"]
        lines.append(f"Tables analysed: {', '.join(f'`{r}`' for r in schema_info)}\n")

        if findings:
            lines.append(f"## Issues ({len(findings)})\n")
            lines.extend(findings)
        if good:
            lines.append("\n## Confirmed Good\n")
            lines.extend(good)

        if not findings:
            lines.append(
                "No schema-level issues found. Partition and cluster filters look correct."
            )

        lines += [
            "",
            "_Tip: also run `review_query` for static SQL pattern checks (SELECT *, CROSS JOIN, etc.)._",
        ]
        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"review_query_with_schema failed: {e}")
