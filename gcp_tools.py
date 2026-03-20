"""
GCP-powered BqForge tools.

All public coroutines return list[types.TextContent] matching the MCP contract.
Blocking BigQuery SDK calls are dispatched to a thread pool via asyncio.to_thread.
"""

import asyncio
from datetime import datetime, timezone

from mcp import types

_ON_DEMAND_PRICE_PER_TB = 6.25  # USD, BigQuery on-demand pricing

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

        rows = await _run(lambda: list(client.query(sql).result()))

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

        rows = await _run(lambda: list(client.query(sql).result()))

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

        rows = await _run(lambda: list(client.query(sql).result()))
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
        rows = await _run(lambda: list(client.query(sql).result()))
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
        rows = await _run(lambda: list(client.query(sql).result()))
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
