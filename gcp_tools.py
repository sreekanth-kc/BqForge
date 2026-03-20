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
