QUERY_OPTIMIZATION = {
    "title": "Query Optimization",
    "description": (
        "Best practices to write efficient, performant BigQuery SQL that minimises "
        "bytes processed and maximises slot utilisation."
    ),
    "practices": [
        {
            "id": "QO-001",
            "title": "Select only the columns you need",
            "severity": "HIGH",
            "impact": "Cost & Performance",
            "description": (
                "BigQuery is columnar; SELECT * reads every column and bills you for "
                "all bytes in those columns. Always project only the fields required."
            ),
            "do": [
                "SELECT user_id, event_type, created_at FROM events",
                "Use column pruning in views and materialised views",
            ],
            "dont": [
                "SELECT * FROM large_table",
                "SELECT * in subqueries or CTEs",
            ],
            "example": "SELECT order_id, total_amount FROM orders WHERE status = 'COMPLETED'",
        },
        {
            "id": "QO-002",
            "title": "Filter on partition and cluster columns early",
            "severity": "HIGH",
            "impact": "Cost & Performance",
            "description": (
                "Partition pruning eliminates entire partitions from the scan. "
                "Clustering reduces blocks read within a partition. Always filter "
                "on the partition column (usually a DATE/TIMESTAMP) and cluster keys."
            ),
            "do": [
                "WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31'",
                "WHERE region = 'us-east' AND event_date = CURRENT_DATE()",
                "Declare partition expiration to auto-delete old data",
            ],
            "dont": [
                "WHERE DATE(timestamp_col) = '2024-01-01'  -- wrapping disables pruning",
                "WHERE CAST(partition_col AS STRING) = '20240101'",
            ],
            "example": (
                "SELECT * FROM `project.dataset.events`\n"
                "WHERE event_date = '2024-06-15'  -- partition filter\n"
                "  AND country = 'IN'             -- cluster filter"
            ),
        },
        {
            "id": "QO-003",
            "title": "Avoid data skew in JOINs and GROUP BY",
            "severity": "MEDIUM",
            "impact": "Performance",
            "description": (
                "When a join key or GROUP BY key has very uneven distribution "
                "(e.g., a single NULL or 'unknown' value represents 80% of rows), "
                "one worker slot gets overloaded. Pre-filter nulls and consider "
                "approximate aggregations."
            ),
            "do": [
                "Filter NULL keys before joining: WHERE key IS NOT NULL",
                "Use APPROX_COUNT_DISTINCT() instead of COUNT(DISTINCT ...) for large sets",
                "Use TABLESAMPLE for exploratory queries on skewed data",
            ],
            "dont": [
                "JOIN on columns with high NULL rates without filtering",
                "GROUP BY on low-cardinality skewed columns without pre-filtering",
            ],
            "example": (
                "-- Pre-filter NULLs to avoid skew\n"
                "SELECT a.user_id, SUM(b.amount)\n"
                "FROM users a\n"
                "JOIN transactions b ON a.user_id = b.user_id\n"
                "WHERE a.user_id IS NOT NULL\n"
                "GROUP BY a.user_id"
            ),
        },
        {
            "id": "QO-004",
            "title": "Use CTEs instead of deeply nested subqueries",
            "severity": "LOW",
            "impact": "Readability & Maintainability",
            "description": (
                "Common Table Expressions (WITH clauses) improve readability and "
                "allow BigQuery's optimiser to materialise intermediate results. "
                "Deeply nested subqueries are hard to debug and optimise."
            ),
            "do": [
                "WITH active_users AS (SELECT ... FROM users WHERE active = TRUE)\n  SELECT ... FROM active_users",
                "Name CTEs descriptively",
            ],
            "dont": [
                "SELECT * FROM (SELECT * FROM (SELECT * FROM ...))",
                "Duplicate the same subquery in multiple places",
            ],
            "example": (
                "WITH orders_2024 AS (\n"
                "  SELECT order_id, customer_id, amount\n"
                "  FROM orders\n"
                "  WHERE order_date >= '2024-01-01'\n"
                "),\nhigh_value AS (\n"
                "  SELECT * FROM orders_2024 WHERE amount > 1000\n"
                ")\nSELECT customer_id, COUNT(*) AS big_orders\n"
                "FROM high_value\nGROUP BY customer_id"
            ),
        },
        {
            "id": "QO-005",
            "title": "Prefer JOIN over correlated subqueries",
            "severity": "HIGH",
            "impact": "Performance",
            "description": (
                "Correlated subqueries execute once per row of the outer query. "
                "A JOIN is processed in parallel across slots and is almost always faster."
            ),
            "do": [
                "Rewrite correlated subqueries as LEFT JOIN ... IS NULL for NOT EXISTS patterns",
                "Use window functions instead of self-joins for running totals/ranks",
            ],
            "dont": [
                "WHERE col IN (SELECT col FROM other WHERE outer_col = this_col)",
                "SELECT (SELECT MAX(x) FROM t WHERE t.id = o.id) FROM o -- per-row subquery",
            ],
            "example": (
                "-- Instead of correlated subquery\n"
                "SELECT o.order_id\n"
                "FROM orders o\n"
                "LEFT JOIN refunds r ON o.order_id = r.order_id\n"
                "WHERE r.order_id IS NULL  -- orders with no refund"
            ),
        },
        {
            "id": "QO-006",
            "title": "Use approximate aggregation functions for analytics",
            "severity": "MEDIUM",
            "impact": "Performance & Cost",
            "description": (
                "For dashboards and explorations where ~1% error is acceptable, "
                "APPROX_COUNT_DISTINCT and HyperLogLog sketches are orders of magnitude "
                "faster and cheaper than exact COUNT(DISTINCT)."
            ),
            "do": [
                "APPROX_COUNT_DISTINCT(user_id) for DAU/MAU metrics",
                "APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] for p95 latency",
                "HLL_COUNT.INIT + HLL_COUNT.MERGE for cross-query aggregation",
            ],
            "dont": [
                "COUNT(DISTINCT user_id) across billions of rows without need for exactness",
            ],
            "example": (
                "SELECT\n"
                "  event_date,\n"
                "  APPROX_COUNT_DISTINCT(user_id) AS dau\n"
                "FROM events\n"
                "WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)\n"
                "GROUP BY event_date\n"
                "ORDER BY event_date"
            ),
        },
        {
            "id": "QO-007",
            "title": "Optimise JOIN order — largest table first",
            "severity": "MEDIUM",
            "impact": "Performance",
            "description": (
                "BigQuery's distributed JOIN engine broadcasts smaller tables to all "
                "workers. Placing the largest table first (left side) and the smallest "
                "last enables broadcast joins, reducing shuffle overhead. Pre-aggregate "
                "before joining to reduce row counts early."
            ),
            "do": [
                "Put the largest fact table on the left, dimension tables on the right",
                "Pre-aggregate dimension tables with GROUP BY before joining to the fact",
                "Use EXPLAIN or the query plan to verify join strategy chosen by BQ",
            ],
            "dont": [
                "Join a 10 TB fact table to a 500 GB intermediate result without pre-filtering",
                "Assume BQ always picks the optimal join order without inspection",
            ],
            "example": (
                "-- Largest table (orders) first, smaller lookup (products) last\n"
                "SELECT o.order_id, p.product_name, o.amount\n"
                "FROM dataset.orders o          -- large\n"
                "JOIN dataset.products p        -- small lookup\n"
                "  ON o.product_id = p.product_id\n"
                "WHERE o.order_date >= '2024-01-01'"
            ),
        },
        {
            "id": "QO-008",
            "title": "Replace self-joins with window functions",
            "severity": "HIGH",
            "impact": "Performance & Cost",
            "description": (
                "Self-joins scan the same table twice and can square the output row "
                "count. Window functions (LAG, LEAD, SUM OVER, RANK) achieve running "
                "totals, comparisons to previous rows, and rankings in a single pass."
            ),
            "do": [
                "Use LAG(col, 1) OVER (PARTITION BY id ORDER BY ts) for previous-row comparisons",
                "Use SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) for running totals",
                "Use RANK() / ROW_NUMBER() instead of joining a subquery that counts preceding rows",
            ],
            "dont": [
                "JOIN t AS a ON a.id = b.id AND a.ts < b.ts to get the previous event",
                "Use self-joins to compute running aggregates on large tables",
            ],
            "example": (
                "-- Running revenue per user — window function, no self-join\n"
                "SELECT\n"
                "  user_id,\n"
                "  order_date,\n"
                "  amount,\n"
                "  SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) AS running_total\n"
                "FROM dataset.orders"
            ),
        },
        {
            "id": "QO-009",
            "title": "Use SEARCH() and search indexes for full-text lookups",
            "severity": "MEDIUM",
            "impact": "Performance",
            "description": (
                "The BigQuery SEARCH() function with a search index performs "
                "inverted-index lookups on STRING, JSON, and ARRAY<STRING> columns, "
                "dramatically outperforming LIKE '%term%' or REGEXP_CONTAINS on large "
                "tables (> 10 GB). Indexes are automatically maintained."
            ),
            "do": [
                "CREATE SEARCH INDEX idx ON table(ALL COLUMNS) for broad text search",
                "Use SEARCH(table, 'keyword') or SEARCH(col, 'term') in WHERE clauses",
                "Combine SEARCH() with partition filters for maximum pruning",
                "Check index usage with INFORMATION_SCHEMA.SEARCH_INDEXES",
            ],
            "dont": [
                "Create search indexes on tables < 10 GB (BQ won't populate them)",
                "Index columns with very few unique values (no benefit)",
                "Use LIKE '%keyword%' on multi-TB text columns when SEARCH() is available",
            ],
            "example": (
                "-- Create index then use SEARCH() for fast text lookup\n"
                "CREATE SEARCH INDEX log_idx ON dataset.app_logs(log_message);\n\n"
                "SELECT request_id, log_message\n"
                "FROM dataset.app_logs\n"
                "WHERE log_date = CURRENT_DATE()\n"
                "  AND SEARCH(log_message, 'NullPointerException')"
            ),
        },
        {
            "id": "QO-010",
            "title": "Optimise UNNEST patterns for ARRAY and STRUCT columns",
            "severity": "MEDIUM",
            "impact": "Performance & Cost",
            "description": (
                "UNNEST explodes array elements into rows. Unnecessary unnesting of "
                "large arrays multiplies row counts and increases shuffle costs. "
                "Filter before unnesting and avoid unnesting multiple independent "
                "arrays in the same query (produces a cross-product)."
            ),
            "do": [
                "Filter at the top level before unnesting with EXISTS(SELECT 1 FROM UNNEST(arr) AS x WHERE x = val)",
                "Use ARRAY_LENGTH() to pre-check array size before unnesting",
                "Use STRUCT field access (col.field) without UNNEST when you only need one field",
            ],
            "dont": [
                "CROSS JOIN UNNEST(arr1), UNNEST(arr2) — produces Cartesian product",
                "UNNEST a large array column and then immediately re-aggregate it (use ARRAY functions instead)",
            ],
            "example": (
                "-- Check array contains value WITHOUT full unnest\n"
                "SELECT user_id\n"
                "FROM dataset.users\n"
                "WHERE EXISTS (\n"
                "  SELECT 1\n"
                "  FROM UNNEST(interest_tags) AS tag\n"
                "  WHERE tag = 'sports'\n"
                ")\n\n"
                "-- Access nested struct field directly (no UNNEST needed)\n"
                "SELECT address.city FROM dataset.users"
            ),
        },
        {
            "id": "QO-011",
            "title": "Use TABLESAMPLE for fast exploratory queries",
            "severity": "LOW",
            "impact": "Cost & Speed",
            "description": (
                "TABLESAMPLE SYSTEM draws a random percentage of data blocks, "
                "reducing bytes scanned proportionally. It is ideal for validating "
                "query logic, profiling distributions, and building quick prototypes "
                "before running full scans."
            ),
            "do": [
                "SELECT ... FROM large_table TABLESAMPLE SYSTEM (1 PERCENT) for quick profiling",
                "Combine with partition filter to sample from a specific time range",
                "Use for schema validation and data-quality checks before full ETL runs",
            ],
            "dont": [
                "Use TABLESAMPLE in production aggregation pipelines where accuracy matters",
                "Assume TABLESAMPLE always returns exactly N% (it samples blocks, not rows)",
            ],
            "example": (
                "-- Profile value distribution on 1% of the table at 1% of the cost\n"
                "SELECT\n"
                "  status,\n"
                "  COUNT(*) AS cnt\n"
                "FROM dataset.orders TABLESAMPLE SYSTEM (1 PERCENT)\n"
                "GROUP BY status\n"
                "ORDER BY cnt DESC"
            ),
        },
        {
            "id": "QO-012",
            "title": "Use granular wildcard table prefixes",
            "severity": "MEDIUM",
            "impact": "Cost & Performance",
            "description": (
                "Wildcard tables (`` dataset.prefix_* ``) scan every matching table. "
                "The more specific the prefix, the fewer tables scanned. Always add "
                "a _TABLE_SUFFIX filter to push down partition elimination across "
                "the matched shards."
            ),
            "do": [
                "FROM `dataset.events_2024*` rather than `dataset.events_*`",
                "WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131' to restrict scan range",
                "Consider migrating date-sharded tables to a single partitioned table for better pruning",
            ],
            "dont": [
                "FROM `dataset.*` — scans every table in the dataset",
                "Use wildcards without a _TABLE_SUFFIX filter clause",
            ],
            "example": (
                "SELECT event_type, COUNT(*) AS cnt\n"
                "FROM `project.dataset.events_2024*`\n"
                "WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0131'  -- Jan 2024 only\n"
                "GROUP BY event_type"
            ),
        },
    ],
}
