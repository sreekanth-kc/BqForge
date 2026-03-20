AUTHORIZED_VIEWS = {
    "title": "Authorized Views & Row-Level Security",
    "description": (
        "Best practices for controlling data access in BigQuery using authorized views, "
        "authorized datasets, and row-level security policies."
    ),
    "practices": [
        {
            "id": "AV-001",
            "title": "Use authorized views instead of duplicating data for access control",
            "severity": "high",
            "impact": "security",
            "description": (
                "Authorized views let you share a filtered subset of a table without copying data. "
                "Users query the view without needing access to the underlying source table."
            ),
            "do": [
                "Create views that filter sensitive columns/rows and authorize them on the source dataset.",
                "Use SESSION_USER() in view definitions to apply per-user row filters.",
                "Prefer authorized views over table copies when the data changes frequently.",
            ],
            "dont": [
                "Don't grant direct table access when a view with row/column filtering is sufficient.",
                "Don't duplicate tables across projects just to isolate access.",
                "Don't forget to authorize the view — without authorization users get permission errors.",
            ],
            "example": (
                "-- 1. Create the view in a separate dataset:\n"
                "CREATE VIEW reporting_dataset.orders_view AS\n"
                "SELECT order_id, customer_id, total\n"
                "FROM source_dataset.orders\n"
                "WHERE region = 'US';\n\n"
                "-- 2. Authorize the view on the source dataset (Console or API):\n"
                "-- BigQuery Console → source_dataset → Sharing → Authorize views → add reporting_dataset.orders_view"
            ),
        },
        {
            "id": "AV-002",
            "title": "Apply row-level security with session user filters",
            "severity": "high",
            "impact": "security",
            "description": (
                "Use SESSION_USER() in authorized views or BigQuery Row Access Policies "
                "to restrict which rows each user or group can see."
            ),
            "do": [
                "Use Row Access Policies (CREATE ROW ACCESS POLICY) for dynamic, group-based row filtering.",
                "Use SESSION_USER() in views for simple single-user filtering.",
                "Test row-level policies with different test users before deploying to production.",
            ],
            "dont": [
                "Don't rely solely on application-layer filters — enforce at the data layer.",
                "Don't create one view per user — use row access policies that scale.",
            ],
            "example": (
                "-- Row Access Policy: each user sees only their own department's rows\n"
                "CREATE ROW ACCESS POLICY dept_filter\n"
                "ON my_dataset.employee_data\n"
                "GRANT TO ('group:analysts@company.com')\n"
                "FILTER USING (department = SESSION_USER());"
            ),
        },
        {
            "id": "AV-003",
            "title": "Use column-level security for sensitive fields",
            "severity": "critical",
            "impact": "security",
            "description": (
                "BigQuery Policy Tags allow column-level access control enforced at query time. "
                "Users without the correct IAM role on a policy tag will receive an error when "
                "selecting that column, even if they have table-level access."
            ),
            "do": [
                "Tag PII and sensitive columns (SSN, email, phone) with Policy Tags in Data Catalog.",
                "Grant Fine-Grained Reader role only to users who legitimately need the raw values.",
                "Use data masking rules to show partial/hashed values to analysts who don't need the raw data.",
            ],
            "dont": [
                "Don't store PII in untagged columns accessible to all data warehouse users.",
                "Don't use authorized views as the sole mechanism for column-level security on PII.",
            ],
            "example": (
                "-- After tagging columns in Data Catalog, query fails for unauthorized users:\n"
                "-- Error: Access Denied: Column 'email' is restricted by column-level security policy.\n\n"
                "-- Grant access:\n"
                "-- IAM → Grant 'BigQuery Fine-Grained Reader' role on the policy tag resource"
            ),
        },
        {
            "id": "AV-004",
            "title": "Audit authorized view and IAM usage regularly",
            "severity": "medium",
            "impact": "security",
            "description": (
                "Authorized views and IAM bindings on BigQuery datasets accumulate over time. "
                "Regular audits prevent privilege creep and dangling access to stale views."
            ),
            "do": [
                "Use Cloud Audit Logs (DATA_READ, DATA_WRITE) to track who queries sensitive views.",
                "Periodically review authorized view lists on datasets using the BigQuery API.",
                "Remove authorization for deleted or deprecated views immediately.",
            ],
            "dont": [
                "Don't rely on manual tracking of who has view authorization.",
                "Don't authorize views from dev datasets on production source tables.",
            ],
            "example": (
                "-- Query Data Access audit logs in BigQuery sink:\n"
                "SELECT protopayload_auditlog.authenticationInfo.principalEmail,\n"
                "       protopayload_auditlog.resourceName,\n"
                "       timestamp\n"
                "FROM `my_project.audit_logs.cloudaudit_googleapis_com_data_access`\n"
                "WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)\n"
                "  AND protopayload_auditlog.resourceName LIKE '%sensitive_view%';"
            ),
        },
    ],
}
