SECURITY = {
    "title": "Security & Access Control",
    "description": (
        "Best practices to secure BigQuery data, control access at the right "
        "granularity, and maintain compliance with data-governance policies."
    ),
    "practices": [
        {
            "id": "SE-001",
            "title": "Apply principle of least privilege with IAM",
            "severity": "HIGH",
            "impact": "Security & Compliance",
            "description": (
                "Grant the minimum BigQuery IAM role required for each principal. "
                "Prefer predefined roles over primitive roles (Owner/Editor) and "
                "grant at the dataset or table level, not the project level."
            ),
            "do": [
                "Grant roles/bigquery.dataViewer at dataset level for analysts",
                "Grant roles/bigquery.jobUser at project level for query execution",
                "Use Service Accounts for pipelines, not personal user credentials",
                "Use IAM Conditions for time-bound or attribute-based access",
            ],
            "dont": [
                "Grant roles/bigquery.admin to all developers",
                "Use primitive roles Owner or Editor for data access",
                "Share service account JSON keys; use Workload Identity instead",
            ],
            "example": (
                "# Grant dataset-level viewer (gcloud)\n"
                "gcloud projects add-iam-policy-binding PROJECT_ID \\\n"
                "  --member='serviceAccount:pipeline@project.iam.gserviceaccount.com' \\\n"
                "  --role='roles/bigquery.dataViewer'"
            ),
        },
        {
            "id": "SE-002",
            "title": "Use column-level and row-level security",
            "severity": "HIGH",
            "impact": "Data Privacy",
            "description": (
                "Column-level security (Policy Tags + Data Catalog) restricts access "
                "to PII/sensitive columns. Row-level security (Row Access Policies) "
                "filters rows based on the querying user's attributes."
            ),
            "do": [
                "Tag PII columns (SSN, email, phone) with Policy Tags in Data Catalog",
                "Assign roles/datacatalog.categoryFineGrainedReader only to authorised users",
                "Create Row Access Policies for multi-tenant tables",
            ],
            "dont": [
                "Rely solely on view-based access control for sensitive columns",
                "Store un-masked PII in a table accessible to all analysts",
            ],
            "example": (
                "-- Row access policy: sales reps see only their region\n"
                "CREATE ROW ACCESS POLICY region_filter\n"
                "ON dataset.sales\n"
                "GRANT TO ('group:apac-sales@company.com')\n"
                "FILTER USING (region = 'APAC')"
            ),
        },
        {
            "id": "SE-003",
            "title": "Encrypt sensitive data with Cloud KMS CMEK",
            "severity": "MEDIUM",
            "impact": "Compliance",
            "description": (
                "By default BigQuery encrypts data with Google-managed keys. "
                "For regulatory requirements (HIPAA, PCI-DSS) use Customer-Managed "
                "Encryption Keys (CMEK) via Cloud KMS to retain key control."
            ),
            "do": [
                "Specify kms_key_name when creating datasets/tables that hold regulated data",
                "Rotate KMS keys annually or per your compliance policy",
                "Use VPC Service Controls to prevent data exfiltration",
            ],
            "dont": [
                "Assume Google-managed encryption satisfies all compliance frameworks without review",
                "Store KMS key IDs or credentials in query strings or code",
            ],
            "example": (
                "CREATE TABLE dataset.phi_records\n"
                "OPTIONS(\n"
                "  kms_key_name = 'projects/proj/locations/us/keyRings/ring/cryptoKeys/bq-key'\n"
                ")\n"
                "AS SELECT * FROM staging.phi_staging"
            ),
        },
        {
            "id": "SE-004",
            "title": "Audit and monitor data access with Cloud Audit Logs",
            "severity": "HIGH",
            "impact": "Security & Compliance",
            "description": (
                "Enable Data Access audit logs (DATA_READ, DATA_WRITE) for BigQuery. "
                "Export logs to a centralised SIEM or BigQuery log sink for "
                "anomaly detection and compliance reporting."
            ),
            "do": [
                "Enable DATA_READ and DATA_WRITE audit logs for bigquery.googleapis.com",
                "Create a log sink to a locked BigQuery dataset for long-term retention",
                "Alert on unusual large-scan jobs or off-hours access patterns",
                "Use INFORMATION_SCHEMA.JOBS_BY_PROJECT for query-level audit trails",
            ],
            "dont": [
                "Rely only on ADMIN_READ logs (they miss data access events)",
                "Delete or restrict access to the audit log dataset",
            ],
            "example": (
                "-- Find top data consumers in the last 7 days\n"
                "SELECT user_email, SUM(total_bytes_billed)/1e12 AS tb_billed\n"
                "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT\n"
                "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)\n"
                "  AND job_type = 'QUERY'\n"
                "GROUP BY user_email\n"
                "ORDER BY tb_billed DESC\n"
                "LIMIT 20"
            ),
        },
        {
            "id": "SE-005",
            "title": "Use VPC Service Controls and authorised views",
            "severity": "MEDIUM",
            "impact": "Data Exfiltration Prevention",
            "description": (
                "VPC Service Controls create a security perimeter around BigQuery, "
                "preventing data exfiltration to unauthorised projects. "
                "Authorised views let you share query results without exposing the underlying table."
            ),
            "do": [
                "Enroll BigQuery in a VPC Service Controls perimeter for sensitive projects",
                "Create authorised views to share aggregated/masked data with analysts",
                "Use Authorised Datasets to grant view access at dataset level",
            ],
            "dont": [
                "Grant direct table access when a curated view is sufficient",
                "Allow ingress from untrusted projects inside the same perimeter",
            ],
            "example": (
                "-- Create an authorised view that masks email\n"
                "CREATE VIEW analytics.users_masked AS\n"
                "SELECT\n"
                "  user_id,\n"
                "  CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[OFFSET(1)]) AS email_masked,\n"
                "  country,\n"
                "  created_date\n"
                "FROM pii_dataset.users"
            ),
        },
        {
            "id": "SE-006",
            "title": "Apply dynamic data masking to sensitive columns",
            "severity": "HIGH",
            "impact": "Data Privacy",
            "description": (
                "BigQuery dynamic data masking (built on top of Policy Tags) lets you "
                "define masking rules (SHA256 hash, nullify, default value, last 4 chars) "
                "that are applied automatically based on the querying user's role. "
                "Analysts see masked values; authorised roles see the original data — "
                "all without query-level rewrites."
            ),
            "do": [
                "Create a masking policy via Data Catalog for each PII classification level",
                "Assign roles/datacatalog.maskedReader to analyst groups for auto-masking",
                "Use SHA256 masking for tokens that should still be joinable across datasets",
                "Audit masking policy assignments with INFORMATION_SCHEMA.COLUMN_FIELD_PATHS",
            ],
            "dont": [
                "Rely on application-layer masking that can be bypassed by direct BigQuery access",
                "Store unmasked and masked copies of the same data in separate tables",
            ],
            "example": (
                "-- Create a masking rule that hashes email for non-privileged users\n"
                "CREATE OR REPLACE DATA MASKING POLICY hash_email\n"
                "USING (SHA256(CAST(email AS BYTES)));\n\n"
                "-- Assign masking to a Policy Tag column\n"
                "-- (done via Data Catalog UI or Terraform google_data_catalog_policy_tag_iam_binding)"
            ),
        },
        {
            "id": "SE-007",
            "title": "Use Workload Identity Federation instead of service account keys",
            "severity": "HIGH",
            "impact": "Security",
            "description": (
                "Downloading service account JSON key files is a major security risk — "
                "keys can be leaked, are hard to rotate, and persist indefinitely if "
                "not managed. Workload Identity Federation (and Workload Identity for GKE) "
                "lets external workloads (AWS, GitHub Actions, on-prem) authenticate "
                "as a service account without a long-lived credential."
            ),
            "do": [
                "Use Workload Identity Federation for CI/CD pipelines (GitHub Actions, Jenkins)",
                "Use Workload Identity for GKE pods instead of mounted key files",
                "Audit and rotate all existing SA keys: gcloud iam service-accounts keys list",
                "Set organisation policy constraints/iam.disableServiceAccountKeyCreation",
            ],
            "dont": [
                "Commit service account key JSON files to source control",
                "Pass SA key files via environment variables in containers",
                "Create long-lived keys when a short-lived federated token suffices",
            ],
            "example": (
                "# GitHub Actions: authenticate via Workload Identity (no key file)\n"
                "- uses: google-github-actions/auth@v2\n"
                "  with:\n"
                "    workload_identity_provider: 'projects/123/locations/global/workloadIdentityPools/pool/providers/github'\n"
                "    service_account: 'bq-pipeline@project.iam.gserviceaccount.com'"
            ),
        },
        {
            "id": "SE-008",
            "title": "Enable and govern data lineage with Dataplex",
            "severity": "MEDIUM",
            "impact": "Compliance & Governance",
            "description": (
                "Dataplex automatically tracks table-level and column-level data lineage "
                "across BigQuery jobs, Dataflow, and Spark. Lineage is essential for "
                "impact analysis (which dashboards break if I rename a column?), "
                "GDPR/CCPA right-to-erasure, and regulatory compliance audits."
            ),
            "do": [
                "Enable Dataplex lineage API: gcloud services enable datalineage.googleapis.com",
                "Use Dataplex Universal Catalog for centralised metadata and lineage browsing",
                "Emit custom lineage events for non-BigQuery transformations (dbt, Spark)",
                "Query lineage programmatically via the Data Lineage API for automated impact analysis",
            ],
            "dont": [
                "Rely solely on BigQuery INFORMATION_SCHEMA for lineage (covers only BQ jobs)",
                "Ignore lineage tracking for PII columns subject to data-subject deletion requests",
            ],
            "example": (
                "# Enable Dataplex lineage (one-time setup)\n"
                "gcloud services enable datalineage.googleapis.com --project=PROJECT_ID\n\n"
                "# Lineage is then automatically captured for BigQuery jobs.\n"
                "# View in Console: BigQuery → table → Lineage tab"
            ),
        },
    ],
}
