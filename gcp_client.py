"""
GCP authentication for BqForge.

Priority order:
  1. GCP_SERVICE_ACCOUNT_JSON  — inline service-account JSON string (best for MCP config)
  2. GOOGLE_APPLICATION_CREDENTIALS — path to a service-account key file
  3. Application Default Credentials (ADC) — local `gcloud auth application-default login`
"""

import json
import os
from typing import Optional

from google.cloud import bigquery
from google.oauth2 import service_account

_client: Optional[bigquery.Client] = None
_BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]


def get_client() -> bigquery.Client:
    global _client
    if _client is not None:
        return _client

    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if sa_json:
        info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=_BQ_SCOPES
        )
        _client = bigquery.Client(
            credentials=credentials,
            project=info.get("project_id"),
        )
        return _client

    # GOOGLE_APPLICATION_CREDENTIALS or ADC — SDK picks these up automatically
    _client = bigquery.Client()
    return _client


def reset_client() -> None:
    """Force re-initialisation on next get_client() call."""
    global _client
    _client = None


def gcp_available() -> bool:
    """Return True if explicit GCP credentials are configured."""
    return bool(
        os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
