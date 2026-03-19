<div align="center">

![BqForge Preview](assets/Screenshot%202026-03-20%20at%202.52.54%20AM.png)

# BqForge

### BigQuery Best Practices MCP Server

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![MCP](https://img.shields.io/badge/MCP-Compatible-6C47FF?style=for-the-badge)
![BigQuery](https://img.shields.io/badge/Google-BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)

A Model Context Protocol (MCP) server that exposes curated Google BigQuery best practices as **Tools** and **Resources** — directly inside Claude and other MCP-compatible AI clients.

**[Live Site](https://sreekanth-kc.github.io/BqForge-FE/)**

</div>

---

## Covered Categories

| Category | Practices | ID Prefix |
|---|:---:|:---:|
| Query Optimization | 6 | `QO-xxx` |
| Schema Design | 5 | `SD-xxx` |
| Cost Management | 5 | `CO-xxx` |
| Security & Access Control | 5 | `SE-xxx` |
| Materialized Views | 5 | `MV-xxx` |
| Monitoring & Observability | 5 | `MO-xxx` |
| Workload Management | 5 | `WM-xxx` |
| Data Ingestion | 5 | `DI-xxx` |

> **62 practices total** across 8 categories

---

## How It Works

BqForge uses a two-step workflow:

```
1. resolve_topic("reduce query cost")
   → ranked list of relevant practice IDs

2. get_practices(topic="reduce query cost", max_tokens=3000)
   → focused markdown content within your token budget
```

No need to call tools manually — just configure the system prompt once and Claude handles the rest.

---

## Installation

```bash
# Clone the repo
git clone https://github.com/sreekanth-kc/BqForge.git
cd BqForge

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Claude Desktop Setup

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "bqforge": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["/absolute/path/to/BqForge/server.py"]
    }
  }
}
```

Restart Claude Desktop after saving.

---

## Activating in Prompts

Fetch the system prompt snippet and paste it into your Claude Project instructions:

```
Resource URI: bigquery://prompt
```

Or just tell Claude:

```
use bqforge best practices
```

Claude will automatically call `resolve_topic` → `get_practices` whenever BigQuery topics come up.

---

## Available Tools

### Workflow tools

| Tool | Description |
|---|---|
| `resolve_topic` | Resolve a natural-language question to ranked practice IDs |
| `get_practices` | Fetch focused practice content within a token budget |

### Utility tools

| Tool | Description |
|---|---|
| `get_best_practices` | Retrieve all practices for a category |
| `search_practices` | Full-text keyword search across all practices |
| `get_practice_detail` | Get full detail for a single practice by ID |
| `review_query` | Analyse a BigQuery SQL query for best-practice violations |
| `list_all_practice_ids` | Compact list of every practice ID and title |

---

## Example Prompts

```
What are BigQuery query optimisation best practices?
→ get_best_practices(category="query_optimization")

How do I reduce BigQuery query cost?
→ resolve_topic("reduce query cost")
  get_practices(topic="reduce query cost", max_tokens=3000)

How should I handle PII in BigQuery?
→ resolve_topic("PII column security")
  get_practices(topic="PII column security")

Review this query: SELECT * FROM orders WHERE status = 'OPEN'
→ review_query(sql="SELECT * FROM orders WHERE status = 'OPEN'")

Tell me everything about practice QO-002
→ get_practice_detail(practice_id="QO-002")
```

---

## Available Resources

| URI | Description |
|---|---|
| `bigquery://overview` | High-level overview of all categories |
| `bigquery://prompt` | System prompt snippet for Claude projects |
| `bigquery://query_optimization` | Full query optimisation practices (JSON) |
| `bigquery://schema_design` | Full schema design practices (JSON) |
| `bigquery://cost_management` | Full cost management practices (JSON) |
| `bigquery://security` | Full security practices (JSON) |
| `bigquery://materialized_views` | Full materialized views practices (JSON) |
| `bigquery://monitoring` | Full monitoring & observability practices (JSON) |

---

## Project Structure

```
BqForge/
├── server.py                      # MCP server entry point
├── data/
│   ├── __init__.py
│   ├── query_optimization.py      # QO-xxx
│   ├── schema_design.py           # SD-xxx
│   ├── cost_management.py         # CO-xxx
│   ├── security.py                # SE-xxx
│   ├── materialized_views.py      # MV-xxx
│   ├── monitoring.py              # MO-xxx
│   ├── workload_management.py     # WM-xxx
│   └── data_ingestion.py          # DI-xxx
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## Adding New Practices

1. Open the relevant file in `data/` (or create a new one).
2. Add a dict to the `"practices"` list:

```python
{
    "id": "QO-007",
    "title": "...",
    "severity": "HIGH | MEDIUM | LOW",
    "impact": "...",
    "description": "...",
    "do": ["..."],
    "dont": ["..."],
    "example": "...",  # optional SQL/code snippet
}
```

3. If adding a new category file, import it in `server.py`'s `ALL_PRACTICES` dict.
4. Restart the server.
