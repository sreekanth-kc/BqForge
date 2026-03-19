# BqForge – BigQuery Best Practices MCP Server

A Model Context Protocol (MCP) server that exposes curated Google BigQuery best
practices as **Tools** and **Resources**.

Designed with a **two-step workflow**: resolve a topic to ranked
practice IDs, then fetch focused content within a token budget.

## Covered Categories

| Category | Practices | ID Prefix |
|---|---|---|
| Query Optimization | 6 | QO-xxx |
| Schema Design | 5 | SD-xxx |
| Cost Management | 5 | CO-xxx |
| Security & Access Control | 5 | SE-xxx |
| Materialized Views | 5 | MV-xxx |
| Monitoring & Observability | 5 | MO-xxx |

**31 practices total.**

---

## Installation

```bash
# 1. Clone / copy the project
cd bqforge

# 2. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running the server

```bash
# Stdio mode (used by Claude Desktop and most MCP hosts)
python server.py
```

---

## Claude Desktop configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "bqforge": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["/absolute/path/to/bqforge/server.py"]
    }
  }
}
```

Restart Claude Desktop after saving.

---

## "use bqforge" – activating the server in prompts

Fetch the system prompt snippet and paste it into your system prompt or
Claude Project instructions:

```
Resource URI: bigquery://prompt
```

Or instruct the model directly:

```
use bqforge
```

Claude will then automatically call `resolve_topic` → `get_practices` whenever
BigQuery topics arise.

---

## Available Tools

### Recommended workflow

| Tool | Description |
|---|---|
| `resolve_topic` | Resolve a natural-language question to ranked practice IDs |
| `get_practices` | Fetch focused practice content within a token budget |

**Example:**
```
resolve_topic(query="reduce query cost")
→ [CO-001 (score 9), QO-001 (score 7), CO-002 (score 6), …]

get_practices(topic="reduce query cost", max_tokens=3000)
→ Markdown with the top matching practices, fitting within ~3000 tokens
```

### Other tools

| Tool | Description |
|---|---|
| `get_best_practices` | Retrieve all practices for a category |
| `search_practices` | Full-text keyword search across all practices |
| `get_practice_detail` | Get full detail for a single practice by ID |
| `review_query` | Analyse a SQL query for best-practice violations |
| `list_all_practice_ids` | Compact list of every practice ID + title |

---

## Example prompts

```
What are BigQuery query optimisation best practices?
→ get_best_practices(category="query_optimization")

How do I reduce BigQuery query cost?
→ resolve_topic(query="reduce query cost")
  get_practices(topic="reduce query cost", max_tokens=3000)

How should I handle PII in BigQuery?
→ resolve_topic(query="PII column security")
  get_practices(topic="PII column security")

Review this query: SELECT * FROM orders WHERE status = 'OPEN'
→ review_query(sql="SELECT * FROM orders WHERE status = 'OPEN'")

Tell me everything about practice QO-002
→ get_practice_detail(practice_id="QO-002")

What monitoring should I set up for BigQuery?
→ get_best_practices(category="monitoring")
```

---

## Available Resources

| URI | Description |
|---|---|
| `bigquery://overview` | High-level overview of all categories |
| `bigquery://prompt` | "use bqforge" system prompt snippet |
| `bigquery://query_optimization` | Full query optimisation practices (JSON) |
| `bigquery://schema_design` | Full schema design practices (JSON) |
| `bigquery://cost_management` | Full cost management practices (JSON) |
| `bigquery://security` | Full security practices (JSON) |
| `bigquery://materialized_views` | Full materialized views practices (JSON) |
| `bigquery://monitoring` | Full monitoring & observability practices (JSON) |

---

## Project Structure

```
bqforge/
├── server.py                    # MCP server entry point
├── data/
│   ├── __init__.py
│   ├── query_optimization.py    # QO-xxx (6 practices)
│   ├── schema_design.py         # SD-xxx (5 practices)
│   ├── cost_management.py       # CO-xxx (5 practices)
│   ├── security.py              # SE-xxx (5 practices)
│   ├── materialized_views.py    # MV-xxx (5 practices)
│   └── monitoring.py            # MO-xxx (5 practices)
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## Adding new practices

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
