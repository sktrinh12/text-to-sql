# Text-to-SQL — LangGraph + MCP

A natural language to SQL agent that converts plain English questions into
executable PostgreSQL queries. It introspects the live database schema,
generates SQL via a free-tier LLM, validates it statically with SQLGlot,
and executes it (using mcp2cli in order to simplify code) — with an automatic correction loop for queries that fail. 
Note `mcp2cli` is mainly used to economise on token usage but in this case it was used for simplicity. Since the design does not provide tool-calling for the LLM, that is, the workflow is deterministic — validate always runs, execute always follows a valid query. Giving the LLM agency over that would add non-determinism and potentially more LLM calls without improving results.

## Architecture

| Concept | Implementation |
|---|---|
| **Orchestration** | LangGraph `StateGraph` with sequential and conditional edges |
| **Iteration / retry** | `iteration_count` guard on conditional edges (max 3 corrections) |
| **SQL cleaning** | `_clean_sql()` helper strips markdown fences before execution |
| **MCP server** | `FastMCP` exposes `load_schema`, `validate_sql`, `execute_sql` |
| **MCP client** | `mcp2cli` subprocess calls — no tool schemas injected into LLM context |
| **LLM** | Groq free tier (`llama-3.3-70b-versatile`) or Google Gemini Flash |
| **Validation** | SQLGlot static + semantic validation against live schema |

## Graph topology

```
START
  │
extract_schema  ← mcp2cli: load-schema
  │
generate_sql    ← LLM (Groq / Gemini)
  │
validate_sql    ← mcp2cli: validate-sql
  │
  ├─ valid ──► execute_sql  ← mcp2cli: execute-sql
  │               │
  │               ├─ success ──► END ✓
  │               └─ error   ──► correct_sql ──┐
  │                                            │
  └─ invalid ──────────────► correct_sql ──────┘
                                   │
                              (back to validate_sql, max 3 iterations)
```

## Why mcp2cli instead of a native MCP client?

The standard approach (`MultiServerMCPClient` from `langchain-mcp-adapters`) loads
every tool's full JSON schema into memory and injects them into the LLM context on
every turn — whether the tools are used or not.

[mcp2cli](https://github.com/knowsuchagency/mcp2cli) replaces that with a thin
subprocess call per tool invocation. The LLM never sees the tool schemas; it only
sees the SQL it generated and the error it needs to fix.

| | Native MCP client | mcp2cli |
|---|---|---|
| Tool schemas in LLM context | Every turn (~121 tokens/tool) | Never |
| Schema discovery | Upfront, always | On-demand via `--list` |
| 10-turn conversation (3 tools) | ~3,600 tokens overhead | ~200 tokens overhead |
| Works with any LLM provider | ✗ (varies) | ✓ |

For this 3-tool server the token savings are modest, but the pattern scales well
if you add more tools or connect to additional MCP servers in future.

## Setup

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Initialize database tables

```bash
# Create app_users table and Chainlit chat history tables
python -m texttosql.manage_users init

# Add users
python -m texttosql.manage_users add r.shetty mysecretpass scientist
python -m texttosql.manage_users add admin adminpass admin "Admin User"

# List users
python -m texttosql.manage_users list
```

### 3. Configure environment

Create a `.env` file in the project root:

```bash
# Database
DB_DIALECT=postgresql
DB_URI=postgresql://user:pass@localhost:5432/prelude

# MCP server (default — only change if running the server on a different host/port)
# MCP_SERVER_URL=http://localhost:3001/mcp

# LLM — Option A: Groq (default, free tier) — https://console.groq.com
LLM_PROVIDER=groq
GROQ_API_KEY=gsk_...
GROQ_MODEL=llama-3.3-70b-versatile # mixtral-8x7b-32768

# LLM — Option B: Google Gemini Flash (free tier) — https://aistudio.google.com
# LLM_PROVIDER=gemini
# GOOGLE_API_KEY=AIza...
# GEMINI_MODEL=gemini-1.5-flash
```

### 4. Seed sample data (optional)

To load the anonymised AetherGen biotech dataset (218 rows) into the local
PostgreSQL instance:

```bash
# Docker
cat seed_data.sql | docker exec -i <container> psql -U postgres -d <dbname>

# Native Postgres
psql -h localhost -U postgres -d <dbname> -f seed_data.sql
```

### 5. Run

```bash
# Terminal 1 — start the MCP server (keep running)
python -m texttosql.mcp_server

# Terminal 2 — ask a question
python -m texttosql.main "How many experiments were completed by a.pl?"

# Or launch the interactive REPL
python -m texttosql.main

# Or launch the Chainlit UI (from ui/ directory)
cd ui
chainlit run chainlit_app.py
```

### Visualizations

When using the Chainlit UI, queries that return numeric/categorical data will
automatically generate interactive Plotly charts. The raw data table is also
displayed below each chart for reference.

## File map

```
src/texttosql/
├── config.py       ← env vars + JSON_COLUMN_HINTS for TEXT-stored JSON columns
├── state.py        ← GraphState TypedDict (shared across all nodes)
├── mcp_server.py   ← FastMCP server: load_schema / validate_sql / execute_sql
├── llm_factory.py  ← Groq / Gemini chat model factory
├── nodes.py        ← LangGraph node functions (calls MCP tools via mcp2cli)
├── graph.py        ← StateGraph definition and run_pipeline() entry point
├── main.py         ← CLI / interactive REPL
├── viz.py          ← Plotly chart generation from query results
└── dialects/
    ├── dialect.py  ← Abstract base class for database dialects
    ├── factory.py  ← Returns dialect instance based on DB_DIALECT env var
    ├── engine.py   ← SQLValidator (SQLGlot) + SQLExecutor (psycopg2)
    └── postgres.py ← PostgreSQL dialect: schema introspection, type mapping


ui/
├── chainlit.md
├── chainlit_app.py
├── config.toml
├── manage_users.py   # User management CLI
└── public/          # Static assets
```

## MCP tools

The MCP server (`mcp_server.py`) exposes three tools over **streamable-http** transport
on `http://localhost:3001/mcp`. Called by the agent via `mcp2cli` subprocesses.

| Tool | mcp2cli name | Description |
|---|---|---|
| `load_schema` | `load-schema` | Introspects all public tables; returns DDL + sqlglot schema dict |
| `validate_sql` | `validate-sql` | SQLGlot syntax + semantic validation against live schema |
| `execute_sql` | `execute-sql` | Runs the query; returns column names + rows as JSON |

### JSON columns stored as TEXT

PostgreSQL columns that store JSON as `TEXT` require a cast before using the `->>` operator:

```sql
-- correct
WHERE summary_data::jsonb->>'ISID' = 'r.shetty'

-- wrong — TEXT type has no ->> operator
WHERE summary_data->>'ISID' = 'r.shetty'
```

Add any such columns to `JSON_COLUMN_HINTS` in `config.py` so the LLM always
generates the correct cast. The hints also document the JSON keys and their meaning.

## Adding a new database or table

- **New table, regular columns** — nothing to do. `load_schema` introspects all
  tables in the `public` schema automatically.
- **New table with JSON-as-TEXT columns** — add an entry to `JSON_COLUMN_HINTS`
  in `config.py`.
- **Different database** — update `DB_URI` (and `DB_DIALECT` if switching away
  from PostgreSQL) in `.env`.

## Sample test prompts

- "List all experiment IDs countersigned by 'chemist_010'."
- "Find the total number of experiments created in September 2016."
- "Show the system_name and BOOK number for all experiments where status is '0'."
- "Which scientist has completed the most experiments in the AetherGen system?"
- "Find all experiments that mention 'methanol' or 'ethanol' in the write-up."
- "How many experiments involved a sealed tube or were heated to 100C?"
- "List experiment IDs where the notes mention the product was 'not consistent'."
- "Count experiments per unique system_name where the write-up mentions silica gel chromatography."
- "Show experiments where CREATED_DATE in summary_data differs from analysis_date."
- "Show the average page value in write-ups for the countersigner a.pl"
- "Show me barplot of all scientists and how many unique experiments they completed."

## References

- [mcp2cli](https://github.com/knowsuchagency/mcp2cli) — CLI wrapper for MCP servers and OpenAPI specs
- [LangGraph](https://langchain-ai.github.io/langgraph/) — stateful agent orchestration
- [FastMCP](https://github.com/jlowin/fastmcp) — ergonomic MCP server framework
- [SQLGlot](https://github.com/tobymao/sqlglot) — SQL parser and validator
- Original concept: [text-to-sql-agent](https://github.com/kweinmeister/text-to-sql-agent) by kweinmeister
