# Text-to-SQL — LangGraph + MCP

## Principal 

| Concept | LangGraph equivalent |
| --- | --- |
| **Orchestration** | `StateGraph` with sequential and conditional edges |
| **Iteration** | `iteration_count` guard on conditional edges (max 3) |
| **Async Logic** | Plain `async def` node functions |
| **Callbacks** | Dedicated Pre/Post nodes inside the graph |
| **SQL Cleaning** | `_clean_sql()` helper called directly inside nodes |
| **Tooling** | **MCP tools** via `FastMCP` + `MultiServerMCPClient` |
| **LLM Inference** | **Groq free tier** (`llama-3.1-8b-instant`) or Gemini Flash |

## Graph topology

```
START
  │
extract_schema  ← MCP: load_schema
  │
generate_sql    ← LLM (Groq / Gemini)
  │
validate_sql    ← MCP: validate_sql
  │
  ├─ valid ──► execute_sql  ← MCP: execute_sql
  │                │
  │                ├─ success ──► END ✓
  │                └─ error   ──► correct_sql ──┐
  │                                             │
  └─ invalid ──────────────► correct_sql ──────┘
                                   │
                              (back to validate_sql, max 3 iterations)

```

## Setup

### 1. Install dependencies

```bash
pip install -e .

```

### 2. Configure environment

Create a `.env` (or export variables):

```bash
# Database
DB_DIALECT=postgresql
DB_URI=postgresql://user:pass@localhost:5432/prelude


# Option A: Groq (default) — https://console.groq.com
LLM_PROVIDER=groq
GROQ_API_KEY=gsk_...
GROQ_MODEL=llama-3.1-8b-instant

# Option B: Google Gemini Flash — https://aistudio.google.com
# LLM_PROVIDER=gemini
# GOOGLE_API_KEY=AIza...
# GEMINI_MODEL=gemini-1.5-flash

```

### 3. Seed Anonymized Data (AetherGen Dataset)

To load the sample biotech testing data (218 rows) into your local PostgreSQL instance:

```bash
# If running via Docker (assuming container name is 'psql' and database name is dbname)
cat aethergen_final_v1.sql | docker exec -i psql psql -U postgres -d dbname

# If running Postgres natively
psql -h localhost -U postgres -d dbname -f aethergen_final_v1.sql

```

### 4. Run

```bash
# Terminal 1 — start the MCP server
python -m texttosql.mcp_server

# Terminal 2 — run a query
python -m texttosql.main "How many experiments were completed by a.pl?"

```

## File map

```
src/texttosql/
├── config.py       ← env vars (updated: free-tier LLMs)
├── state.py        ← GraphState TypedDict
├── mcp_server.py   ← FastMCP server (load_schema / validate_sql / execute_sql)
├── llm_factory.py  ← Groq / Gemini factory
├── nodes.py        ← LangGraph node functions
├── graph.py        ← StateGraph + MultiServerMCPClient bootstrap
├── main.py         ← CLI entry point
├── engine.py       ← SQLValidator + SQLExecutor
└── dialects/
    ├── dialect.py
    ├── factory.py
    ├── sqlite.py
    └── postgres.py

```

## MCP tools

The MCP server (`mcp_server.py`) exposes three tools over **stdio transport**:

| Tool | Description |
| --- | --- |
| `load_schema` | Introspects the DB and returns DDL + sqlglot schema dict |
| `validate_sql` | Runs SQLGlot syntax + semantic validation |
| `execute_sql` | Executes the query and returns rows as JSON |



---

### Testing

- Prompt 1: "List all experiment IDs that were countersigned by 'chemist_010'."
- Prompt 2: "Find the total number of experiments created in September 2016."
- Prompt 3: "Show me the system_name and BOOK number for all experiments where the status is '0'."
- Prompt 4: "Which scientist (ISID) has completed the most experiments in the AetherGen system?"
- Prompt 5: "Search for all experiments that mention using 'methanol' or 'ethanol' in the write-up."
- Prompt 6: "How many experiments involved a 'sealed tube' or were heated to '100 oC'?"
- Prompt 7: "Find all entries that reference a specific J. Org. Chem. publication from 2004."
- Prompt 8: "List the experiment IDs where the notes mention that the product was 'not consistent' or had 'impurities'."
- Prompt 9: "Give me a count of experiments for each unique system_name where the write-up mentions 'silica gel chromatography'."
- Prompt 10: "Identify any experiments where the CREATED_DATE in the summary data is different from the analysis_date column."
- Prompt 11: "Show the average yield percentage mentioned in the write-ups for scientist 'a.pl'."
(Note: This is a stress test—the LLM would have to try to regex a number out of a text block, which might fail or require a creative SQL function).


---

### References

- original idea taken from a google ADK [text-to-sql-agent repo](https://github.com/kweinmeister/text-to-sql-agent/blob/main/src/texttosql/engine.py)
