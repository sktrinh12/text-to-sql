"""
LangGraph nodes — each node is a plain async function that reads GraphState
and returns a partial state update dict.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import BaseTool

from .config import JSON_COLUMN_HINTS
from .llm_factory import get_llm
from .state import GraphState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_tool(tools: list[BaseTool], name: str) -> BaseTool:
    for t in tools:
        if t.name == name:
            return t
    raise RuntimeError(
        f"MCP tool '{name}' not found. Available: {[t.name for t in tools]}"
    )


def _clean_sql(raw: str) -> str:
    """Strip markdown fences and normalise whitespace."""
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    text = match.group(1) if match else raw
    cleaned = text.strip()
    if cleaned:
        words = cleaned.split()
        if words and words[0].upper() == words[0] and len(words[0]) > 1:
            if not cleaned.endswith(";"):
                cleaned += ";"
    return cleaned


def _parse_tool_result(raw: Any) -> dict[str, Any]:
    """
    Normalise whatever langchain-mcp-adapters hands back from tool.ainvoke():
      - str  → our JSON string from the server, parse it
      - list → [TextContent(...)] from MCP protocol, grab .text of first item
      - dict → already parsed, return as-is
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, list):
        for item in raw:
            text = getattr(item, "text", None)
            if text is None and isinstance(item, dict):
                text = item.get("text")
            if text:
                return json.loads(text)
        raise ValueError(f"No text found in MCP result list: {raw}")
    raise TypeError(f"Unexpected MCP tool result type {type(raw)}: {raw}")


def _build_json_hints_block() -> str:
    """
    Build a human-readable section describing every known JSON/JSONB column
    and its internal keys.  Injected into both generator and corrector prompts.

    Example output
    --------------
    ## JSON / JSONB Column Reference

    ### Table: eln_writeup_api_extract  →  Column: summary_data  (JSONB)
    Access values with:  summary_data->>'KEY'  (returns TEXT)
    Available keys and their meaning:
      • COMPLETED_ISID   — User ID of the person who completed ...
      ...
    """
    if not JSON_COLUMN_HINTS:
        return ""

    lines: list[str] = [
        "",
        "## JSON / JSONB Column Reference",
        "",
        "The columns below store JSON as TEXT. You MUST cast to JSONB before",
        "using the ->> operator: col::jsonb->>'KEY'",
        "",
        "PostgreSQL JSON cheat-sheet (for TEXT columns storing JSON):",
        "  col::jsonb->>'KEY'               -- extract KEY as TEXT",
        "  col::jsonb->'KEY'                -- extract KEY as JSON sub-object",
        "  (col::jsonb->>'NUMBER_KEY')::int -- cast extracted text to integer",
        "  NEVER use col->>'KEY' alone — always cast with ::jsonb first",
        "",
    ]

    for table, cols in JSON_COLUMN_HINTS.items():
        for col, keys in cols.items():
            lines.append(f"### Table: {table}  →  Column: {col}  (TEXT containing JSON)")
            lines.append(f"Access with:  {col}::jsonb->>'KEY'  (MUST include ::jsonb cast)")
            lines.append("Available keys:")
            for key, description in keys.items():
                lines.append(f"  • {key:<22} — {description}")
            lines.append("")

    return "\n".join(lines)


_JSON_HINTS = _build_json_hints_block()   # built once at import time


# ---------------------------------------------------------------------------
# Node: extract_schema
# ---------------------------------------------------------------------------

async def extract_schema(state: GraphState, mcp_tools: list[BaseTool]) -> dict[str, Any]:
    """Calls the load_schema MCP tool and stores DDL + sqlglot schema in state."""
    logger.info("[extract_schema] Loading schema via MCP.")
    tool = _find_tool(mcp_tools, "load_schema")
    raw = await tool.ainvoke({})
    payload: dict[str, Any] = _parse_tool_result(raw)

    if payload.get("status") == "error":
        logger.error("[extract_schema] %s", payload.get("error"))
        return {"schema_ddl": "", "sqlglot_schema": {}, "error": payload.get("error")}

    return {
        "schema_ddl": payload["ddl"],
        "sqlglot_schema": payload["sqlglot_schema"],
        "error": None,
    }


# ---------------------------------------------------------------------------
# Node: generate_sql
# ---------------------------------------------------------------------------

async def generate_sql(state: GraphState) -> dict[str, Any]:
    """LLM generates the first SQL attempt from the user question."""
    logger.info("[generate_sql] question=%s", state.get("message"))
    llm = get_llm()

    system = SystemMessage(content=f"""You are an expert PostgreSQL query writer.
Given the user's question and the database schema, write a single syntactically
correct PostgreSQL SELECT query that fully answers the question.

Rules:
1. Respond ONLY with the SQL — no markdown, no explanation, no preamble.
2. Use ONLY tables and columns that exist in the schema below.
3. For JSONB columns use the ->> operator (see JSON reference below).
4. Add clear column aliases in SELECT so results are readable.

## Database Schema (source of truth)
{state['schema_ddl']}
{_JSON_HINTS}""")

    human = HumanMessage(content=state["message"])
    response = await llm.ainvoke([system, human])
    raw = response.content if isinstance(response.content, str) else str(response.content)
    cleaned = _clean_sql(raw)
    logger.info("[generate_sql] -> %s", cleaned)

    return {
        "sql_query": cleaned,
        "iteration_count": 0,
        "validation_result": None,
        "execution_result": None,
        "final_sql_query": None,
    }


# ---------------------------------------------------------------------------
# Node: validate_sql
# ---------------------------------------------------------------------------

async def validate_sql(state: GraphState, mcp_tools: list[BaseTool]) -> dict[str, Any]:
    """Calls the validate_sql MCP tool."""
    sql = state.get("sql_query", "")
    logger.info("[validate_sql] %s", sql)
    tool = _find_tool(mcp_tools, "validate_sql")
    raw = await tool.ainvoke({"sql_query": sql})
    result: dict[str, Any] = _parse_tool_result(raw)
    logger.info("[validate_sql] result=%s", result)
    return {"validation_result": result}


# ---------------------------------------------------------------------------
# Node: execute_sql
# ---------------------------------------------------------------------------

async def execute_sql(state: GraphState, mcp_tools: list[BaseTool]) -> dict[str, Any]:
    """Calls the execute_sql MCP tool; marks final_sql_query on success."""
    sql = state.get("sql_query", "")
    logger.info("[execute_sql] %s", sql)
    tool = _find_tool(mcp_tools, "execute_sql")
    raw = await tool.ainvoke({"sql_query": sql})
    result: dict[str, Any] = _parse_tool_result(raw)
    logger.info("[execute_sql] status=%s", result.get("status"))

    update: dict[str, Any] = {"execution_result": result}
    if result.get("status") == "success":
        update["final_sql_query"] = sql
        update["result_columns"] = result.get("columns", [])
    return update


# ---------------------------------------------------------------------------
# Node: correct_sql
# ---------------------------------------------------------------------------

async def correct_sql(state: GraphState) -> dict[str, Any]:
    """LLM corrects a failing SQL query using the error feedback."""
    iteration = state.get("iteration_count", 0) + 1
    logger.info("[correct_sql] attempt=%d", iteration)
    llm = get_llm()

    system = SystemMessage(content=f"""You are a PostgreSQL expert correcting a failed SQL query.

Correction priority:
1. Execution error from the database is the most reliable signal — fix it first.
2. Follow the schema strictly; never guess table or column names.
3. For JSONB columns always use ->> (extract as TEXT) or -> (extract as JSON).
4. Respond ONLY with the corrected SQL — no markdown, no explanation.

## Database Schema (source of truth)
{state['schema_ddl']}
{_JSON_HINTS}""")

    human = HumanMessage(content=(
        f"Original question: {state.get('message')}\n\n"
        f"Faulty SQL:\n{state.get('sql_query')}\n\n"
        f"Validation errors: {json.dumps(state.get('validation_result'))}\n\n"
        f"Execution error:   {json.dumps(state.get('execution_result'))}"
    ))

    response = await llm.ainvoke([system, human])
    raw = response.content if isinstance(response.content, str) else str(response.content)
    cleaned = _clean_sql(raw)
    logger.info("[correct_sql] -> %s", cleaned)

    return {
        "sql_query": cleaned,
        "iteration_count": iteration,
        "validation_result": None,
        "execution_result": None,
    }
