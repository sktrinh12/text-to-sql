"""
LangGraph nodes — each node is a plain async function that reads GraphState
and returns a partial state update dict.

MCP tools are called via mcp2cli subprocess calls instead of
langchain-mcp-adapters. This avoids injecting tool schemas into the LLM
context and removes the MultiServerMCPClient overhead entirely.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from .config import JSON_COLUMN_HINTS, MCP_SERVER_URL
from .llm_factory import get_llm
from .state import GraphState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# mcp2cli helper
# ---------------------------------------------------------------------------


def _mcp2cli(tool_name: str, args: dict[str, str] | None = None) -> dict[str, Any]:
    """
    Call an MCP tool via mcp2cli and return the parsed JSON result.

    Uses --pretty for readable output and --transport streamable to match
    the server transport we configured.

    Raises RuntimeError if mcp2cli is not installed or the call fails.
    """
    if shutil.which("mcp2cli") is None:
        raise RuntimeError("mcp2cli not found. Install it with: pip install mcp2cli")

    cmd = [
        "mcp2cli",
        "--mcp",
        MCP_SERVER_URL,
        "--transport",
        "streamable",
        tool_name.replace("_", "-"),  # load_schema → load-schema
    ]

    stdin_data = None
    if args:
        for key, value in args.items():
            kebab_key = key.replace("_", "-")  # sql_query → sql-query
            # For long/multiline values (e.g. SQL), pass via --stdin as JSON
            # to avoid shell quoting issues
            if "\n" in value or len(value) > 200:
                import json as _json

                stdin_data = _json.dumps({key: value})
                cmd.append("--stdin")
            else:
                cmd.extend([f"--{kebab_key}", value])

    logger.debug("[mcp2cli] Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, input=stdin_data)

    if result.returncode != 0:
        raise RuntimeError(
            f"mcp2cli call to '{tool_name}' failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    raw = result.stdout.strip()
    if not raw:
        raise RuntimeError(f"mcp2cli returned empty output for tool '{tool_name}'")

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"mcp2cli output for '{tool_name}' is not valid JSON: {raw[:200]}"
        ) from exc


async def _mcp2cli_async(
    tool_name: str, args: dict[str, str] | None = None
) -> dict[str, Any]:
    """Async wrapper — runs _mcp2cli in a thread so it doesn't block the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _mcp2cli, tool_name, args)


# ---------------------------------------------------------------------------
# SQL cleaning helper
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# JSON column hints for LLM prompts
# ---------------------------------------------------------------------------


def _build_json_hints_block() -> str:
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
            lines.append(
                f"### Table: {table}  \u2192  Column: {col}  (TEXT containing JSON)"
            )
            lines.append(
                f"Access with:  {col}::jsonb->>'KEY'  (MUST include ::jsonb cast)"
            )
            lines.append("Available keys:")
            for key, description in keys.items():
                lines.append(f"  \u2022 {key:<22} \u2014 {description}")
            lines.append("")

    return "\n".join(lines)


_JSON_HINTS = _build_json_hints_block()


# ---------------------------------------------------------------------------
# Node: extract_schema
# ---------------------------------------------------------------------------


async def extract_schema(state: GraphState) -> dict[str, Any]:
    """Calls the load_schema MCP tool via mcp2cli."""
    logger.info("[extract_schema] Loading schema via mcp2cli.")
    try:
        payload = await _mcp2cli_async("load_schema")
        if payload.get("status") == "error":
            logger.error("[extract_schema] %s", payload.get("error"))
            return {
                "schema_ddl": "",
                "sqlglot_schema": {},
                "error": payload.get("error"),
            }
        return {
            "schema_ddl": payload["ddl"],
            "sqlglot_schema": payload["sqlglot_schema"],
            "error": None,
        }
    except Exception as exc:
        logger.error("[extract_schema] %s", exc)
        return {"schema_ddl": "", "sqlglot_schema": {}, "error": str(exc)}


# ---------------------------------------------------------------------------
# Node: generate_sql
# ---------------------------------------------------------------------------


async def generate_sql(state: GraphState) -> dict[str, Any]:
    """LLM generates the first SQL attempt from the user question."""
    logger.info("[generate_sql] question=%s", state.get("message"))
    llm = get_llm()

    system = SystemMessage(
        content=f"""You are an expert PostgreSQL query writer.
Given the user's question and the database schema, write a single syntactically
correct PostgreSQL SELECT query that fully answers the question.

Rules:
1. Respond ONLY with the SQL — no markdown, no explanation, no preamble.
2. Use ONLY tables and columns that exist in the schema below.
3. For JSONB columns use the ::jsonb cast (see JSON reference below).
4. Add clear column aliases in SELECT so results are readable.
5. Use SELECT DISTINCT when fetching IDs or names to avoid duplicate rows.
6. CRITICAL: When searching for experimental conditions (reactions, procedures, 
   temperatures, equipment like 'sealed tube', 'heated to 100C', etc.), 
   ALWAYS search the WRITE_UP column — NOT EXPERIMENT_NAME. 
   EXPERIMENT_NAME only contains the title, not the procedure details.

## Database Schema (source of truth)
{state["schema_ddl"]}
{_JSON_HINTS}"""
    )

    human = HumanMessage(content=state["message"])
    response = await llm.ainvoke([system, human])
    usage = response.response_metadata.get("token_usage", {})
    logger.info(
        "[generate_sql] tokens — prompt=%s completion=%s",
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
    )
    raw = (
        response.content if isinstance(response.content, str) else str(response.content)
    )
    cleaned = _clean_sql(raw)
    logger.info("[generate_sql] -> %s", cleaned)

    return {
        "sql_query": cleaned,
        "iteration_count": 0,
        "validation_result": None,
        "execution_result": None,
        "result_columns": None,
        "final_sql_query": None,
    }


# ---------------------------------------------------------------------------
# Node: validate_sql
# ---------------------------------------------------------------------------


async def validate_sql(state: GraphState) -> dict[str, Any]:
    """Calls the validate_sql MCP tool via mcp2cli."""
    sql = state.get("sql_query", "")
    logger.info("[validate_sql] %s", sql)
    try:
        result = await _mcp2cli_async("validate_sql", {"sql_query": sql})
    except Exception as exc:
        result = {"status": "error", "errors": [str(exc)]}
    logger.info("[validate_sql] result=%s", result)
    return {"validation_result": result}


# ---------------------------------------------------------------------------
# Node: execute_sql
# ---------------------------------------------------------------------------


async def execute_sql(state: GraphState) -> dict[str, Any]:
    """Calls the execute_sql MCP tool via mcp2cli; marks final_sql_query on success."""
    sql = state.get("sql_query", "")
    logger.info("[execute_sql] %s", sql)
    try:
        result = await _mcp2cli_async("execute_sql", {"sql_query": sql})
    except Exception as exc:
        result = {"status": "error", "error_message": str(exc)}
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

    system = SystemMessage(
        content=f"""You are a PostgreSQL expert correcting a failed SQL query.

Correction priority:
1. Execution error from the database is the most reliable signal — fix it first.
2. Follow the schema strictly; never guess table or column names.
3. For JSONB columns always use ::jsonb cast before ->> or -> operators.
4. Respond ONLY with the corrected SQL — no markdown, no explanation.

## Database Schema (source of truth)
{state["schema_ddl"]}
{_JSON_HINTS}"""
    )

    human = HumanMessage(
        content=(
            f"Original question: {state.get('message')}\n\n"
            f"Faulty SQL:\n{state.get('sql_query')}\n\n"
            f"Validation errors: {json.dumps(state.get('validation_result'))}\n\n"
            f"Execution error:   {json.dumps(state.get('execution_result'))}"
        )
    )

    response = await llm.ainvoke([system, human])
    raw = (
        response.content if isinstance(response.content, str) else str(response.content)
    )
    cleaned = _clean_sql(raw)
    logger.info("[correct_sql] -> %s", cleaned)

    return {
        "sql_query": cleaned,
        "iteration_count": iteration,
        "validation_result": None,
        "execution_result": None,
    }
