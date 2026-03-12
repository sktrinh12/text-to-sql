"""
LangGraph Graph
===============
MCP tools are called via mcp2cli subprocesses — no MultiServerMCPClient,
no tool schema injection, no langchain-mcp-adapters dependency.

Graph topology
--------------

  START
    |
  extract_schema           (mcp2cli: load_schema)
    |
  generate_sql             (LLM)
    |
  validate_sql <--------+  (mcp2cli: validate_sql)
    |                   |
    +- valid -> execute_sql   (mcp2cli: execute_sql)
    |               |
    |               +- success -> END
    |               +- error   -> correct_sql -> (loop)
    |
    +- invalid -> correct_sql -> (loop, max 3)

Usage
-----
# Terminal 1 — start the MCP server
python -m texttosql.mcp_server

# Terminal 2 — run a query
python -m texttosql.main "How many experiments were completed by r.shetty?"
"""
from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import END, StateGraph

from .config import MAX_CORRECTION_ITERATIONS
from .nodes import correct_sql, execute_sql, extract_schema, generate_sql, validate_sql
from .state import GraphState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------

def _route_after_validate(state: GraphState) -> str:
    result = state.get("validation_result") or {}
    if result.get("status") == "success":
        return "execute_sql"
    if state.get("iteration_count", 0) >= MAX_CORRECTION_ITERATIONS:
        logger.warning("Max correction iterations reached after validation failure.")
        return END
    return "correct_sql"


def _route_after_execute(state: GraphState) -> str:
    result = state.get("execution_result") or {}
    if result.get("status") == "success":
        return END
    if state.get("iteration_count", 0) >= MAX_CORRECTION_ITERATIONS:
        logger.warning("Max correction iterations reached after execution failure.")
        return END
    return "correct_sql"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def _build_graph() -> Any:
    builder = StateGraph(GraphState)

    builder.add_node("extract_schema", extract_schema)
    builder.add_node("generate_sql",   generate_sql)
    builder.add_node("validate_sql",   validate_sql)
    builder.add_node("execute_sql",    execute_sql)
    builder.add_node("correct_sql",    correct_sql)

    builder.set_entry_point("extract_schema")
    builder.add_edge("extract_schema", "generate_sql")
    builder.add_edge("generate_sql",   "validate_sql")

    builder.add_conditional_edges(
        "validate_sql",
        _route_after_validate,
        {"execute_sql": "execute_sql", "correct_sql": "correct_sql", END: END},
    )
    builder.add_conditional_edges(
        "execute_sql",
        _route_after_execute,
        {END: END, "correct_sql": "correct_sql"},
    )
    builder.add_edge("correct_sql", "validate_sql")

    return builder.compile()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

# Compile once at import time — no client setup needed
_graph = _build_graph()


async def run_pipeline(user_question: str) -> dict[str, Any]:
    """
    Run the full Text-to-SQL pipeline for a user question.

    The MCP server must already be running:
        python -m texttosql.mcp_server
    """
    initial_state: GraphState = {
        "message": user_question,
        "schema_ddl": "",
        "sqlglot_schema": {},
        "sql_query": None,
        "validation_result": None,
        "execution_result": None,
        "result_columns": None,
        "final_sql_query": None,
        "iteration_count": 0,
        "error": None,
    }

    logger.info("Invoking graph for: %s", user_question)
    return await _graph.ainvoke(initial_state)
