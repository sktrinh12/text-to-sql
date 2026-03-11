"""
LangGraph Graph
===============
Replaces the ADK SequentialAgent + LoopAgent pipeline with a LangGraph
StateGraph.  The MCP server must be started separately before running the agent.

Graph topology
--------------

  START
    │
  extract_schema           (MCP: load_schema)
    │
  generate_sql             (LLM)
    │
  validate_sql ◄────────┐  (MCP: validate_sql)
    │                   │
    ├─ success ─► execute_sql   (MCP: execute_sql)
    │               │
    │               ├─ success ─► END  ✓
    │               └─ error   ─► correct_sql ──► (loop)
    │
    └─ error ────────────► correct_sql ──────────► (loop)
                              │
                          (iteration >= MAX → END with error)

Usage
-----
# Terminal 1 — start the MCP server
python -m texttosql.mcp_server

# Terminal 2 — run a query
python -m texttosql.main "How many experiments were completed by r.shetty?"
"""
from __future__ import annotations

import functools
import logging
import os
from typing import Any

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.graph import END, StateGraph

from .config import MAX_CORRECTION_ITERATIONS
from .nodes import correct_sql, execute_sql, extract_schema, generate_sql, validate_sql
from .state import GraphState

logger = logging.getLogger(__name__)

# MCP server URL — override with MCP_SERVER_URL env var if running on a different host/port
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:3001/mcp")


# ---------------------------------------------------------------------------
# Routing functions (conditional edges)
# ---------------------------------------------------------------------------

def _route_after_validate(state: GraphState) -> str:
    """After validate_sql: go to execute if valid, else correct or give up."""
    result = state.get("validation_result") or {}
    if result.get("status") == "success":
        return "execute_sql"
    if (state.get("iteration_count", 0) >= MAX_CORRECTION_ITERATIONS):
        logger.warning("Max correction iterations reached after validation failure.")
        return END
    return "correct_sql"


def _route_after_execute(state: GraphState) -> str:
    """After execute_sql: done on success, try to correct on failure."""
    result = state.get("execution_result") or {}
    if result.get("status") == "success":
        return END
    if (state.get("iteration_count", 0) >= MAX_CORRECTION_ITERATIONS):
        logger.warning("Max correction iterations reached after execution failure.")
        return END
    return "correct_sql"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def _build_graph(mcp_tools: list) -> Any:
    """
    Construct and compile the StateGraph, binding MCP tools to the nodes
    that require them via functools.partial.
    """
    _extract_schema = functools.partial(extract_schema, mcp_tools=mcp_tools)
    _validate_sql   = functools.partial(validate_sql,   mcp_tools=mcp_tools)
    _execute_sql    = functools.partial(execute_sql,    mcp_tools=mcp_tools)

    builder = StateGraph(GraphState)

    # ── Nodes ─────────────────────────────────────────────────────────────
    builder.add_node("extract_schema", _extract_schema)
    builder.add_node("generate_sql",   generate_sql)
    builder.add_node("validate_sql",   _validate_sql)
    builder.add_node("execute_sql",    _execute_sql)
    builder.add_node("correct_sql",    correct_sql)

    # ── Edges ─────────────────────────────────────────────────────────────
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

async def run_pipeline(user_question: str) -> dict[str, Any]:
    """
    Run the full Text-to-SQL pipeline for a user question.

    Connects to the already-running MCP server via HTTP, builds + executes
    the LangGraph, and returns the final state.

    The MCP server must be started first:
        python -m texttosql.mcp_server
    """
    mcp_server_config = {
        "texttosql": {
            "url": MCP_SERVER_URL,
            "transport": "streamable_http",
        }
    }

    logger.info("Connecting to MCP server at %s …", MCP_SERVER_URL)
    client = MultiServerMCPClient(mcp_server_config)
    mcp_tools = await client.get_tools()
    logger.info("MCP tools loaded: %s", [t.name for t in mcp_tools])

    graph = _build_graph(mcp_tools)

    initial_state: GraphState = {
        "message": user_question,
        "schema_ddl": "",
        "sqlglot_schema": {},
        "sql_query": None,
        "validation_result": None,
        "execution_result": None,
        "final_sql_query": None,
        "iteration_count": 0,
        "error": None,
    }

    logger.info("Invoking graph …")
    final_state: GraphState = await graph.ainvoke(initial_state)
    return final_state
