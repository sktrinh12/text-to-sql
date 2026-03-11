"""
MCP Server — Text-to-SQL tools
================================
Start in one terminal:   python -m texttosql.mcp_server
Run the agent in another: python -m texttosql.main "How many experiments?"
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from texttosql.config import DB_URI
from texttosql.dialects.factory import get_dialect
from texttosql.dialects.engine import SQLExecutor, SQLValidator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(levelname)s [mcp_server] %(message)s",
)
logger = logging.getLogger(__name__)

MCP_HOST = os.environ.get("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.environ.get("MCP_SERVER_PORT", "3001"))


def _json_serial(obj: Any) -> str:
    """Fallback serializer for types json.dumps can't handle natively."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# ---------------------------------------------------------------------------
# FastMCP app
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="texttosql-tools",
    instructions="SQL schema introspection, validation and execution for PostgreSQL.",
)

# ---------------------------------------------------------------------------
# Tool: load_schema
# ---------------------------------------------------------------------------

@mcp.tool()
def load_schema() -> str:
    """
    Introspect the configured database and return its schema.

    Returns a JSON object:
      - ddl:            CREATE TABLE DDL shown to the LLM
      - sqlglot_schema: {table: {column: type}} used for validation
    """
    try:
        dialect = get_dialect()
        logger.info("load_schema: dialect=%s uri=%s", dialect.name, DB_URI)
        ddl = dialect.get_ddl(DB_URI)
        sqlglot_schema: dict[str, Any] = dialect.get_sqlglot_schema(DB_URI)
        return json.dumps({"status": "success", "ddl": ddl, "sqlglot_schema": sqlglot_schema})
    except Exception as exc:
        logger.error("load_schema failed: %s", exc, exc_info=True)
        return json.dumps({"status": "error", "error": str(exc)})


# ---------------------------------------------------------------------------
# Tool: validate_sql
# ---------------------------------------------------------------------------

@mcp.tool()
def validate_sql(sql_query: str) -> str:
    """
    Validate a SQL query for syntax and semantic correctness.

    Args:
        sql_query: The SQL statement to validate.

    Returns {"status": "success"} or {"status": "error", "errors": [...]}
    """
    try:
        dialect = get_dialect()
        sqlglot_schema = dialect.get_sqlglot_schema(DB_URI)
        validator = SQLValidator()
        result = validator.validate(sql_query, dialect, sqlglot_schema)
        return json.dumps(result)
    except Exception as exc:
        logger.error("validate_sql failed: %s", exc, exc_info=True)
        return json.dumps({"status": "error", "errors": [str(exc)]})


# ---------------------------------------------------------------------------
# Tool: execute_sql
# ---------------------------------------------------------------------------

@mcp.tool()
def execute_sql(sql_query: str) -> str:
    """
    Execute a SQL query against the configured database.

    Args:
        sql_query: A valid SQL statement.

    Returns {"status": "success", "columns": [...], "rows": [...], "row_count": N}
         or {"status": "error", "error_message": "..."}
    """
    try:
        dialect = get_dialect()
        with dialect.get_connection(DB_URI) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            serialisable = [list(row) for row in rows]
            return json.dumps({
                "status": "success",
                "columns": columns,
                "rows": serialisable,
                "row_count": len(serialisable),
            }, default=_json_serial)
    except Exception as exc:
        logger.error("execute_sql failed: %s", exc, exc_info=True)
        return json.dumps({"status": "error", "error_message": str(exc)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("TextToSQL MCP server → http://%s:%s/mcp", MCP_HOST, MCP_PORT)
    mcp.run(transport="streamable-http", host=MCP_HOST, port=MCP_PORT)
