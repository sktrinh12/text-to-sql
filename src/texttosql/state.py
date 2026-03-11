"""
LangGraph state definition for the Text-to-SQL workflow.
"""
from typing import Any, Optional
from typing_extensions import TypedDict


class GraphState(TypedDict):
    """Shared state passed between all LangGraph nodes."""

    # Input
    message: str

    # Schema (populated by extract_schema node)
    schema_ddl: str
    sqlglot_schema: dict[str, Any]

    # SQL lifecycle
    sql_query: Optional[str]
    validation_result: Optional[dict[str, Any]]
    execution_result: Optional[dict[str, Any]]
    result_columns: Optional[list[str]]   # column names from last successful execution
    final_sql_query: Optional[str]

    # Loop control (mirrors LoopAgent max_iterations=3)
    iteration_count: int

    # Top-level error propagation
    error: Optional[str]
