"""
Text-to-SQL CLI
===============
Usage:
    python -m texttosql.main "How many orders were placed last month?"

Or run in interactive REPL mode (no argument):
    python -m texttosql.main
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)


async def _run(question: str) -> None:
    from texttosql.graph import run_pipeline  # local import keeps startup fast

    print(f"\nQuestion: {question}\n{'─' * 60}")
    state = await run_pipeline(question)

    final_sql = state.get("final_sql_query")
    exec_result = state.get("execution_result") or {}

    if final_sql and exec_result.get("status") == "success":
        print(f"✅  SQL:\n{final_sql}\n")
        rows = exec_result.get("rows", [])
        columns = state.get("result_columns") or []
        row_count = exec_result.get("row_count", 0)
        display_rows = rows[:50]

        if columns and rows:
            # Calculate column widths
            col_widths = [len(str(c)) for c in columns]
            for row in display_rows:
                for i, val in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

            # Header
            header = "  ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(columns))
            divider = "  ".join("─" * col_widths[i] for i in range(len(columns)))
            print(f"Results ({row_count} rows):")
            print(header)
            print(divider)
            for row in display_rows:
                line = "  ".join(str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(row))
                print(line)
            if row_count > 50:
                print(f"  … ({row_count - 50} more rows not shown)")
        else:
            # Fallback for queries with no column metadata
            print(f"Results ({row_count} rows):")
            for row in display_rows:
                print(" ", row)
    else:
        print("❌  Could not generate a valid SQL query.")
        if state.get("error"):
            print(f"   Error: {state['error']}")
        elif exec_result.get("error_message"):
            print(f"   Execution error: {exec_result['error_message']}")
        val = state.get("validation_result") or {}
        if val.get("errors"):
            print(f"   Validation errors: {val['errors']}")
        print(f"\nLast attempted SQL:\n{state.get('sql_query', 'N/A')}")


def main() -> None:
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        asyncio.run(_run(question))
    else:
        # Interactive REPL
        print("Text-to-SQL interactive mode  (Ctrl-C to exit)\n")
        while True:
            try:
                question = input("Ask a question > ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nBye!")
                break
            if not question:
                continue
            asyncio.run(_run(question))


if __name__ == "__main__":
    main()
