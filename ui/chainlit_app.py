"""
Chainlit chat UI for the Text-to-SQL agent.

Start with:
    chainlit run src/texttosql/chainlit_app.py
"""
import chainlit as cl

from texttosql.graph import run_pipeline


@cl.on_chat_start
async def on_chat_start():
    await cl.Message(
        content=(
            "👋 Hi! Ask me anything about the database in plain English.\n\n")
    ).send()


@cl.on_message
async def on_message(message: cl.Message):
    # Show a thinking indicator while the pipeline runs
    async with cl.Step(name="Running Text-to-SQL pipeline") as step:
        state = await run_pipeline(message.content)
        step.output = state.get("final_sql_query") or "No SQL generated"

    final_sql   = state.get("final_sql_query")
    exec_result = state.get("execution_result") or {}
    columns     = state.get("result_columns") or []

    if final_sql and exec_result.get("status") == "success":
        rows      = exec_result.get("rows", [])
        row_count = exec_result.get("row_count", 0)

        # ── SQL block ──────────────────────────────────────────────────────
        sql_block = f"```sql\n{final_sql}\n```"

        # ── Results as markdown table ──────────────────────────────────────
        if columns and rows:
            header    = "| " + " | ".join(str(c) for c in columns) + " |"
            separator = "| " + " | ".join("---" for _ in columns) + " |"
            data_rows = [
                "| " + " | ".join(str(v) if v is not None else "NULL" for v in row) + " |"
                for row in rows[:50]
            ]
            table = "\n".join([header, separator] + data_rows)
            truncation = (
                f"\n\n*Showing 50 of {row_count} rows.*"
                if row_count > 50 else ""
            )
            result_block = f"\n\n**Results — {row_count} row(s)**\n\n{table}{truncation}"
        else:
            result_block = f"\n\n**{row_count} row(s) returned.**"

        await cl.Message(content=sql_block + result_block).send()

    else:
        # ── Error state ────────────────────────────────────────────────────
        error = (
            exec_result.get("error_message")
            or state.get("error")
            or "Unknown error"
        )
        val_errors = (state.get("validation_result") or {}).get("errors", [])
        detail = ""
        if val_errors:
            detail = "\n\n**Validation errors:**\n" + "\n".join(f"- {e}" for e in val_errors)

        last_sql = state.get("sql_query", "N/A")
        await cl.Message(
            content=(
                f"❌ Could not generate a valid SQL query.\n\n"
                f"**Error:** {error}{detail}\n\n"
                f"**Last attempted SQL:**\n```sql\n{last_sql}\n```"
            )
        ).send()
