"""
Chainlit UI for the Text-to-SQL agent.

Start:
    chainlit run src/texttosql/chainlit_app.py

Requirements:
    pip install chainlit passlib bcrypt asyncpg sqlalchemy

Environment variables (add to .env):
    DB_URI=postgresql://postgres:postgres@localhost:5432/prelude

The PostgreSQL DB uses the asyncpg driver (required by SQLAlchemyDataLayer).

The DB_URI uses the standard psycopg2 driver (used by the SQL pipeline).
Both can point at the same database.

User management:
    python -m texttosql.manage_users add r.shetty mysecretpass scientist
    python -m texttosql.manage_users add admin adminpass admin
    python -m texttosql.manage_users list
    python -m texttosql.manage_users delete r.shetty
"""

from __future__ import annotations

import logging
import os

import chainlit as cl
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from chainlit.types import ThreadDict
from dotenv import load_dotenv
from manage_users import _verify_password

from texttosql.graph import run_pipeline
from texttosql.viz import wants_visualization, create_chart

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


@cl.password_auth_callback
def auth_callback(username: str, password: str) -> cl.User | None:
    """
    Verify credentials against the app_users table in PostgreSQL.

    Schema (created by manage_users.py):
        CREATE TABLE app_users (
            username      TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'scientist',
            display_name  TEXT
        );
    """
    import psycopg2
    from texttosql.config import DB_URI

    try:
        with psycopg2.connect(DB_URI) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT password_hash, role, display_name "
                    "FROM app_users WHERE username = %s",
                    (username,),
                )
                row = cur.fetchone()
    except Exception as exc:
        logger.error("auth_callback DB error: %s", exc)
        return None

    if row is None:
        return None

    password_hash, role, display_name = row
    if not _verify_password(password, password_hash):
        return None

    return cl.User(
        identifier=username,
        metadata={"role": role, "display_name": display_name or username},
    )


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------


@cl.data_layer
def get_data_layer() -> SQLAlchemyDataLayer | None:
    conninfo = os.getenv("DB_URI")
    if not conninfo:
        logger.warning("DB_URI not set — chat history will not be persisted.")
        return None
    if conninfo.startswith("postgresql://"):
        conninfo = conninfo.replace("postgresql://", "postgresql+asyncpg://", 1)
    return SQLAlchemyDataLayer(conninfo=conninfo, storage_provider=None)


# ---------------------------------------------------------------------------
# Chat lifecycle
# ---------------------------------------------------------------------------


@cl.on_chat_start
async def on_chat_start() -> None:
    user = cl.user_session.get("user")
    name = user.metadata.get("display_name", user.identifier) if user else "there"
    await cl.Message(
        content=(
            f"👋 Hi **{name}**! Ask me anything about the ELN database in plain English.\n"
        )
    ).send()


@cl.on_chat_resume
async def on_chat_resume(thread: dict) -> None:
    user = cl.user_session.get("user")
    thread_id = thread.get("id")
    logger.info(
        "Resuming thread %s for user %s (metadata=%s)",
        thread_id,
        user.identifier if user else "unknown",
        thread.get("metadata"),
    )
    cl.user_session.set("thread_id", thread_id)
    name = user.metadata.get("display_name", user.identifier) if user else "there"
    await cl.Message(
        content=(f"👋 Welcome back **{name}**! Continuing our conversation.\n")
    ).send()


@cl.on_stop
async def on_stop() -> None:
    logger.info("User stopped the current task.")


@cl.on_chat_end
async def on_chat_end() -> None:
    user = cl.user_session.get("user")
    logger.info("User %s disconnected.", user.identifier if user else "unknown")


# ---------------------------------------------------------------------------
# Message handler
# ---------------------------------------------------------------------------


def _format_value(v: Any, decimals: int = 2) -> str:
    """Format a value for display, rounding floats to specified decimal places."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return str(round(v, decimals))
    return str(v)


@cl.on_message
async def on_message(message: cl.Message) -> None:
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="Please log in to continue the conversation.").send()
        return
    thread_id = cl.user_session.get("thread_id") or (
        message.thread_id if hasattr(message, "thread_id") else None
    )
    logger.info(
        "on_message: user=%s, thread=%s", user.identifier if user else None, thread_id
    )
    try:
        async with cl.Step(name="Text-to-SQL pipeline") as step:
            state = await run_pipeline(message.content)
            step.output = state.get("final_sql_query") or "No SQL generated"
    except Exception as exc:
        logger.exception("Error processing message: %s", exc)
        await cl.Message(content=f"Error: {exc}").send()
        return

    final_sql = state.get("final_sql_query")
    exec_result = state.get("execution_result") or {}
    columns = state.get("result_columns") or []

    if final_sql and exec_result.get("status") == "success":
        rows = exec_result.get("rows", [])
        row_count = exec_result.get("row_count", 0)

        sql_block = f"```sql\n{final_sql}\n```"

        if wants_visualization(message.content) and rows and columns:
            try:
                fig = create_chart(columns, rows)
                elements = [cl.Plotly(name="chart", figure=fig, display="inline")]

                header = "| " + " | ".join(str(c) for c in columns) + " |"
                separator = "| " + " | ".join("---" for _ in columns) + " |"
                data_rows = [
                    "| " + " | ".join(_format_value(v) for v in row) + " |"
                    for row in rows[:50]
                ]
                table = "\n".join([header, separator] + data_rows)
                truncation = (
                    f"\n\n*Showing 50 of {row_count} rows.*" if row_count > 50 else ""
                )
                raw_data = f"**Raw data ({row_count} rows)**\n{table}{truncation}"

                await cl.Message(
                    content=sql_block + "\n\n" + raw_data, elements=elements
                ).send()
                return
            except Exception as exc:
                logger.warning("Chart generation failed: %s", exc)

        if columns and rows:
            header = "| " + " | ".join(str(c) for c in columns) + " |"
            separator = "| " + " | ".join("---" for _ in columns) + " |"
            data_rows = [
                "| " + " | ".join(_format_value(v) for v in row) + " |"
                for row in rows[:50]
            ]
            table = "\n".join([header, separator] + data_rows)
            truncation = (
                f"\n\n*Showing 50 of {row_count} rows.*" if row_count > 50 else ""
            )
            result_block = (
                f"\n\n**Results — {row_count} row(s)**\n\n{table}{truncation}"
            )
        else:
            result_block = f"\n\n**{row_count} row(s) returned.**"

        await cl.Message(content=sql_block + result_block).send()

    else:
        error = (
            exec_result.get("error_message") or state.get("error") or "Unknown error"
        )
        val_errors = (state.get("validation_result") or {}).get("errors", [])
        detail = (
            "\n\n**Validation errors:**\n" + "\n".join(f"- {e}" for e in val_errors)
            if val_errors
            else ""
        )
        await cl.Message(
            content=(
                f"❌ Could not generate a valid SQL query.\n\n"
                f"**Error:** {error}{detail}\n\n"
                f"**Last attempted SQL:**\n```sql\n{state.get('sql_query', 'N/A')}\n```"
            )
        ).send()
