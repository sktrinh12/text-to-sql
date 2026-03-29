"""
User management CLI for the Chainlit app.

Usage
-----
# Create tables (app_users + Chainlit chat history tables)
python -m texttosql.manage_users init

# Add a user
python -m texttosql.manage_users add r.shetty mysecretpass scientist
python -m texttosql.manage_users add admin adminpass admin "Admin User"

# List all users
python -m texttosql.manage_users list

# Change a password
python -m texttosql.manage_users passwd r.shetty newpassword

# Delete a user
python -m texttosql.manage_users delete r.shetty
"""

from __future__ import annotations

import argparse
import sys

import psycopg2
from dotenv import load_dotenv
import bcrypt

load_dotenv()

from texttosql.config import DB_URI  # noqa: E402 — after load_dotenv


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


DDL_APP_USERS = """
CREATE TABLE IF NOT EXISTS app_users (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'scientist',
    display_name  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

DDL_CHAINLIT = """
-- Chainlit SQLAlchemyDataLayer tables for chat history persistence

CREATE TABLE IF NOT EXISTS users (
    "id" UUID PRIMARY KEY,
    "identifier" TEXT NOT NULL UNIQUE,
    "metadata" JSONB NOT NULL,
    "createdAt" TEXT
);

CREATE TABLE IF NOT EXISTS threads (
    "id" UUID PRIMARY KEY,
    "createdAt" TEXT,
    "name" TEXT,
    "userId" UUID,
    "userIdentifier" TEXT,
    "tags" TEXT[],
    "metadata" JSONB,
    FOREIGN KEY ("userId") REFERENCES users("id") ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS steps (
    "id" UUID PRIMARY KEY,
    "name" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "threadId" UUID NOT NULL,
    "parentId" UUID,
    "streaming" BOOLEAN NOT NULL,
    "waitForAnswer" BOOLEAN,
    "isError" BOOLEAN,
    "metadata" JSONB,
    "tags" TEXT[],
    "input" TEXT,
    "output" TEXT,
    "createdAt" TEXT,
    "command" TEXT,
    "start" TEXT,
    "end" TEXT,
    "generation" JSONB,
    "showInput" TEXT,
    "language" TEXT,
    "indent" INT,
    "defaultOpen" BOOLEAN,
    FOREIGN KEY ("threadId") REFERENCES threads("id") ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS elements (
    "id" UUID PRIMARY KEY,
    "threadId" UUID,
    "type" TEXT,
    "url" TEXT,
    "chainlitKey" TEXT,
    "name" TEXT NOT NULL,
    "display" TEXT,
    "objectKey" TEXT,
    "size" TEXT,
    "page" INT,
    "language" TEXT,
    "forId" UUID,
    "mime" TEXT,
    "props" JSONB,
    FOREIGN KEY ("threadId") REFERENCES threads("id") ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS feedbacks (
    "id" UUID PRIMARY KEY,
    "forId" UUID NOT NULL,
    "threadId" UUID NOT NULL,
    "value" INT NOT NULL,
    "comment" TEXT,
    FOREIGN KEY ("threadId") REFERENCES threads("id") ON DELETE CASCADE
);
"""


def _conn():
    return psycopg2.connect(DB_URI)


def cmd_init(_args) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_APP_USERS)
        with conn.cursor() as cur:
            cur.execute(DDL_CHAINLIT)
    print("✅  app_users and Chainlit tables ready.")


def cmd_add(args) -> None:
    display = args.display_name or args.username
    hashed = _hash_password(args.password)
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO app_users (username, password_hash, role, display_name) "
                    "VALUES (%s, %s, %s, %s)",
                    (args.username, hashed, args.role, display),
                )
        print(f"✅  Added user '{args.username}' (role={args.role}).")
    except psycopg2.errors.UniqueViolation:
        print(
            f"❌  User '{args.username}' already exists. Use passwd to change password."
        )
        sys.exit(1)


def cmd_list(_args) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT username, role, display_name, created_at "
                "FROM app_users ORDER BY created_at"
            )
            rows = cur.fetchall()
    if not rows:
        print("No users found. Run `init` then `add`.")
        return
    print(f"\n{'USERNAME':<20} {'ROLE':<12} {'DISPLAY NAME':<25} CREATED")
    print("─" * 75)
    for username, role, display_name, created_at in rows:
        print(
            f"{username:<20} {role:<12} {(display_name or ''):<25} {created_at:%Y-%m-%d}"
        )
    print()


def cmd_passwd(args) -> None:
    hashed = _hash_password(args.password)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app_users SET password_hash = %s WHERE username = %s",
                (hashed, args.username),
            )
            if cur.rowcount == 0:
                print(f"❌  User '{args.username}' not found.")
                sys.exit(1)
    print(f"✅  Password updated for '{args.username}'.")


def cmd_delete(args) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_users WHERE username = %s", (args.username,))
            if cur.rowcount == 0:
                print(f"❌  User '{args.username}' not found.")
                sys.exit(1)
    print(f"✅  Deleted user '{args.username}'.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage Chainlit app users stored in PostgreSQL."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Create the app_users table (safe to re-run)")

    p_add = sub.add_parser("add", help="Add a new user")
    p_add.add_argument("username")
    p_add.add_argument("password")
    p_add.add_argument(
        "role", choices=["scientist", "admin"], default="scientist", nargs="?"
    )
    p_add.add_argument("display_name", nargs="?", default=None)

    sub.add_parser("list", help="List all users")

    p_pw = sub.add_parser("passwd", help="Change a user's password")
    p_pw.add_argument("username")
    p_pw.add_argument("password")

    p_del = sub.add_parser("delete", help="Delete a user")
    p_del.add_argument("username")

    args = parser.parse_args()
    {
        "init": cmd_init,
        "add": cmd_add,
        "list": cmd_list,
        "passwd": cmd_passwd,
        "delete": cmd_delete,
    }[args.command](args)


if __name__ == "__main__":
    main()
