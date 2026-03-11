from ..config import DB_DIALECT
from .dialect import DatabaseDialect


def get_dialect() -> DatabaseDialect:
    """
    Returns the configured DatabaseDialect based on the DB_DIALECT env var.
    Defaults to 'postgresql' if not set.

    SQLite is imported lazily so the server doesn't crash when sqlite.py
    is absent and DB_DIALECT=postgresql.
    """
    dialect_name = DB_DIALECT.lower()

    if dialect_name == "sqlite":
        from .sqlite import SQLiteDialect  # lazy — only when actually needed
        return SQLiteDialect()

    if dialect_name == "postgresql":
        from .postgres import PostgreSQLDialect
        return PostgreSQLDialect()

    raise ValueError(f"Unsupported DB_DIALECT: '{dialect_name}'. Use 'sqlite' or 'postgresql'.")
