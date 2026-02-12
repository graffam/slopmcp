"""DuckDB connection and query helpers for MIMIC-IV demo database."""

import math
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "mimic_iv_demo.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def query_df(sql: str, params: list | None = None) -> list[dict]:
    """Execute SQL and return list of dicts (JSON-safe)."""
    conn = get_connection()
    try:
        result = conn.execute(sql, params or []).fetchdf()
        # Convert timestamps to ISO strings for JSON serialization
        for col in result.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            result[col] = result[col].astype(str).replace({"NaT": None})
        # Replace NaN/NaT with None for JSON compatibility
        result = result.where(result.notna(), None)
        records = result.to_dict("records")
        # Catch any remaining float nan values
        for row in records:
            for k, v in row.items():
                if isinstance(v, float) and math.isnan(v):
                    row[k] = None
        return records
    finally:
        conn.close()


def query_scalar(sql: str, params: list | None = None):
    """Execute SQL and return single value."""
    conn = get_connection()
    try:
        result = conn.execute(sql, params or []).fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def query_columns(sql: str, params: list | None = None) -> list[str]:
    """Execute SQL and return column names."""
    conn = get_connection()
    try:
        result = conn.execute(sql, params or [])
        return [desc[0] for desc in result.description]
    finally:
        conn.close()


def list_tables() -> dict[str, list[str]]:
    """Return dict of schema -> [table_names]."""
    conn = get_connection()
    try:
        tables = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema LIKE 'mimiciv_%' ORDER BY table_schema, table_name"
        ).fetchall()
        result: dict[str, list[str]] = {}
        for schema, table in tables:
            result.setdefault(schema, []).append(table)
        return result
    finally:
        conn.close()
