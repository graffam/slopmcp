"""Small markdown rendering helpers for tool text output."""

from __future__ import annotations

from typing import Any, Iterable, Sequence


def fmt(value: Any, fallback: str = "n/a") -> str:
    """Normalize arbitrary values for display."""
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def md_escape(value: Any) -> str:
    """Escape markdown-table control chars."""
    text = fmt(value)
    return text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def md_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """Render a markdown table from rows."""
    rows = list(rows)
    if not rows:
        return "_No data._"

    header_line = "| " + " | ".join(md_escape(h) for h in headers) + " |"
    separator_line = "| " + " | ".join("---" for _ in headers) + " |"
    body_lines = [
        "| " + " | ".join(md_escape(cell) for cell in row) + " |"
        for row in rows
    ]
    return "\n".join([header_line, separator_line, *body_lines])


def md_bullets(items: Iterable[Any], empty_text: str = "_None._") -> str:
    """Render a markdown bullet list."""
    values = [fmt(item) for item in items if fmt(item) != "n/a"]
    if not values:
        return empty_text
    return "\n".join(f"- {value}" for value in values)

