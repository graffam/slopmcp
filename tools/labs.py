"""Lab results tools."""

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent

import db
from tools.markdown import md_table


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_lab_results",
        description=(
            "Retrieve laboratory test results for a specific patient in the "
            "MIMIC-IV database. Returns test name, value, units, abnormal "
            "flags (HIGH/LOW), and timestamps. Can optionally filter by lab "
            "test category (e.g. 'Chemistry', 'Hematology', 'Blood Gas'). "
            "Requires a subject_id (patient ID)."
        ),
        meta={
            "openai/toolInvocation/invoking": "Querying lab results...",
            "openai/toolInvocation/invoked": "Lab results ready (v4)",
        },
    )
    def get_lab_results(
        subject_id: int,
        category: str | None = None,
        item_label: str | None = None,
        hadm_id: int | None = None,
        limit: int = 200,
    ) -> CallToolResult:
        conditions = ["le.subject_id = ?"]
        params: list = [subject_id]

        if category:
            conditions.append("di.category ILIKE ?")
            params.append(f"%{category}%")
        if item_label:
            conditions.append("di.label ILIKE ?")
            params.append(f"%{item_label}%")
        if hadm_id is not None:
            conditions.append("le.hadm_id = ?")
            params.append(hadm_id)

        where = " AND ".join(conditions)
        safe_limit = max(1, min(limit, 300))
        params.append(safe_limit)

        sql = f"""
            SELECT
                le.labevent_id,
                le.itemid,
                di.label AS test_name,
                di.category,
                le.charttime,
                le.value,
                le.valuenum,
                le.valueuom AS unit,
                le.ref_range_lower,
                le.ref_range_upper,
                le.flag
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems di ON le.itemid = di.itemid
            WHERE {where}
            ORDER BY le.charttime DESC
            LIMIT ?
        """

        rows = db.query_df(sql, params)
        count = len(rows)

        # Also get distinct categories available for this patient
        categories = db.query_df(
            """
            SELECT DISTINCT di.category, COUNT(*) AS count
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems di ON le.itemid = di.itemid
            WHERE le.subject_id = ?
            GROUP BY di.category
            ORDER BY count DESC
            """,
            [subject_id],
        )

        summary = f"Found {count} lab result(s) for patient {subject_id}"
        if category:
            summary += f" in category '{category}'"
        summary += "."

        preview_rows = rows[:12]
        result_table = md_table(
            ["Chart Time", "Test", "Category", "Value", "Flag"],
            [
                [
                    row.get("charttime"),
                    row.get("test_name"),
                    row.get("category"),
                    (
                        f"{row.get('valuenum') if row.get('valuenum') is not None else row.get('value')} "
                        f"{row.get('unit') or ''}"
                    ).strip(),
                    row.get("flag") or "normal",
                ]
                for row in preview_rows
            ],
        )
        category_table = md_table(
            ["Category", "Count"],
            [
                [row.get("category"), row.get("count")]
                for row in categories[:10]
            ],
        )
        markdown = "\n\n".join(
            [
                "## Lab Results",
                summary,
                "### Latest Measurements",
                result_table,
                (
                    f"_Showing first {len(preview_rows)} of {count} result(s)._"
                    if count > len(preview_rows)
                    else "_Showing all results._"
                ),
                "### Available Categories",
                category_table,
            ]
        )

        return CallToolResult(
            content=[TextContent(type="text", text=markdown)],
            structuredContent={
                "labs": rows,
                "count": count,
                "categories": categories,
                "subject_id": subject_id,
            },
        )

    @mcp.tool(
        name="get_lab_trend",
        description=(
            "Get the time series of a specific lab test for a patient. "
            "Useful for tracking how a lab value changes over time across "
            "admissions. Requires subject_id and either itemid (numeric) "
            "or item_label (text search like 'Creatinine')."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading lab trend...",
            "openai/toolInvocation/invoked": "Lab trend ready (v4)",
        },
    )
    def get_lab_trend(
        subject_id: int,
        itemid: int | None = None,
        item_label: str | None = None,
    ) -> CallToolResult:
        if itemid is None and item_label is None:
            return CallToolResult(
                isError=True,
                content=[
                    TextContent(
                        type="text",
                        text="Must provide either itemid or item_label.",
                    )
                ],
            )

        if itemid is not None:
            condition = "le.itemid = ?"
            params: list = [subject_id, itemid]
        else:
            condition = "di.label ILIKE ?"
            params = [subject_id, f"%{item_label}%"]

        sql = f"""
            SELECT
                le.itemid,
                di.label AS test_name,
                le.charttime,
                le.valuenum,
                le.valueuom AS unit,
                le.ref_range_lower,
                le.ref_range_upper,
                le.flag
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems di ON le.itemid = di.itemid
            WHERE le.subject_id = ? AND {condition}
                AND le.valuenum IS NOT NULL
            ORDER BY le.charttime ASC
        """

        rows = db.query_df(sql, params)

        test_name = rows[0]["test_name"] if rows else (item_label or str(itemid))
        preview_rows = rows[-20:]
        trend_table = md_table(
            ["Chart Time", "Value", "Unit", "Ref Range", "Flag"],
            [
                [
                    row.get("charttime"),
                    row.get("valuenum"),
                    row.get("unit"),
                    (
                        f"{row.get('ref_range_lower')} - {row.get('ref_range_upper')}"
                        if row.get("ref_range_lower") is not None
                        and row.get("ref_range_upper") is not None
                        else "n/a"
                    ),
                    row.get("flag") or "normal",
                ]
                for row in preview_rows
            ],
        )
        markdown = "\n\n".join(
            [
                f"## Lab Trend: {test_name}",
                f"Found **{len(rows)}** measurement(s) for patient **{subject_id}**.",
                trend_table,
                (
                    f"_Showing most recent {len(preview_rows)} of {len(rows)} measurement(s)._"
                    if len(rows) > len(preview_rows)
                    else "_Showing all measurements._"
                ),
            ]
        )
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=markdown,
                )
            ],
            structuredContent={
                "trend": rows,
                "test_name": test_name,
                "subject_id": subject_id,
            },
        )
