"""Patient search and lookup tools."""

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent

import db
from tools.markdown import md_table


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="search_patients",
        description=(
            "Search for patients in the MIMIC-IV clinical demo database. "
            "Can search by subject_id (patient ID) or filter by gender and "
            "minimum age. Returns patient demographics including subject_id, "
            "gender, and anchor_age. The demo dataset contains ~100 patients."
        ),
        meta={
            "openai/toolInvocation/invoking": "Searching patients...",
            "openai/toolInvocation/invoked": "Patient search complete (v4)",
        },
    )
    def search_patients(
        subject_id: int | None = None,
        gender: str | None = None,
        min_age: int | None = None,
        max_age: int | None = None,
    ) -> CallToolResult:
        conditions = []
        params = []

        if subject_id is not None:
            conditions.append("p.subject_id = ?")
            params.append(subject_id)
        if gender is not None:
            conditions.append("p.gender = ?")
            params.append(gender.upper())
        if min_age is not None:
            conditions.append("p.anchor_age >= ?")
            params.append(min_age)
        if max_age is not None:
            conditions.append("p.anchor_age <= ?")
            params.append(max_age)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
            SELECT
                p.subject_id,
                p.gender,
                p.anchor_age,
                p.anchor_year,
                p.anchor_year_group,
                p.dod,
                COUNT(DISTINCT a.hadm_id) AS num_admissions
            FROM mimiciv_hosp.patients p
            LEFT JOIN mimiciv_hosp.admissions a
                ON p.subject_id = a.subject_id
            {where}
            GROUP BY p.subject_id, p.gender, p.anchor_age,
                     p.anchor_year, p.anchor_year_group, p.dod
            ORDER BY p.subject_id
            LIMIT 50
        """

        rows = db.query_df(sql, params)
        count = len(rows)
        preview_rows = rows[:10]
        markdown = "\n\n".join(
            [
                "## Patient Search Results",
                f"Found **{count}** patient(s) matching the criteria.",
                md_table(
                    [
                        "Subject ID",
                        "Gender",
                        "Age",
                        "Admissions",
                        "Anchor Year Group",
                        "DOD",
                    ],
                    [
                        [
                            row.get("subject_id"),
                            row.get("gender"),
                            row.get("anchor_age"),
                            row.get("num_admissions"),
                            row.get("anchor_year_group"),
                            row.get("dod"),
                        ]
                        for row in preview_rows
                    ],
                ),
                (
                    f"_Showing first {len(preview_rows)} of {count} result(s)._"
                    if count > len(preview_rows)
                    else "_Showing all results._"
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
            structuredContent={"patients": rows, "count": count},
        )
