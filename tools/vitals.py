"""ICU vital signs tools."""

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent

import db
from tools.markdown import md_table

# Common MIMIC-IV itemids for vital signs in chartevents
VITAL_ITEMIDS = {
    "Heart Rate": [220045],
    "Systolic BP": [220050, 220179],
    "Diastolic BP": [220051, 220180],
    "Mean BP": [220052, 220181],
    "Respiratory Rate": [220210, 224690],
    "SpO2": [220277],
    "Temperature (F)": [223761],
    "Temperature (C)": [223762],
}

ALL_VITAL_IDS = [vid for ids in VITAL_ITEMIDS.values() for vid in ids]


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_vitals",
        description=(
            "Get ICU vital signs for a patient in the MIMIC-IV database. "
            "Returns time-series data for heart rate, blood pressure, SpO2, "
            "respiratory rate, and temperature. Can filter by a specific "
            "ICU stay_id. Requires subject_id (patient ID)."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading vital signs...",
            "openai/toolInvocation/invoked": "Vital signs ready (v4)",
        },
    )
    def get_vitals(
        subject_id: int,
        stay_id: int | None = None,
    ) -> CallToolResult:
        # First, find ICU stays for this patient
        stays = db.query_df(
            """
            SELECT stay_id, hadm_id, intime, outtime,
                   first_careunit, last_careunit, los
            FROM mimiciv_icu.icustays
            WHERE subject_id = ?
            ORDER BY intime
            """,
            [subject_id],
        )

        if not stays:
            markdown = "\n\n".join(
                [
                    f"## ICU Vitals for Patient {subject_id}",
                    "No ICU stays were found for this patient.",
                ]
            )
            return CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text=markdown,
                    )
                ],
                structuredContent={"vitals": [], "stays": [], "subject_id": subject_id},
            )

        # Build conditions
        conditions = ["ce.subject_id = ?"]
        params: list = [subject_id]

        id_placeholders = ",".join("?" * len(ALL_VITAL_IDS))
        conditions.append(f"ce.itemid IN ({id_placeholders})")
        params.extend(ALL_VITAL_IDS)

        if stay_id is not None:
            conditions.append("ce.stay_id = ?")
            params.append(stay_id)

        where = " AND ".join(conditions)

        sql = f"""
            SELECT
                ce.stay_id,
                ce.charttime,
                ce.itemid,
                di.label AS vital_name,
                ce.valuenum AS value,
                ce.valueuom AS unit
            FROM mimiciv_icu.chartevents ce
            JOIN mimiciv_icu.d_items di ON ce.itemid = di.itemid
            WHERE {where}
                AND ce.valuenum IS NOT NULL
            ORDER BY ce.charttime
            LIMIT 2000
        """

        rows = db.query_df(sql, params)

        target = f"stay {stay_id}" if stay_id else f"patient {subject_id}"
        recent_vitals = list(reversed(rows[-20:]))
        markdown = "\n\n".join(
            [
                f"## ICU Vitals for {target}",
                (
                    f"Retrieved **{len(rows)}** measurement(s) across "
                    f"**{len(stays)}** ICU stay(s)."
                ),
                "### ICU Stays",
                md_table(
                    ["Stay ID", "HADM ID", "In Time", "Out Time", "Care Unit"],
                    [
                        [
                            row.get("stay_id"),
                            row.get("hadm_id"),
                            row.get("intime"),
                            row.get("outtime"),
                            row.get("first_careunit"),
                        ]
                        for row in stays[:10]
                    ],
                ),
                "### Recent Vitals",
                md_table(
                    ["Chart Time", "Stay ID", "Vital", "Value", "Unit"],
                    [
                        [
                            row.get("charttime"),
                            row.get("stay_id"),
                            row.get("vital_name"),
                            row.get("value"),
                            row.get("unit"),
                        ]
                        for row in recent_vitals
                    ],
                ),
                (
                    f"_Showing most recent {len(recent_vitals)} of {len(rows)} vitals._"
                    if len(rows) > len(recent_vitals)
                    else "_Showing all vitals._"
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
                "vitals": rows,
                "stays": stays,
                "subject_id": subject_id,
                "vital_groups": {
                    name: ids for name, ids in VITAL_ITEMIDS.items()
                },
            },
        )

    @mcp.tool(
        name="list_icu_stays",
        description=(
            "List all ICU stays for a patient in MIMIC-IV. Returns stay_id, "
            "admission ID, in/out times, care unit, and length of stay. "
            "Use stay_id values with get_vitals to see vitals for a specific stay."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading ICU stays...",
            "openai/toolInvocation/invoked": "ICU stays loaded",
        },
    )
    def list_icu_stays(subject_id: int) -> CallToolResult:
        rows = db.query_df(
            """
            SELECT stay_id, hadm_id, intime, outtime,
                   first_careunit, last_careunit, los
            FROM mimiciv_icu.icustays
            WHERE subject_id = ?
            ORDER BY intime
            """,
            [subject_id],
        )
        markdown = "\n\n".join(
            [
                f"## ICU Stays for Patient {subject_id}",
                f"Found **{len(rows)}** ICU stay(s).",
                md_table(
                    ["Stay ID", "HADM ID", "In Time", "Out Time", "LOS (days)"],
                    [
                        [
                            row.get("stay_id"),
                            row.get("hadm_id"),
                            row.get("intime"),
                            row.get("outtime"),
                            row.get("los"),
                        ]
                        for row in rows[:20]
                    ],
                ),
                (
                    f"_Showing first {min(len(rows), 20)} of {len(rows)} ICU stays._"
                    if len(rows) > 20
                    else "_Showing all ICU stays._"
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
                "stays": rows,
                "subject_id": subject_id,
                "count": len(rows),
            },
        )
