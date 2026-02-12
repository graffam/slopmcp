"""Admissions and diagnoses tools."""

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent

import db
from tools.markdown import md_table


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_admissions",
        description=(
            "Get all hospital admissions for a specific patient in the "
            "MIMIC-IV database. Returns admission/discharge times, "
            "admission type, location, insurance, marital status, and race. "
            "Requires a subject_id (patient ID)."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading admissions...",
            "openai/toolInvocation/invoked": "Admissions loaded",
        },
    )
    def get_admissions(subject_id: int) -> CallToolResult:
        rows = db.query_df(
            """
            SELECT
                hadm_id,
                admittime,
                dischtime,
                deathtime,
                admission_type,
                admit_provider_id,
                admission_location,
                discharge_location,
                insurance,
                language,
                marital_status,
                race,
                edregtime,
                edouttime,
                hospital_expire_flag
            FROM mimiciv_hosp.admissions
            WHERE subject_id = ?
            ORDER BY admittime
            """,
            [subject_id],
        )
        preview_rows = rows[:10]
        markdown = "\n\n".join(
            [
                f"## Admissions for Patient {subject_id}",
                f"Found **{len(rows)}** admission(s).",
                md_table(
                    [
                        "HADM ID",
                        "Admit Time",
                        "Discharge Time",
                        "Admission Type",
                        "Insurance",
                        "Race",
                    ],
                    [
                        [
                            row.get("hadm_id"),
                            row.get("admittime"),
                            row.get("dischtime"),
                            row.get("admission_type"),
                            row.get("insurance"),
                            row.get("race"),
                        ]
                        for row in preview_rows
                    ],
                ),
                (
                    f"_Showing first {len(preview_rows)} of {len(rows)} admission(s)._"
                    if len(rows) > len(preview_rows)
                    else "_Showing all admissions._"
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
                "admissions": rows,
                "subject_id": subject_id,
                "count": len(rows),
            },
        )

    @mcp.tool(
        name="get_diagnoses",
        description=(
            "Get ICD diagnosis codes for a specific hospital admission in "
            "MIMIC-IV. Returns ICD codes with their long title descriptions "
            "and sequence numbers. Requires a hadm_id (hospital admission ID). "
            "You can get hadm_id values from the get_admissions tool."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading diagnoses...",
            "openai/toolInvocation/invoked": "Diagnoses loaded",
        },
    )
    def get_diagnoses(hadm_id: int) -> CallToolResult:
        rows = db.query_df(
            """
            SELECT
                d.seq_num,
                d.icd_code,
                d.icd_version,
                COALESCE(di.long_title, d.icd_code) AS diagnosis_title
            FROM mimiciv_hosp.diagnoses_icd d
            LEFT JOIN mimiciv_hosp.d_icd_diagnoses di
                ON d.icd_code = di.icd_code
                AND d.icd_version = di.icd_version
            WHERE d.hadm_id = ?
            ORDER BY d.seq_num
            """,
            [hadm_id],
        )
        preview_rows = rows[:15]
        markdown = "\n\n".join(
            [
                f"## Diagnoses for Admission {hadm_id}",
                f"Found **{len(rows)}** diagnosis code(s).",
                md_table(
                    ["Seq", "ICD Code", "Version", "Diagnosis"],
                    [
                        [
                            row.get("seq_num"),
                            row.get("icd_code"),
                            row.get("icd_version"),
                            row.get("diagnosis_title"),
                        ]
                        for row in preview_rows
                    ],
                ),
                (
                    f"_Showing first {len(preview_rows)} of {len(rows)} diagnoses._"
                    if len(rows) > len(preview_rows)
                    else "_Showing all diagnoses._"
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
                "diagnoses": rows,
                "hadm_id": hadm_id,
                "count": len(rows),
            },
        )

    @mcp.tool(
        name="get_prescriptions",
        description=(
            "Get medication prescriptions for a specific hospital admission "
            "in MIMIC-IV. Returns drug name, dose, route, and timing. "
            "Requires a hadm_id (hospital admission ID)."
        ),
        meta={
            "openai/toolInvocation/invoking": "Loading prescriptions...",
            "openai/toolInvocation/invoked": "Prescriptions loaded",
        },
    )
    def get_prescriptions(hadm_id: int) -> CallToolResult:
        rows = db.query_df(
            """
            SELECT
                pharmacy_id,
                drug,
                drug_type,
                prod_strength,
                dose_val_rx,
                dose_unit_rx,
                form_val_disp,
                form_unit_disp,
                route,
                starttime,
                stoptime
            FROM mimiciv_hosp.prescriptions
            WHERE hadm_id = ?
            ORDER BY starttime
            """,
            [hadm_id],
        )
        preview_rows = rows[:15]
        markdown = "\n\n".join(
            [
                f"## Prescriptions for Admission {hadm_id}",
                f"Found **{len(rows)}** prescription(s).",
                md_table(
                    ["Start Time", "Drug", "Dose", "Route", "Type"],
                    [
                        [
                            row.get("starttime"),
                            row.get("drug"),
                            (
                                f"{row.get('dose_val_rx') or ''} "
                                f"{row.get('dose_unit_rx') or ''}"
                            ).strip() or "n/a",
                            row.get("route"),
                            row.get("drug_type"),
                        ]
                        for row in preview_rows
                    ],
                ),
                (
                    f"_Showing first {len(preview_rows)} of {len(rows)} prescriptions._"
                    if len(rows) > len(preview_rows)
                    else "_Showing all prescriptions._"
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
                "prescriptions": rows,
                "hadm_id": hadm_id,
                "count": len(rows),
            },
        )
