"""Patient-level summary and health readout tools."""

from __future__ import annotations

import re
from statistics import mean
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, ToolAnnotations

import db
from tools.markdown import md_bullets, md_table

OMR_RESULT_NAMES = (
    "Blood Pressure",
    "BMI (kg/m2)",
    "Weight (Lbs)",
    "Height (Inches)",
)

LAB_METRIC_LABELS: dict[str, tuple[str, ...]] = {
    "creatinine": ("Creatinine",),
    "bun": ("Urea Nitrogen",),
    "glucose": ("Glucose",),
    "a1c": ("% Hemoglobin A1c",),
    "hemoglobin": ("Hemoglobin",),
    "wbc": ("White Blood Cells",),
    "platelets": ("Platelet Count",),
    "sodium": ("Sodium",),
    "potassium": ("Potassium",),
}

VITAL_METRIC_ITEMIDS: dict[str, tuple[int, ...]] = {
    "heart_rate": (220045,),
    "systolic_bp": (220050, 220179),
    "diastolic_bp": (220051, 220180),
    "resp_rate": (220210, 224690),
    "spo2": (220277,),
}

LAB_DISPLAY_NAMES = {
    "creatinine": "Creatinine",
    "bun": "Urea Nitrogen",
    "glucose": "Glucose",
    "a1c": "Hemoglobin A1c",
    "hemoglobin": "Hemoglobin",
    "wbc": "White Blood Cells",
    "platelets": "Platelet Count",
    "sodium": "Sodium",
    "potassium": "Potassium",
}

VITAL_DISPLAY_NAMES = {
    "heart_rate": "Heart Rate",
    "systolic_bp": "Systolic Blood Pressure",
    "diastolic_bp": "Diastolic Blood Pressure",
    "resp_rate": "Respiratory Rate",
    "spo2": "SpO2",
}


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _parse_bp(value: Any) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    text = str(value)
    match = re.search(r"(\d{2,3})\s*/\s*(\d{2,3})", text)
    if not match:
        return None, None
    return float(match.group(1)), float(match.group(2))


def _status_from_score(score: int) -> str:
    if score >= 85:
        return "good"
    if score >= 65:
        return "needs-attention"
    return "high-risk"


def _trend_direction(
    latest: float | None, previous: float | None, stable_band: float
) -> tuple[str, float | None]:
    if latest is None or previous is None:
        return "no-trend", None
    delta = latest - previous
    if abs(delta) <= stable_band:
        return "stable", delta
    return ("up" if delta > 0 else "down"), delta


def _fmt_number(value: float | None, decimals: int = 1) -> str | None:
    if value is None:
        return None
    if decimals == 0:
        return str(int(round(value)))
    return f"{value:.{decimals}f}"


def _query_patient(subject_id: int) -> dict[str, Any] | None:
    rows = db.query_df(
        """
        SELECT
            p.subject_id,
            p.gender,
            p.anchor_age,
            p.anchor_year,
            p.anchor_year_group,
            p.dod,
            COUNT(DISTINCT a.hadm_id) AS admission_count,
            MIN(a.admittime) AS first_admittime,
            MAX(a.dischtime) AS last_dischtime,
            MAX(COALESCE(a.hospital_expire_flag, 0)) AS had_in_hospital_mortality
        FROM mimiciv_hosp.patients p
        LEFT JOIN mimiciv_hosp.admissions a
            ON p.subject_id = a.subject_id
        WHERE p.subject_id = ?
        GROUP BY
            p.subject_id, p.gender, p.anchor_age, p.anchor_year,
            p.anchor_year_group, p.dod
        """,
        [subject_id],
    )
    if not rows:
        return None

    patient = rows[0]
    patient["icu_stay_count"] = db.query_scalar(
        "SELECT COUNT(*) FROM mimiciv_icu.icustays WHERE subject_id = ?",
        [subject_id],
    )
    return patient


def _query_admissions(subject_id: int) -> list[dict[str, Any]]:
    return db.query_df(
        """
        SELECT
            hadm_id,
            admittime,
            dischtime,
            deathtime,
            admission_type,
            admission_location,
            discharge_location,
            insurance,
            race,
            hospital_expire_flag,
            DATE_DIFF('day', admittime, dischtime) AS length_of_stay_days
        FROM mimiciv_hosp.admissions
        WHERE subject_id = ?
        ORDER BY admittime DESC
        """,
        [subject_id],
    )


def _select_admission(
    admissions: list[dict[str, Any]], hadm_id: int | None
) -> tuple[dict[str, Any] | None, str | None]:
    if not admissions:
        return None, None
    if hadm_id is None:
        return admissions[0], None

    for admission in admissions:
        if admission["hadm_id"] == hadm_id:
            return admission, None

    return admissions[0], (
        f"Requested hadm_id {hadm_id} was not found for this patient. "
        "Using the most recent admission instead."
    )


def _query_diagnoses_for_admission(hadm_id: int, limit: int = 12) -> list[dict[str, Any]]:
    return db.query_df(
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
        LIMIT ?
        """,
        [hadm_id, limit],
    )


def _query_chronic_diagnoses(subject_id: int, limit: int = 8) -> list[dict[str, Any]]:
    return db.query_df(
        """
        SELECT
            COALESCE(di.long_title, d.icd_code) AS diagnosis_title,
            COUNT(*) AS mentions
        FROM mimiciv_hosp.diagnoses_icd d
        JOIN mimiciv_hosp.admissions a ON d.hadm_id = a.hadm_id
        LEFT JOIN mimiciv_hosp.d_icd_diagnoses di
            ON d.icd_code = di.icd_code
            AND d.icd_version = di.icd_version
        WHERE a.subject_id = ?
        GROUP BY diagnosis_title
        ORDER BY mentions DESC, diagnosis_title
        LIMIT ?
        """,
        [subject_id, limit],
    )


def _query_recent_medications(hadm_id: int, limit: int = 12) -> list[dict[str, Any]]:
    return db.query_df(
        """
        WITH ranked AS (
            SELECT
                drug,
                drug_type,
                route,
                dose_val_rx,
                dose_unit_rx,
                starttime,
                stoptime,
                ROW_NUMBER() OVER (
                    PARTITION BY drug
                    ORDER BY starttime DESC NULLS LAST
                ) AS rn
            FROM mimiciv_hosp.prescriptions
            WHERE hadm_id = ?
                AND drug IS NOT NULL
        )
        SELECT
            drug,
            drug_type,
            route,
            dose_val_rx,
            dose_unit_rx,
            starttime,
            stoptime
        FROM ranked
        WHERE rn = 1
        ORDER BY starttime DESC NULLS LAST
        LIMIT ?
        """,
        [hadm_id, limit],
    )


def _query_omr_history(subject_id: int, per_metric: int = 2) -> dict[str, list[dict[str, Any]]]:
    placeholders = ",".join("?" for _ in OMR_RESULT_NAMES)
    rows = db.query_df(
        f"""
        WITH ranked AS (
            SELECT
                result_name,
                chartdate,
                seq_num,
                result_value,
                ROW_NUMBER() OVER (
                    PARTITION BY result_name
                    ORDER BY chartdate DESC, seq_num DESC
                ) AS rn
            FROM mimiciv_hosp.omr
            WHERE subject_id = ?
                AND result_name IN ({placeholders})
        )
        SELECT result_name, chartdate, seq_num, result_value
        FROM ranked
        WHERE rn <= ?
        ORDER BY result_name, chartdate DESC, seq_num DESC
        """,
        [subject_id, *OMR_RESULT_NAMES, per_metric],
    )

    history: dict[str, list[dict[str, Any]]] = {name: [] for name in OMR_RESULT_NAMES}
    for row in rows:
        history.setdefault(row["result_name"], []).append(row)
    return history


def _query_lab_history(
    subject_id: int,
    hadm_id: int | None = None,
    per_metric: int = 2,
) -> dict[str, list[dict[str, Any]]]:
    all_labels = sorted({label for labels in LAB_METRIC_LABELS.values() for label in labels})
    label_placeholders = ",".join("?" for _ in all_labels)
    hadm_clause = "AND le.hadm_id = ?" if hadm_id is not None else ""

    case_lines = []
    for metric, labels in LAB_METRIC_LABELS.items():
        quoted_labels = ", ".join("'" + label.replace("'", "''") + "'" for label in labels)
        case_lines.append(f"WHEN di.label IN ({quoted_labels}) THEN '{metric}'")
    case_sql = "CASE " + " ".join(case_lines) + " ELSE NULL END"

    params: list[Any] = [subject_id, *all_labels]
    if hadm_id is not None:
        params.append(hadm_id)
    params.append(per_metric)

    rows = db.query_df(
        f"""
        WITH lab_candidates AS (
            SELECT
                le.hadm_id,
                le.charttime,
                di.label,
                le.valuenum,
                le.valueuom,
                le.flag,
                le.ref_range_lower,
                le.ref_range_upper,
                {case_sql} AS metric
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems di ON le.itemid = di.itemid
            WHERE le.subject_id = ?
                AND le.valuenum IS NOT NULL
                AND di.label IN ({label_placeholders})
                {hadm_clause}
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY metric
                    ORDER BY charttime DESC
                ) AS rn
            FROM lab_candidates
            WHERE metric IS NOT NULL
        )
        SELECT
            metric,
            hadm_id,
            charttime,
            label,
            valuenum,
            valueuom,
            flag,
            ref_range_lower,
            ref_range_upper
        FROM ranked
        WHERE rn <= ?
        ORDER BY metric, charttime DESC
        """,
        params,
    )

    history: dict[str, list[dict[str, Any]]] = {metric: [] for metric in LAB_METRIC_LABELS}
    for row in rows:
        history.setdefault(row["metric"], []).append(row)
    return history


def _query_vital_history(
    subject_id: int,
    hadm_id: int | None = None,
    per_metric: int = 2,
) -> dict[str, list[dict[str, Any]]]:
    all_itemids = sorted({itemid for ids in VITAL_METRIC_ITEMIDS.values() for itemid in ids})
    id_placeholders = ",".join("?" for _ in all_itemids)
    hadm_clause = "AND ce.hadm_id = ?" if hadm_id is not None else ""

    case_lines = []
    for metric, ids in VITAL_METRIC_ITEMIDS.items():
        case_lines.append(f"WHEN ce.itemid IN ({', '.join(str(i) for i in ids)}) THEN '{metric}'")
    case_sql = "CASE " + " ".join(case_lines) + " ELSE NULL END"

    params: list[Any] = [subject_id, *all_itemids]
    if hadm_id is not None:
        params.append(hadm_id)
    params.append(per_metric)

    rows = db.query_df(
        f"""
        WITH vital_candidates AS (
            SELECT
                ce.hadm_id,
                ce.stay_id,
                ce.charttime,
                ce.itemid,
                ce.valuenum,
                ce.valueuom,
                {case_sql} AS metric
            FROM mimiciv_icu.chartevents ce
            WHERE ce.subject_id = ?
                AND ce.valuenum IS NOT NULL
                AND ce.itemid IN ({id_placeholders})
                {hadm_clause}
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY metric
                    ORDER BY charttime DESC
                ) AS rn
            FROM vital_candidates
            WHERE metric IS NOT NULL
        )
        SELECT
            metric,
            hadm_id,
            stay_id,
            charttime,
            itemid,
            valuenum,
            valueuom
        FROM ranked
        WHERE rn <= ?
        ORDER BY metric, charttime DESC
        """,
        params,
    )

    history: dict[str, list[dict[str, Any]]] = {metric: [] for metric in VITAL_METRIC_ITEMIDS}
    for row in rows:
        history.setdefault(row["metric"], []).append(row)
    return history


def _score_bp(systolic: float | None, diastolic: float | None) -> tuple[int, str, str]:
    if systolic is None or diastolic is None:
        return 50, "unknown", "No recent blood pressure data."
    if systolic >= 180 or diastolic >= 120:
        return 25, "high-risk", "Severely elevated blood pressure."
    if systolic < 80 or diastolic < 50:
        return 35, "high-risk", "Blood pressure is in a low range."
    if systolic >= 140 or diastolic >= 90:
        return 55, "high-risk", "Blood pressure is above stage-2 range."
    if systolic >= 130 or diastolic >= 80:
        return 70, "needs-attention", "Blood pressure is in stage-1 range."
    if systolic >= 120 and diastolic < 80:
        return 82, "needs-attention", "Systolic pressure is mildly elevated."
    if systolic < 90 or diastolic < 60:
        return 65, "needs-attention", "Blood pressure is mildly low."
    return 94, "good", "Blood pressure is in the target range."


def _score_bmi(bmi: float | None) -> tuple[int, str, str]:
    if bmi is None:
        return 50, "unknown", "No recent BMI data."
    if bmi < 16:
        return 40, "high-risk", "BMI indicates severe underweight."
    if bmi < 18.5:
        return 64, "needs-attention", "BMI is below the healthy range."
    if bmi < 25:
        return 94, "good", "BMI is in the healthy range."
    if bmi < 30:
        return 78, "needs-attention", "BMI is in the overweight range."
    if bmi < 35:
        return 60, "high-risk", "BMI is in obesity class I."
    if bmi < 40:
        return 50, "high-risk", "BMI is in obesity class II."
    return 40, "high-risk", "BMI is in obesity class III."


def _score_glucose(a1c: float | None, glucose: float | None) -> tuple[int, str, str, str]:
    if a1c is not None:
        if a1c < 5.7:
            return 94, "good", "A1c is in non-diabetic range.", "a1c"
        if a1c < 6.5:
            return 78, "needs-attention", "A1c is in prediabetes range.", "a1c"
        if a1c < 8.0:
            return 60, "high-risk", "A1c is above diabetic target range.", "a1c"
        return 45, "high-risk", "A1c is markedly elevated.", "a1c"

    if glucose is None:
        return 50, "unknown", "No recent glucose markers.", "glucose"
    if glucose < 70:
        return 40, "high-risk", "Glucose is in hypoglycemic range.", "glucose"
    if glucose <= 140:
        return 88, "good", "Glucose is in a reasonable range.", "glucose"
    if glucose <= 199:
        return 70, "needs-attention", "Glucose is elevated.", "glucose"
    return 48, "high-risk", "Glucose is markedly elevated.", "glucose"


def _score_kidney(creatinine: float | None, bun: float | None) -> tuple[int, str, str]:
    if creatinine is None:
        return 50, "unknown", "No recent kidney markers."

    if creatinine <= 1.3:
        score = 90
        status = "good"
        insight = "Creatinine is within the expected range."
    elif creatinine <= 2.0:
        score = 72
        status = "needs-attention"
        insight = "Creatinine is mildly elevated."
    else:
        score = 50
        status = "high-risk"
        insight = "Creatinine is significantly elevated."

    if bun is not None and bun > 35:
        score = max(35, score - 8)
        if status == "good":
            status = "needs-attention"
            insight = "Creatinine is normal but BUN is elevated."
    return score, status, insight


def _score_spo2(spo2: float | None) -> tuple[int, str, str]:
    if spo2 is None:
        return 50, "unknown", "No recent oxygen saturation values."
    if spo2 >= 95:
        return 92, "good", "Oxygen saturation is in the target range."
    if spo2 >= 90:
        return 70, "needs-attention", "Oxygen saturation is mildly low."
    return 45, "high-risk", "Oxygen saturation is low."


def _hematology_component_scores(
    hemoglobin: float | None, wbc: float | None, platelets: float | None
) -> tuple[int, str, str]:
    components: list[tuple[str, int, str]] = []

    if hemoglobin is not None:
        if hemoglobin < 10:
            components.append(("Hemoglobin", 45, "low"))
        elif hemoglobin < 12:
            components.append(("Hemoglobin", 70, "mildly low"))
        elif hemoglobin > 18:
            components.append(("Hemoglobin", 70, "high"))
        else:
            components.append(("Hemoglobin", 90, "normal"))

    if wbc is not None:
        if wbc < 3 or wbc > 15:
            components.append(("WBC", 45, "abnormal"))
        elif wbc < 4 or wbc > 11:
            components.append(("WBC", 70, "outside reference"))
        else:
            components.append(("WBC", 90, "normal"))

    if platelets is not None:
        if platelets < 100 or platelets > 600:
            components.append(("Platelets", 45, "abnormal"))
        elif platelets < 150 or platelets > 450:
            components.append(("Platelets", 70, "outside reference"))
        else:
            components.append(("Platelets", 90, "normal"))

    if not components:
        return 50, "unknown", "No recent hematology markers."

    score = round(mean(component[1] for component in components))
    if any(component[1] <= 50 for component in components):
        status = "high-risk"
    elif any(component[1] < 85 for component in components):
        status = "needs-attention"
    else:
        status = "good"

    abnormal = [
        f"{name} {detail}"
        for name, sub_score, detail in components
        if sub_score < 85 and detail != "normal"
    ]
    if abnormal:
        insight = " / ".join(abnormal)
    else:
        insight = "Hematology markers are in expected range."
    return score, status, insight


def _latest(history: dict[str, list[dict[str, Any]]], metric: str) -> dict[str, Any] | None:
    rows = history.get(metric, [])
    return rows[0] if rows else None


def _previous(history: dict[str, list[dict[str, Any]]], metric: str) -> dict[str, Any] | None:
    rows = history.get(metric, [])
    return rows[1] if len(rows) > 1 else None


def _normalize_latest_labs(
    labs: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for metric, rows in labs.items():
        if not rows:
            continue
        row = rows[0]
        output.append(
            {
                "metric": metric,
                "display_name": LAB_DISPLAY_NAMES.get(metric, metric),
                "label": row["label"],
                "value": row["valuenum"],
                "unit": row["valueuom"],
                "flag": row["flag"],
                "charttime": row["charttime"],
                "hadm_id": row["hadm_id"],
                "ref_range_lower": row["ref_range_lower"],
                "ref_range_upper": row["ref_range_upper"],
            }
        )
    output.sort(key=lambda row: str(row.get("charttime") or ""), reverse=True)
    return output


def _normalize_latest_vitals(
    vitals: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for metric, rows in vitals.items():
        if not rows:
            continue
        row = rows[0]
        output.append(
            {
                "metric": metric,
                "display_name": VITAL_DISPLAY_NAMES.get(metric, metric),
                "value": row["valuenum"],
                "unit": row["valueuom"],
                "charttime": row["charttime"],
                "hadm_id": row["hadm_id"],
                "stay_id": row["stay_id"],
            }
        )
    output.sort(key=lambda row: str(row.get("charttime") or ""), reverse=True)
    return output


def _build_readout(
    subject_id: int,
    omr: dict[str, list[dict[str, Any]]],
    labs: dict[str, list[dict[str, Any]]],
    vitals: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []

    bp_latest = _latest(omr, "Blood Pressure")
    bp_prev = _previous(omr, "Blood Pressure")
    source = "OMR"
    bp_sys, bp_dia = _parse_bp(bp_latest["result_value"]) if bp_latest else (None, None)
    prev_sys, prev_dia = _parse_bp(bp_prev["result_value"]) if bp_prev else (None, None)
    recorded_at = bp_latest["chartdate"] if bp_latest else None
    value_label = None

    if bp_sys is None or bp_dia is None:
        vital_sys = _latest(vitals, "systolic_bp")
        vital_dia = _latest(vitals, "diastolic_bp")
        if vital_sys and vital_dia:
            bp_sys = _as_float(vital_sys["valuenum"])
            bp_dia = _as_float(vital_dia["valuenum"])
            prev_sys_row = _previous(vitals, "systolic_bp")
            prev_dia_row = _previous(vitals, "diastolic_bp")
            prev_sys = _as_float(prev_sys_row["valuenum"]) if prev_sys_row else None
            prev_dia = _as_float(prev_dia_row["valuenum"]) if prev_dia_row else None
            recorded_at = max(str(vital_sys["charttime"]), str(vital_dia["charttime"]))
            source = "ICU vitals"

    bp_score, bp_status, bp_insight = _score_bp(bp_sys, bp_dia)
    if bp_sys is not None and bp_dia is not None:
        value_label = f"{int(round(bp_sys))}/{int(round(bp_dia))}"
    bp_trend = "no-trend"
    bp_delta_text = None
    if bp_sys is not None and bp_dia is not None and prev_sys is not None and prev_dia is not None:
        sys_dir, sys_delta = _trend_direction(bp_sys, prev_sys, stable_band=3.0)
        dia_dir, dia_delta = _trend_direction(bp_dia, prev_dia, stable_band=3.0)
        if sys_dir == "stable" and dia_dir == "stable":
            bp_trend = "stable"
            bp_delta_text = "stable vs prior reading"
        elif sys_dir == "up" or dia_dir == "up":
            bp_trend = "up"
            bp_delta_text = (
                f"+{abs(int(round(sys_delta or 0)))}/+{abs(int(round(dia_delta or 0)))} vs prior"
            )
        else:
            bp_trend = "down"
            bp_delta_text = (
                f"-{abs(int(round(sys_delta or 0)))}/-{abs(int(round(dia_delta or 0)))} vs prior"
            )

    cards.append(
        {
            "id": "blood_pressure",
            "title": "Blood Pressure",
            "score": bp_score,
            "status": bp_status,
            "value": value_label or "No data",
            "unit": "mmHg",
            "recorded_at": recorded_at,
            "trend": bp_trend,
            "trend_detail": bp_delta_text,
            "insight": bp_insight,
            "source": source,
        }
    )

    bmi_latest = _latest(omr, "BMI (kg/m2)")
    bmi_prev = _previous(omr, "BMI (kg/m2)")
    weight_latest = _latest(omr, "Weight (Lbs)")
    weight_prev = _previous(omr, "Weight (Lbs)")
    height_latest = _latest(omr, "Height (Inches)")

    bmi_val = _as_float(bmi_latest["result_value"]) if bmi_latest else None
    weight_lbs = _as_float(weight_latest["result_value"]) if weight_latest else None
    height_inches = _as_float(height_latest["result_value"]) if height_latest else None
    if bmi_val is None and weight_lbs is not None and height_inches and height_inches > 0:
        bmi_val = (weight_lbs * 703.0) / (height_inches * height_inches)

    prev_bmi = _as_float(bmi_prev["result_value"]) if bmi_prev else None
    bmi_score, bmi_status, bmi_insight = _score_bmi(bmi_val)
    bmi_trend, bmi_delta = _trend_direction(bmi_val, prev_bmi, stable_band=0.3)

    cards.append(
        {
            "id": "body_composition",
            "title": "Body Composition",
            "score": bmi_score,
            "status": bmi_status,
            "value": _fmt_number(bmi_val, decimals=1) or "No data",
            "unit": "kg/m2",
            "recorded_at": bmi_latest["chartdate"] if bmi_latest else (weight_latest["chartdate"] if weight_latest else None),
            "trend": bmi_trend,
            "trend_detail": (
                f"{bmi_delta:+.1f} BMI vs prior"
                if bmi_delta is not None
                else None
            ),
            "insight": bmi_insight,
            "source": "OMR",
            "secondary_value": (
                f"{_fmt_number(weight_lbs, 1)} lbs"
                if weight_lbs is not None
                else None
            ),
            "secondary_trend": (
                f"{(_as_float(weight_latest['result_value']) - _as_float(weight_prev['result_value'])):+.1f} lbs"
                if weight_latest and weight_prev
                and _as_float(weight_latest["result_value"]) is not None
                and _as_float(weight_prev["result_value"]) is not None
                else None
            ),
        }
    )

    glucose_latest = _latest(labs, "glucose")
    a1c_latest = _latest(labs, "a1c")
    glucose_val = _as_float(glucose_latest["valuenum"]) if glucose_latest else None
    a1c_val = _as_float(a1c_latest["valuenum"]) if a1c_latest else None
    glucose_score, glucose_status, glucose_insight, glucose_basis = _score_glucose(a1c_val, glucose_val)

    cards.append(
        {
            "id": "glucose_control",
            "title": "Glucose Control",
            "score": glucose_score,
            "status": glucose_status,
            "value": (
                _fmt_number(a1c_val, 1) if glucose_basis == "a1c" else _fmt_number(glucose_val, 0)
            ) or "No data",
            "unit": "%" if glucose_basis == "a1c" else "mg/dL",
            "recorded_at": (
                a1c_latest["charttime"]
                if glucose_basis == "a1c" and a1c_latest
                else (glucose_latest["charttime"] if glucose_latest else None)
            ),
            "trend": "no-trend",
            "trend_detail": (
                "using A1c"
                if glucose_basis == "a1c"
                else "using latest serum glucose"
            ),
            "insight": glucose_insight,
            "source": "Labs",
        }
    )

    creatinine_latest = _latest(labs, "creatinine")
    bun_latest = _latest(labs, "bun")
    creatinine_val = _as_float(creatinine_latest["valuenum"]) if creatinine_latest else None
    bun_val = _as_float(bun_latest["valuenum"]) if bun_latest else None
    kidney_score, kidney_status, kidney_insight = _score_kidney(creatinine_val, bun_val)

    cards.append(
        {
            "id": "kidney_function",
            "title": "Kidney Function",
            "score": kidney_score,
            "status": kidney_status,
            "value": _fmt_number(creatinine_val, 2) or "No data",
            "unit": "mg/dL",
            "recorded_at": creatinine_latest["charttime"] if creatinine_latest else None,
            "trend": "no-trend",
            "trend_detail": (
                f"BUN {_fmt_number(bun_val, 0)} mg/dL"
                if bun_val is not None
                else None
            ),
            "insight": kidney_insight,
            "source": "Labs",
        }
    )

    spo2_latest = _latest(vitals, "spo2")
    spo2_val = _as_float(spo2_latest["valuenum"]) if spo2_latest else None
    spo2_prev_row = _previous(vitals, "spo2")
    spo2_prev = _as_float(spo2_prev_row["valuenum"]) if spo2_prev_row else None
    spo2_score, spo2_status, spo2_insight = _score_spo2(spo2_val)
    spo2_trend, spo2_delta = _trend_direction(spo2_val, spo2_prev, stable_band=1.0)

    cards.append(
        {
            "id": "oxygenation",
            "title": "Oxygenation",
            "score": spo2_score,
            "status": spo2_status,
            "value": _fmt_number(spo2_val, 0) or "No data",
            "unit": "%",
            "recorded_at": spo2_latest["charttime"] if spo2_latest else None,
            "trend": spo2_trend,
            "trend_detail": (
                f"{spo2_delta:+.0f} pts vs prior"
                if spo2_delta is not None
                else None
            ),
            "insight": spo2_insight,
            "source": "ICU vitals",
        }
    )

    hemoglobin = _as_float(_latest(labs, "hemoglobin")["valuenum"]) if _latest(labs, "hemoglobin") else None
    wbc = _as_float(_latest(labs, "wbc")["valuenum"]) if _latest(labs, "wbc") else None
    platelets = _as_float(_latest(labs, "platelets")["valuenum"]) if _latest(labs, "platelets") else None
    heme_score, heme_status, heme_insight = _hematology_component_scores(hemoglobin, wbc, platelets)

    cards.append(
        {
            "id": "hematology",
            "title": "Hematology",
            "score": heme_score,
            "status": heme_status,
            "value": (
                f"Hgb {_fmt_number(hemoglobin, 1) or 'NA'} / "
                f"WBC {_fmt_number(wbc, 1) or 'NA'} / "
                f"Plt {_fmt_number(platelets, 0) or 'NA'}"
            ),
            "unit": "",
            "recorded_at": max(
                [
                    str(_latest(labs, "hemoglobin")["charttime"]) if _latest(labs, "hemoglobin") else "",
                    str(_latest(labs, "wbc")["charttime"]) if _latest(labs, "wbc") else "",
                    str(_latest(labs, "platelets")["charttime"]) if _latest(labs, "platelets") else "",
                ]
            )
            or None,
            "trend": "no-trend",
            "trend_detail": None,
            "insight": heme_insight,
            "source": "Labs",
        }
    )

    overall_score = round(mean(card["score"] for card in cards)) if cards else 50
    overall_status = _status_from_score(overall_score)

    concern_cards = [
        card for card in cards if card["status"] in {"needs-attention", "high-risk"}
    ]
    concern_cards.sort(key=lambda card: card["score"])
    insights = [card["insight"] for card in concern_cards[:4] if card.get("insight")]

    return {
        "subject_id": subject_id,
        "overall_score": overall_score,
        "overall_status": overall_status,
        "cards": cards,
        "insights": insights,
        "available_data": {
            "has_omr": any(bool(rows) for rows in omr.values()),
            "has_labs": any(bool(rows) for rows in labs.values()),
            "has_icu_vitals": any(bool(rows) for rows in vitals.values()),
        },
    }


def _build_summary_text(
    patient: dict[str, Any],
    selected_admission: dict[str, Any] | None,
    diagnoses: list[dict[str, Any]],
    readout: dict[str, Any],
    warning: str | None,
) -> str:
    parts = [
        (
            f"Patient {patient['subject_id']} ({patient['gender']}, age {patient['anchor_age']}) "
            f"has {patient['admission_count']} admission(s) and "
            f"{patient['icu_stay_count']} ICU stay(s) in the demo dataset."
        )
    ]

    if selected_admission is not None:
        parts.append(
            (
                f"Focused admission: {selected_admission['hadm_id']} "
                f"({selected_admission.get('admission_type') or 'unknown type'}), "
                f"admitted {selected_admission.get('admittime')}, discharged "
                f"{selected_admission.get('dischtime')}."
            )
        )

    if diagnoses:
        lead_dx = ", ".join(dx["diagnosis_title"] for dx in diagnoses[:3])
        parts.append(f"Top diagnoses for this admission: {lead_dx}.")

    parts.append(
        (
            f"Health readout score is {readout['overall_score']}/100 "
            f"({readout['overall_status']})."
        )
    )
    if readout.get("insights"):
        parts.append("Priority concerns: " + "; ".join(readout["insights"][:3]) + ".")
    if warning:
        parts.append(warning)

    return " ".join(parts)


def _build_summary_markdown(
    *,
    patient: dict[str, Any],
    selected_admission: dict[str, Any] | None,
    diagnoses: list[dict[str, Any]],
    chronic_conditions: list[dict[str, Any]],
    medications: list[dict[str, Any]],
    latest_labs: list[dict[str, Any]],
    latest_vitals: list[dict[str, Any]],
    readout: dict[str, Any],
    summary_text: str,
    warning: str | None,
) -> str:
    overview = summary_text
    demographics = md_bullets(
        [
            f"Sex: {patient.get('gender')}",
            f"Anchor age: {patient.get('anchor_age')}",
            f"Admissions in dataset: {patient.get('admission_count')}",
            f"ICU stays in dataset: {patient.get('icu_stay_count')}",
            f"Anchor year group: {patient.get('anchor_year_group')}",
        ],
        empty_text="_No demographic fields available._",
    )

    admission_table = md_table(
        ["HADM ID", "Admission Type", "Admit Time", "Discharge Time", "Race"],
        (
            [
                [
                    selected_admission.get("hadm_id"),
                    selected_admission.get("admission_type"),
                    selected_admission.get("admittime"),
                    selected_admission.get("dischtime"),
                    selected_admission.get("race"),
                ]
            ]
            if selected_admission is not None
            else []
        ),
    )
    diagnoses_table = md_table(
        ["Seq", "ICD Code", "Diagnosis"],
        [
            [
                row.get("seq_num"),
                row.get("icd_code"),
                row.get("diagnosis_title"),
            ]
            for row in diagnoses[:12]
        ],
    )
    chronic_list = md_bullets(
        [
            f"{row.get('diagnosis_title')} (mentions: {row.get('mentions')})"
            for row in chronic_conditions[:8]
        ],
        empty_text="_No chronic-condition rollup available._",
    )
    medication_table = md_table(
        ["Drug", "Dose", "Route", "Start", "Stop"],
        [
            [
                row.get("drug"),
                (
                    f"{row.get('dose_val_rx') or ''} "
                    f"{row.get('dose_unit_rx') or ''}"
                ).strip()
                or "n/a",
                row.get("route"),
                row.get("starttime"),
                row.get("stoptime"),
            ]
            for row in medications[:12]
        ],
    )
    labs_table = md_table(
        ["Metric", "Value", "Flag", "Time"],
        [
            [
                row.get("display_name"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("flag") or "normal",
                row.get("charttime"),
            ]
            for row in latest_labs[:12]
        ],
    )
    vitals_table = md_table(
        ["Metric", "Value", "Time"],
        [
            [
                row.get("display_name"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("charttime"),
            ]
            for row in latest_vitals[:12]
        ],
    )
    readout_table = md_table(
        ["Domain", "Score", "Status", "Value", "Insight"],
        [
            [
                row.get("title"),
                f"{row.get('score')}/100",
                row.get("status"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("insight"),
            ]
            for row in readout.get("cards", [])[:12]
        ],
    )
    concern_list = md_bullets(
        readout.get("insights", [])[:6],
        empty_text="_No priority concerns flagged._",
    )

    sections = [
        f"## Patient Summary: {patient.get('subject_id')}",
        overview,
        "### Demographics",
        demographics,
        "### Focused Admission",
        admission_table,
        "### Diagnoses (Admission)",
        diagnoses_table,
        "### Chronic Conditions (Across Admissions)",
        chronic_list,
        "### Recent Medications",
        medication_table,
        "### Key Labs",
        labs_table,
        "### Key Vitals",
        vitals_table,
        "### Health Readout",
        (
            f"Overall score: **{readout.get('overall_score')}/100** "
            f"({readout.get('overall_status')})."
        ),
        readout_table,
        "### Priority Concerns",
        concern_list,
    ]
    if warning:
        sections.extend(["### Note", warning])
    return "\n\n".join(sections)


def _build_readout_markdown(
    *,
    subject_id: int,
    readout: dict[str, Any],
    latest_labs: list[dict[str, Any]],
    latest_vitals: list[dict[str, Any]],
) -> str:
    score_line = (
        f"Overall score: **{readout.get('overall_score')}/100** "
        f"({readout.get('overall_status')})."
    )
    readout_table = md_table(
        ["Domain", "Score", "Status", "Value", "Insight"],
        [
            [
                row.get("title"),
                f"{row.get('score')}/100",
                row.get("status"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("insight"),
            ]
            for row in readout.get("cards", [])[:12]
        ],
    )
    labs_table = md_table(
        ["Metric", "Value", "Flag", "Time"],
        [
            [
                row.get("display_name"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("flag") or "normal",
                row.get("charttime"),
            ]
            for row in latest_labs[:10]
        ],
    )
    vitals_table = md_table(
        ["Metric", "Value", "Time"],
        [
            [
                row.get("display_name"),
                (
                    f"{row.get('value') if row.get('value') is not None else 'n/a'} "
                    f"{row.get('unit') or ''}"
                ).strip(),
                row.get("charttime"),
            ]
            for row in latest_vitals[:10]
        ],
    )
    concern_list = md_bullets(
        readout.get("insights", [])[:6],
        empty_text="_No priority concerns flagged._",
    )
    return "\n\n".join(
        [
            f"## Health Readout: Patient {subject_id}",
            score_line,
            "### Domain Scores",
            readout_table,
            "### Priority Concerns",
            concern_list,
            "### Latest Labs",
            labs_table,
            "### Latest Vitals",
            vitals_table,
        ]
    )


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="summarize_patient",
        description=(
            "Build a patient-level clinical summary from MIMIC-IV demo data. "
            "Returns demographics, admission timeline, admission diagnoses, "
            "recent medications, latest key labs and vitals, plus a health "
            "readout scorecard. Requires subject_id and optionally hadm_id."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
    )
    def summarize_patient(
        subject_id: int,
        hadm_id: int | None = None,
    ) -> CallToolResult:
        patient = _query_patient(subject_id)
        if not patient:
            return CallToolResult(
                isError=True,
                content=[
                    TextContent(
                        type="text",
                        text=f"Patient {subject_id} was not found.",
                    )
                ],
            )

        admissions = _query_admissions(subject_id)
        selected_admission, warning = _select_admission(admissions, hadm_id)
        selected_hadm_id = selected_admission["hadm_id"] if selected_admission else None

        diagnoses = (
            _query_diagnoses_for_admission(selected_hadm_id)
            if selected_hadm_id is not None
            else []
        )
        chronic_conditions = _query_chronic_diagnoses(subject_id)
        medications = (
            _query_recent_medications(selected_hadm_id)
            if selected_hadm_id is not None
            else []
        )

        omr_history = _query_omr_history(subject_id, per_metric=2)
        lab_history = _query_lab_history(subject_id, hadm_id=selected_hadm_id, per_metric=2)
        vital_history = _query_vital_history(subject_id, hadm_id=selected_hadm_id, per_metric=2)
        readout = _build_readout(subject_id, omr_history, lab_history, vital_history)

        latest_labs = _normalize_latest_labs(lab_history)
        latest_vitals = _normalize_latest_vitals(vital_history)
        summary_text = _build_summary_text(
            patient=patient,
            selected_admission=selected_admission,
            diagnoses=diagnoses,
            readout=readout,
            warning=warning,
        )
        summary_markdown = _build_summary_markdown(
            patient=patient,
            selected_admission=selected_admission,
            diagnoses=diagnoses,
            chronic_conditions=chronic_conditions,
            medications=medications,
            latest_labs=latest_labs,
            latest_vitals=latest_vitals,
            readout=readout,
            summary_text=summary_text,
            warning=warning,
        )

        structured = {
            "subject_id": subject_id,
            "patient": patient,
            "admissions": admissions[:10],
            "selected_admission": selected_admission,
            "diagnoses": diagnoses,
            "chronic_conditions": chronic_conditions,
            "medications": medications,
            "latest_labs": latest_labs,
            "latest_vitals": latest_vitals,
            "readout": readout,
            "summary_text": summary_text,
            "summary_markdown": summary_markdown,
            "warning": warning,
        }

        return CallToolResult(
            content=[TextContent(type="text", text=summary_markdown)],
            structuredContent=structured,
        )

    @mcp.tool(
        name="get_health_readout",
        description=(
            "Generate an Apple Health-style patient readout from available "
            "MIMIC-IV demo data. Scores blood pressure, body composition, "
            "glucose control, kidney function, oxygenation, and hematology. "
            "Requires subject_id."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
    )
    def get_health_readout(subject_id: int) -> CallToolResult:
        patient = _query_patient(subject_id)
        if not patient:
            return CallToolResult(
                isError=True,
                content=[
                    TextContent(
                        type="text",
                        text=f"Patient {subject_id} was not found.",
                    )
                ],
            )

        omr_history = _query_omr_history(subject_id, per_metric=2)
        lab_history = _query_lab_history(subject_id, hadm_id=None, per_metric=2)
        vital_history = _query_vital_history(subject_id, hadm_id=None, per_metric=2)
        readout = _build_readout(subject_id, omr_history, lab_history, vital_history)

        summary = (
            f"Health readout for patient {subject_id}: "
            f"{readout['overall_score']}/100 ({readout['overall_status']})."
        )
        if readout.get("insights"):
            summary += " Key concerns: " + "; ".join(readout["insights"][:3]) + "."
        latest_labs = _normalize_latest_labs(lab_history)
        latest_vitals = _normalize_latest_vitals(vital_history)
        summary_markdown = _build_readout_markdown(
            subject_id=subject_id,
            readout=readout,
            latest_labs=latest_labs,
            latest_vitals=latest_vitals,
        )

        return CallToolResult(
            content=[TextContent(type="text", text=summary_markdown)],
            structuredContent={
                "subject_id": subject_id,
                "patient": patient,
                "readout": readout,
                "summary_text": summary,
                "summary_markdown": summary_markdown,
                "latest_labs": latest_labs,
                "latest_vitals": latest_vitals,
            },
        )
