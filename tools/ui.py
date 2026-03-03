"""Register widget HTML files as MCP App resources."""

from __future__ import annotations

from pathlib import Path

from mcp.server.fastmcp import FastMCP

MIME_TYPE = "text/html+skybridge"
WIDGETS_DIR = Path(__file__).resolve().parent.parent / "widgets"

# Map widget name -> resource URI used in tool meta
WIDGET_URIS: dict[str, str] = {
    "patients": "ui://mimic-iv-demo/patients.html",
    "lab_results": "ui://mimic-iv-demo/lab-results.html",
    "lab_trend": "ui://mimic-iv-demo/lab-trend.html",
    "admissions": "ui://mimic-iv-demo/admissions.html",
    "diagnoses": "ui://mimic-iv-demo/diagnoses.html",
    "prescriptions": "ui://mimic-iv-demo/prescriptions.html",
    "vitals": "ui://mimic-iv-demo/vitals.html",
    "icu_stays": "ui://mimic-iv-demo/icu-stays.html",
    "patient_summary": "ui://mimic-iv-demo/patient-summary.html",
    "health_readout": "ui://mimic-iv-demo/health-readout.html",
}


def register(mcp: FastMCP) -> None:
    for name, uri in WIDGET_URIS.items():
        html_file = WIDGETS_DIR / f"{name}.html"
        _register_widget(mcp, name, uri, html_file)


def _register_widget(
    mcp: FastMCP, name: str, uri: str, html_file: Path
) -> None:
    def _make_reader(path: Path):
        @mcp.resource(
            uri,
            name=name,
            description=f"UI widget for {name.replace('_', ' ')}",
            mime_type=MIME_TYPE,
        )
        def _read() -> str:
            return path.read_text()

    _make_reader(html_file)
