"""Download MIMIC-IV Clinical Database Demo and load into DuckDB."""

import io
import zipfile
from pathlib import Path

import duckdb
import httpx

MIMIC_IV_DEMO_URL = (
    "https://physionet.org/static/published-projects/"
    "mimic-iv-demo/mimic-iv-clinical-database-demo-2.2.zip"
)

DB_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DB_DIR / "mimic_iv_demo.duckdb"

# Mapping from CSV file path fragments to (schema, table_name).
# The zip contains paths like:
#   mimic-iv-clinical-database-demo-2.2/hosp/patients.csv.gz
#   mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv.gz
SCHEMA_MAP = {
    "hosp": "mimiciv_hosp",
    "icu": "mimiciv_icu",
}


def download_zip(url: str) -> bytes:
    print(f"Downloading {url} ...")
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        resp = client.get(url)
        resp.raise_for_status()
    print(f"Downloaded {len(resp.content) / 1024 / 1024:.1f} MB")
    return resp.content


def load_into_duckdb(zip_bytes: bytes) -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)

    # Remove existing DB so we start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = duckdb.connect(str(DB_PATH))

    # Create schemas
    for schema in SCHEMA_MAP.values():
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    csv_files = [n for n in zf.namelist() if n.endswith(".csv.gz") or n.endswith(".csv")]

    print(f"Found {len(csv_files)} CSV files in archive")

    for csv_path in sorted(csv_files):
        parts = Path(csv_path).parts
        # Expect: root_dir / subfolder / filename.csv.gz
        if len(parts) < 3:
            continue

        subfolder = parts[-2]  # "hosp" or "icu"
        filename = parts[-1]   # e.g. "patients.csv.gz"

        schema = SCHEMA_MAP.get(subfolder)
        if schema is None:
            print(f"  Skipping {csv_path} (unknown subfolder '{subfolder}')")
            continue

        # Derive table name from filename
        table_name = filename.replace(".csv.gz", "").replace(".csv", "")
        full_table = f"{schema}.{table_name}"

        print(f"  Loading {csv_path} -> {full_table} ...", end=" ")

        # Extract file to a temp bytes buffer and read with DuckDB
        csv_data = zf.read(csv_path)
        # Write to a temp file so DuckDB can read it (handles .gz natively)
        tmp_path = DB_DIR / filename
        tmp_path.write_bytes(csv_data)

        try:
            conn.execute(
                f"CREATE TABLE {full_table} AS "
                f"SELECT * FROM read_csv_auto('{tmp_path}', header=true, ignore_errors=true)"
            )
            count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
            print(f"{count} rows")
        except Exception as e:
            print(f"FAILED: {e}")
        finally:
            tmp_path.unlink(missing_ok=True)

    # Print summary
    print("\n--- Database Summary ---")
    for schema in SCHEMA_MAP.values():
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' ORDER BY table_name"
        ).fetchall()
        print(f"\n{schema}:")
        for (t,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{t}").fetchone()[0]
            print(f"  {t}: {count} rows")

    conn.close()
    print(f"\nDatabase saved to {DB_PATH}")


def main():
    zip_bytes = download_zip(MIMIC_IV_DEMO_URL)
    load_into_duckdb(zip_bytes)


if __name__ == "__main__":
    main()
