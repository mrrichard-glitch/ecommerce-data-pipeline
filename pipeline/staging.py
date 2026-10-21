"""Load step: copy raw CSV files into the staging schema as text.

Staging keeps every value exactly as the source sent it (all columns are
text), so nothing is lost or silently cast before validation. Reloading a
batch first deletes that batch, which makes the step safe to re-run.
"""
from __future__ import annotations

from datetime import date

from pipeline.generate import COLUMNS


def load_to_staging(conn, batch_date: date, files: dict[str, str]) -> dict[str, int]:
    loaded = {}
    with conn.cursor() as cur:
        for table, columns in COLUMNS.items():
            cols = ", ".join(columns)
            cur.execute(f"DELETE FROM staging.{table} WHERE batch_date = %s", (batch_date,))
            cur.execute(
                f"CREATE TEMP TABLE tmp_{table} ({', '.join(c + ' text' for c in columns)}) "
                "ON COMMIT DROP"
            )
            with open(files[table], encoding="utf-8") as f:
                cur.copy_expert(f"COPY tmp_{table} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            cur.execute(
                f"INSERT INTO staging.{table} (batch_date, {cols}) SELECT %s, {cols} FROM tmp_{table}",
                (batch_date,),
            )
            loaded[table] = cur.rowcount
    return loaded


def purge_old_batches(conn, older_than: date) -> dict[str, int]:
    """Delete staged rows and their reject log for batches before `older_than`."""
    purged = {}
    with conn.cursor() as cur:
        for table in [*COLUMNS, "rejected_records"]:
            cur.execute(f"DELETE FROM staging.{table} WHERE batch_date < %s", (older_than,))
            purged[table] = cur.rowcount
    return purged
