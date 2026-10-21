"""Warehouse steps: schema setup, dimension and fact loads, reconciliation, marts."""
from __future__ import annotations

from datetime import date

from pipeline.config import SQL_DIR
from pipeline.db import run_sql_file


class ReconciliationError(Exception):
    """Raised when the warehouse does not match the validated staging data."""


def ensure_schema(conn) -> None:
    run_sql_file(conn, SQL_DIR / "schema.sql")


def load_dimensions(conn, batch_date: date) -> None:
    params = {"batch_date": batch_date}
    run_sql_file(conn, SQL_DIR / "transform" / "01_dim_product.sql", params)
    run_sql_file(conn, SQL_DIR / "transform" / "02_dim_customer.sql", params)


def load_facts(conn, batch_date: date) -> None:
    params = {"batch_date": batch_date}
    run_sql_file(conn, SQL_DIR / "transform" / "03_fact_orders.sql", params)
    run_sql_file(conn, SQL_DIR / "transform" / "04_fact_sales.sql", params)
    run_sql_file(conn, SQL_DIR / "transform" / "05_fact_payments.sql", params)


def reconcile(conn, batch_date: date) -> list[tuple]:
    run_sql_file(conn, SQL_DIR / "transform" / "06_reconcile.sql", {"batch_date": batch_date})
    with conn.cursor() as cur:
        cur.execute(
            "SELECT check_name, source_value, target_value, passed FROM audit.reconciliation "
            "WHERE batch_date = %s ORDER BY check_name",
            (batch_date,),
        )
        rows = cur.fetchall()
    failed = [r for r in rows if not r[3]]
    if failed:
        detail = "; ".join(f"{name}: staging={src}, warehouse={tgt}" for name, src, tgt, _ in failed)
        raise ReconciliationError(f"Batch {batch_date} did not reconcile -> {detail}")
    return rows


def refresh_marts(conn) -> None:
    run_sql_file(conn, SQL_DIR / "marts.sql")
