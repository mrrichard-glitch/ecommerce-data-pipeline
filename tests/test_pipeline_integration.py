"""End-to-end run against a real PostgreSQL database.

Skipped unless WAREHOUSE_DSN points at a database (CI sets one up).
"""
import os
from datetime import date

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.getenv("WAREHOUSE_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="WAREHOUSE_DSN not set")


def scalar(sql, params=None):
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.fixture(scope="module")
def loaded(tmp_path_factory, monkeypatch_module):
    from pipeline import config, warehouse
    from pipeline.db import connect
    from pipeline.run import run_batch

    monkeypatch_module.setattr(config, "RAW_DIR", tmp_path_factory.mktemp("raw"))
    with connect(DSN) as conn:
        warehouse.ensure_schema(conn)
    run_batch(date(2026, 10, 1), DSN)
    run_batch(date(2026, 10, 2), DSN)
    with connect(DSN) as conn:
        warehouse.refresh_marts(conn)


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_reconciliation_passed(loaded):
    assert scalar("SELECT bool_and(passed) FROM audit.reconciliation WHERE batch_date = '2026-10-02'")


def test_rerunning_a_batch_does_not_duplicate_rows(loaded):
    from pipeline.run import run_batch

    before = scalar("SELECT count(*) FROM dw.fact_sales")
    run_batch(date(2026, 10, 2), DSN)
    assert scalar("SELECT count(*) FROM dw.fact_sales") == before


def test_no_rejected_row_reaches_the_warehouse(loaded):
    assert scalar("SELECT count(*) FROM dw.fact_sales WHERE quantity <= 0") == 0
    assert scalar("SELECT count(*) FROM dw.fact_sales WHERE order_id IN "
                  "(SELECT order_id FROM staging.orders WHERE customer_id IS NULL)") == 0


def test_one_current_version_per_customer(loaded):
    assert scalar("SELECT count(*) FROM (SELECT customer_id FROM dw.dim_customer "
                  "WHERE is_current GROUP BY customer_id HAVING count(*) > 1) d") == 0


def test_customer_history_has_no_gaps_or_overlaps(loaded):
    assert scalar("SELECT count(*) FROM dw.dim_customer WHERE valid_to < valid_from") == 0
    assert scalar("SELECT count(*) FROM dw.dim_customer a JOIN dw.dim_customer b "
                  "ON a.customer_id = b.customer_id AND a.customer_key < b.customer_key "
                  "AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to") == 0


def test_rerunning_an_older_day_keeps_history_and_reconciles(loaded):
    from pipeline.run import run_batch

    versions = scalar("SELECT count(*) FROM dw.dim_customer")
    run_batch(date(2026, 10, 1), DSN)  # 2026-10-02 is already loaded
    assert scalar("SELECT count(*) FROM dw.dim_customer") == versions
    assert scalar("SELECT bool_and(passed) FROM audit.reconciliation WHERE batch_date = '2026-10-01'")


def test_delivery_fees_reach_the_warehouse(loaded):
    assert scalar("SELECT sum(delivery_fee) FROM dw.fact_orders") > 0
    assert scalar("SELECT sum(delivery_fees_ghs) FROM mart.daily_sales") > 0


def stage_customer(conn, batch_date, region, city="Somewhere", customer_id="CTEST"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO staging.customers (batch_date, customer_id, full_name, email, phone, "
            "region, city, signup_date) VALUES (%s, %s, 'Test Customer', 'test@example.com', "
            "'0240000000', %s, %s, '2025-01-01')",
            (batch_date, customer_id, region, city),
        )


def load_customer_day(batch_date, region):
    from pipeline import quality, warehouse
    from pipeline.db import connect

    with connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.customers WHERE batch_date = %s", (batch_date,))
        stage_customer(conn, batch_date, region)
        quality.run_checks(conn, batch_date)
        warehouse.load_dimensions(conn, batch_date)


def customer_history():
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT region, valid_from::text, valid_to::text, is_current "
                        "FROM dw.dim_customer WHERE customer_id = 'CTEST' ORDER BY valid_from")
            return cur.fetchall()
    finally:
        conn.close()


def test_type2_history_handles_moves_reruns_and_backfills(loaded):
    from pipeline.db import connect

    with connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dw.dim_customer WHERE customer_id = 'CTEST'")

    load_customer_day(date(2030, 1, 10), "Ashanti")
    load_customer_day(date(2030, 1, 20), "Volta")        # moves
    load_customer_day(date(2030, 1, 15), "Ashanti")      # re-run inside the first version
    load_customer_day(date(2030, 1, 20), "Volta")        # same-day re-run
    assert customer_history() == [
        ("Ashanti", "2030-01-10", "2030-01-19", False),
        ("Volta", "2030-01-20", "9999-12-31", True),
    ]

    load_customer_day(date(2030, 1, 5), "Ashanti")       # back-fill, same region: stretch back
    load_customer_day(date(2030, 1, 1), "Northern")      # back-fill, other region: new early version
    load_customer_day(date(2030, 1, 15), "Central")      # late correction part-way through a version
    assert customer_history() == [
        ("Northern", "2030-01-01", "2030-01-04", False),
        ("Ashanti", "2030-01-05", "2030-01-14", False),
        ("Central", "2030-01-15", "2030-01-19", False),
        ("Volta", "2030-01-20", "9999-12-31", True),
    ]


def test_bad_values_are_rejected_not_loaded(loaded):
    from pipeline import quality, warehouse
    from pipeline.db import connect

    day = date(2030, 2, 1)
    with connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.customers WHERE batch_date = %s", (day,))
            cur.execute("DELETE FROM staging.orders WHERE batch_date = %s", (day,))
            cur.execute("INSERT INTO staging.orders (batch_date, order_id, customer_id, order_ts, "
                        "status, delivery_fee) VALUES (%s, 'BAD-1', 'CBAD1', '2030-13-45 10:00:00', "
                        "'completed', '0.00')", (day,))
        stage_customer(conn, day, region=None, customer_id="CBAD1")
        stage_customer(conn, day, region="Ashanti", customer_id="CBAD2")
        with conn.cursor() as cur:
            cur.execute("UPDATE staging.customers SET signup_date = 'now' "
                        "WHERE batch_date = %s AND customer_id = 'CBAD2'", (day,))
        quality.run_checks(conn, day)
        warehouse.load_dimensions(conn, day)  # must not fail on the bad rows

    rules = "SELECT rule FROM staging.rejected_records WHERE batch_date = '2030-02-01' AND source_table = %s"
    assert scalar(rules + " AND rule = 'invalid_timestamp'", ("orders",)) == "invalid_timestamp"
    assert scalar(rules + " AND rule = 'missing_attributes'", ("customers",)) == "missing_attributes"
    assert scalar(rules + " AND rule = 'invalid_signup_date'", ("customers",)) == "invalid_signup_date"
    assert scalar("SELECT count(*) FROM dw.dim_customer WHERE customer_id IN ('CBAD1', 'CBAD2')") == 0


def test_marts_return_data(loaded):
    assert scalar("SELECT count(*) FROM mart.daily_sales") > 0
