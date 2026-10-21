from datetime import date

from pipeline.generate import COLUMNS, build_customer, generate_day, purge_raw

DAY = date(2026, 10, 1)


def test_same_date_gives_identical_data():
    assert generate_day(DAY) == generate_day(DAY)


def test_different_dates_give_different_orders():
    assert generate_day(DAY)["orders"] != generate_day(date(2026, 10, 2))["orders"]


def test_every_row_has_exactly_the_expected_columns():
    for table, rows in generate_day(DAY).items():
        assert rows, f"{table} is empty"
        assert all(set(row) == set(COLUMNS[table]) for row in rows), table


def test_dirty_records_are_injected():
    data = generate_day(DAY)
    lines = [(r["order_id"], r["line_number"]) for r in data["order_items"]]
    assert len(lines) > len(set(lines)), "expected duplicate order lines"
    assert any(r["customer_id"] == "" for r in data["orders"])
    assert any(int(r["quantity"]) <= 0 for r in data["order_items"])
    assert any(r["product_id"] == "P999" for r in data["order_items"])
    assert any(r["amount"] == "N/A" for r in data["payments"])


def test_customer_relocation_is_stable_over_time():
    """A customer who moves keeps their new region on every later date."""
    for n in range(1, 600):
        before = build_customer(n, date(2026, 1, 1))
        after = build_customer(n, date(2026, 12, 31))
        if before["region"] != after["region"]:
            assert build_customer(n, date(2027, 6, 1))["region"] == after["region"]
            assert before["customer_id"] == after["customer_id"]
            return
    raise AssertionError("expected at least one customer to relocate during 2026")


def test_purge_raw_removes_only_older_batch_folders(tmp_path):
    for name in ["2026-09-01", "2026-09-30", "2026-10-01", "notes"]:
        (tmp_path / name).mkdir()
    assert purge_raw(tmp_path, date(2026, 10, 1)) == ["2026-09-01", "2026-09-30"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-10-01", "notes"]
