"""Validation step: check staged rows against rules before they reach the warehouse.

Rows that break a rule are not deleted. Their ids and the rule they broke are
written to staging.rejected_records, and every later step simply skips them.
That keeps an audit trail of exactly what was excluded and why.

Rules run in order, table by table, so a rule can depend on earlier results:
an order line is rejected if its parent order was rejected.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


def _duplicate(table: str, key: str) -> str:
    return (
        f"s._row_id IN (SELECT _row_id FROM (SELECT _row_id, row_number() OVER "
        f"(PARTITION BY {key} ORDER BY _row_id) AS rn FROM staging.{table} "
        f"WHERE batch_date = %(batch_date)s) d WHERE d.rn > 1)"
    )


def _valid_in(table: str, alias: str) -> str:
    return (
        f"NOT EXISTS (SELECT 1 FROM staging.rejected_records r "
        f"WHERE r.source_table = '{table}' AND r.row_id = {alias}._row_id)"
    )


def _missing(*columns: str) -> str:
    """TRUE when any of the columns is NULL or blank."""
    return " OR ".join(f"nullif(trim(s.{c}), '') IS NULL" for c in columns)


def _invalid(column: str, pattern: str, pg_type: str) -> str:
    """TRUE when the value is missing, has the wrong shape, or is not a real value of
    the type (e.g. 2026-13-45). The pattern pins the format; pg_input_is_valid (PG16+)
    catches impossible dates and also rejects words like 'now' that a cast would accept."""
    return (f"s.{column} IS NULL OR s.{column} !~ {pattern} "
            f"OR NOT pg_input_is_valid(s.{column}, '{pg_type}')")


MONEY = r"'^[0-9]+(\.[0-9]{1,2})?$'"
POSITIVE_INT = r"'^[1-9][0-9]*$'"
DATE = r"'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"
TIMESTAMP = r"'^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'"
ORDER_STATUSES = "('completed', 'pending', 'cancelled', 'returned')"

# table -> [(rule name, SQL condition that is TRUE for a bad row)]; `s` is the staged row.
# Every column the warehouse requires (NOT NULL or a cast) has a rule here, so bad
# source data is rejected and logged instead of failing the load.
RULES: dict[str, list[tuple[str, str]]] = {
    "customers": [
        ("missing_customer_id", _missing("customer_id")),
        ("missing_attributes", _missing("full_name", "region", "city")),
        ("invalid_signup_date",
         f"s.signup_date IS NOT NULL AND ({_invalid('signup_date', DATE, 'date')})"),
        ("duplicate_customer", _duplicate("customers", "trim(customer_id)")),
    ],
    "products": [
        ("missing_product_id", _missing("product_id")),
        ("missing_attributes", _missing("product_name", "category")),
        ("invalid_price", f"s.unit_price IS NULL OR s.unit_price !~ {MONEY}"),
        ("duplicate_product", _duplicate("products", "trim(product_id)")),
    ],
    "orders": [
        ("missing_order_id", _missing("order_id")),
        ("missing_customer_id", _missing("customer_id")),
        ("unknown_customer",
         "nullif(trim(s.customer_id), '') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM staging.customers c "
         "WHERE c.batch_date = s.batch_date AND trim(c.customer_id) = trim(s.customer_id) "
         f"AND {_valid_in('customers', 'c')})"),
        ("invalid_timestamp", _invalid("order_ts", TIMESTAMP, "timestamp")),
        ("invalid_status", f"s.status IS NULL OR s.status NOT IN {ORDER_STATUSES}"),
        ("invalid_delivery_fee", f"s.delivery_fee IS NULL OR s.delivery_fee !~ {MONEY}"),
        ("duplicate_order", _duplicate("orders", "order_id")),
    ],
    "order_items": [
        ("duplicate_line", _duplicate("order_items", "order_id, line_number")),
        ("invalid_line_number", f"s.line_number IS NULL OR s.line_number !~ {POSITIVE_INT}"),
        ("invalid_quantity", f"s.quantity IS NULL OR s.quantity !~ {POSITIVE_INT}"),
        ("invalid_unit_price", f"s.unit_price IS NULL OR s.unit_price !~ {MONEY}"),
        ("unknown_product",
         "NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.batch_date = s.batch_date "
         f"AND trim(p.product_id) = trim(s.product_id) AND {_valid_in('products', 'p')})"),
        ("parent_order_rejected",
         "NOT EXISTS (SELECT 1 FROM staging.orders o WHERE o.batch_date = s.batch_date "
         f"AND o.order_id = s.order_id AND {_valid_in('orders', 'o')})"),
    ],
    "payments": [
        ("missing_payment_id", _missing("payment_id")),
        ("missing_method_or_status", _missing("payment_method", "payment_status")),
        ("invalid_amount", f"s.amount IS NULL OR s.amount !~ {MONEY}"),
        ("invalid_timestamp", _invalid("paid_at", TIMESTAMP, "timestamp")),
        ("duplicate_payment", _duplicate("payments", "payment_id")),
        ("parent_order_rejected",
         "NOT EXISTS (SELECT 1 FROM staging.orders o WHERE o.batch_date = s.batch_date "
         f"AND o.order_id = s.order_id AND {_valid_in('orders', 'o')})"),
    ],
}


@dataclass(frozen=True)
class TableResult:
    table: str
    total_rows: int
    rejected_rows: int
    by_rule: dict[str, int]

    @property
    def reject_rate(self) -> float:
        return self.rejected_rows / self.total_rows if self.total_rows else 0.0


class DataQualityError(Exception):
    """Raised when a batch is too dirty to load."""


def run_checks(conn, batch_date: date) -> list[TableResult]:
    params = {"batch_date": batch_date}
    results = []
    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.rejected_records WHERE batch_date = %(batch_date)s", params)

        for table, rules in RULES.items():
            by_rule = {}
            for rule, condition in rules:
                cur.execute(
                    f"INSERT INTO staging.rejected_records (batch_date, source_table, row_id, rule) "
                    f"SELECT s.batch_date, '{table}', s._row_id, '{rule}' FROM staging.{table} s "
                    f"WHERE s.batch_date = %(batch_date)s AND ({condition})",
                    params,
                )
                by_rule[rule] = cur.rowcount

            cur.execute(
                f"SELECT count(*), (SELECT count(DISTINCT row_id) FROM staging.rejected_records "
                f"WHERE batch_date = %(batch_date)s AND source_table = '{table}') "
                f"FROM staging.{table} WHERE batch_date = %(batch_date)s",
                params,
            )
            total, rejected = cur.fetchone()
            results.append(TableResult(table, total, rejected, by_rule))

        cur.execute("DELETE FROM audit.dq_results WHERE batch_date = %(batch_date)s", params)
        for r in results:
            cur.execute(
                "INSERT INTO audit.dq_results (batch_date, source_table, total_rows, rejected_rows, reject_rate) "
                "VALUES (%s, %s, %s, %s, %s)",
                (batch_date, r.table, r.total_rows, r.rejected_rows, round(r.reject_rate, 4)),
            )
    return results


def enforce_thresholds(results: list[TableResult], max_reject_rate: float) -> None:
    """Fail the batch if any table rejects more than the allowed share of rows."""
    failures = [
        f"{r.table}: {r.rejected_rows}/{r.total_rows} rejected ({r.reject_rate:.1%})"
        for r in results
        if r.reject_rate > max_reject_rate
    ]
    if failures:
        raise DataQualityError(
            f"Reject rate above {max_reject_rate:.0%} limit -> " + "; ".join(failures)
        )


def summarise(results: list[TableResult]) -> str:
    lines = []
    for r in results:
        broken = ", ".join(f"{rule}={n}" for rule, n in r.by_rule.items() if n)
        lines.append(f"{r.table:<12} {r.total_rows:>5} rows, {r.rejected_rows:>3} rejected"
                     + (f" ({broken})" if broken else ""))
    return "\n".join(lines)
