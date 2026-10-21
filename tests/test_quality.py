import pytest

from pipeline.quality import RULES, DataQualityError, TableResult, enforce_thresholds


def result(table, total, rejected):
    return TableResult(table, total, rejected, by_rule={})


def test_reject_rate():
    assert result("orders", 200, 10).reject_rate == 0.05
    assert result("orders", 0, 0).reject_rate == 0.0


def test_batch_within_limit_passes():
    enforce_thresholds([result("orders", 100, 10), result("payments", 100, 2)], 0.10)


def test_batch_over_limit_fails_and_names_the_table():
    with pytest.raises(DataQualityError, match="payments: 11/100"):
        enforce_thresholds([result("orders", 100, 1), result("payments", 100, 11)], 0.10)


def test_parents_are_validated_before_children():
    order = list(RULES)
    assert order.index("orders") < order.index("order_items")
    assert order.index("orders") < order.index("payments")
    assert order.index("products") < order.index("order_items")
