-- Compare what passed validation in staging with what landed in the warehouse.
DELETE FROM audit.reconciliation WHERE batch_date = %(batch_date)s;

WITH valid_orders AS (
    SELECT o.* FROM staging.orders o
    WHERE o.batch_date = %(batch_date)s
      AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                      WHERE r.source_table = 'orders' AND r.row_id = o._row_id)
), valid_items AS (
    SELECT i.* FROM staging.order_items i
    WHERE i.batch_date = %(batch_date)s
      AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                      WHERE r.source_table = 'order_items' AND r.row_id = i._row_id)
), valid_payments AS (
    SELECT y.* FROM staging.payments y
    WHERE y.batch_date = %(batch_date)s
      AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                      WHERE r.source_table = 'payments' AND r.row_id = y._row_id)
), checks (check_name, source_value, target_value) AS (
    SELECT 'order_row_count',
           (SELECT count(*) FROM valid_orders),
           (SELECT count(*) FROM dw.fact_orders WHERE batch_date = %(batch_date)s)
    UNION ALL
    SELECT 'delivery_fee_ghs',
           (SELECT coalesce(sum(delivery_fee::numeric(12, 2)), 0) FROM valid_orders),
           (SELECT coalesce(sum(delivery_fee), 0) FROM dw.fact_orders WHERE batch_date = %(batch_date)s)
    UNION ALL
    SELECT 'sales_row_count',
           (SELECT count(*) FROM valid_items),
           (SELECT count(*) FROM dw.fact_sales WHERE batch_date = %(batch_date)s)
    UNION ALL
    SELECT 'sales_amount_ghs',
           (SELECT coalesce(sum(quantity::int * unit_price::numeric(12, 2)), 0) FROM valid_items),
           (SELECT coalesce(sum(line_amount), 0) FROM dw.fact_sales WHERE batch_date = %(batch_date)s)
    UNION ALL
    SELECT 'payment_row_count',
           (SELECT count(*) FROM valid_payments),
           (SELECT count(*) FROM dw.fact_payments WHERE batch_date = %(batch_date)s)
    UNION ALL
    SELECT 'payment_amount_ghs',
           (SELECT coalesce(sum(amount::numeric(12, 2)), 0) FROM valid_payments),
           (SELECT coalesce(sum(amount), 0) FROM dw.fact_payments WHERE batch_date = %(batch_date)s)
)
INSERT INTO audit.reconciliation (batch_date, check_name, source_value, target_value, passed)
SELECT %(batch_date)s, check_name, source_value, target_value, source_value = target_value
FROM checks;
