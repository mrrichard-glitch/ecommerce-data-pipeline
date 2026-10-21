-- Rebuild this batch's order lines (delete + insert keeps re-runs idempotent).
-- The customer version is the one valid on the batch date (Type 2 history).
DELETE FROM dw.fact_sales WHERE batch_date = %(batch_date)s;

INSERT INTO dw.fact_sales
    (order_id, line_number, date_key, order_ts, customer_key, product_key,
     quantity, unit_price, line_amount, order_status, payment_method, batch_date)
SELECT i.order_id,
       i.line_number::int,
       to_char(o.order_ts::timestamp, 'YYYYMMDD')::int,
       o.order_ts::timestamp,
       c.customer_key,
       p.product_key,
       i.quantity::int,
       i.unit_price::numeric(12, 2),
       i.quantity::int * i.unit_price::numeric(12, 2),
       o.status,
       pay.payment_method,
       i.batch_date
FROM staging.order_items i
JOIN staging.orders o
  ON o.batch_date = i.batch_date AND o.order_id = i.order_id
 AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                 WHERE r.source_table = 'orders' AND r.row_id = o._row_id)
JOIN dw.dim_product p  ON p.product_id = trim(i.product_id)
JOIN dw.dim_customer c
  ON c.customer_id = trim(o.customer_id)
 AND i.batch_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN LATERAL (
    SELECT y.payment_method
    FROM staging.payments y
    WHERE y.batch_date = i.batch_date AND y.order_id = i.order_id
      AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                      WHERE r.source_table = 'payments' AND r.row_id = y._row_id)
    ORDER BY y._row_id
    LIMIT 1
) pay ON true
WHERE i.batch_date = %(batch_date)s
  AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                  WHERE r.source_table = 'order_items' AND r.row_id = i._row_id);
