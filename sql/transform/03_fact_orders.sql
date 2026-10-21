-- Rebuild this batch's orders (delete + insert keeps re-runs idempotent).
-- The customer version is the one valid on the batch date (Type 2 history).
DELETE FROM dw.fact_orders WHERE batch_date = %(batch_date)s;

INSERT INTO dw.fact_orders
    (order_id, date_key, order_ts, customer_key, order_status, payment_method,
     delivery_fee, batch_date)
SELECT o.order_id,
       to_char(o.order_ts::timestamp, 'YYYYMMDD')::int,
       o.order_ts::timestamp,
       c.customer_key,
       o.status,
       pay.payment_method,
       o.delivery_fee::numeric(12, 2),
       o.batch_date
FROM staging.orders o
JOIN dw.dim_customer c
  ON c.customer_id = trim(o.customer_id)
 AND o.batch_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN LATERAL (
    SELECT y.payment_method
    FROM staging.payments y
    WHERE y.batch_date = o.batch_date AND y.order_id = o.order_id
      AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                      WHERE r.source_table = 'payments' AND r.row_id = y._row_id)
    ORDER BY y._row_id
    LIMIT 1
) pay ON true
WHERE o.batch_date = %(batch_date)s
  AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                  WHERE r.source_table = 'orders' AND r.row_id = o._row_id);
