-- Rebuild this batch's payments (delete + insert keeps re-runs idempotent).
-- The customer version is the one valid on the batch date (Type 2 history).
DELETE FROM dw.fact_payments WHERE batch_date = %(batch_date)s;

INSERT INTO dw.fact_payments
    (payment_id, order_id, date_key, paid_at, customer_key, payment_method,
     amount, payment_status, batch_date)
SELECT y.payment_id,
       y.order_id,
       to_char(y.paid_at::timestamp, 'YYYYMMDD')::int,
       y.paid_at::timestamp,
       c.customer_key,
       y.payment_method,
       y.amount::numeric(12, 2),
       y.payment_status,
       y.batch_date
FROM staging.payments y
JOIN staging.orders o
  ON o.batch_date = y.batch_date AND o.order_id = y.order_id
 AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                 WHERE r.source_table = 'orders' AND r.row_id = o._row_id)
JOIN dw.dim_customer c
  ON c.customer_id = trim(o.customer_id)
 AND y.batch_date BETWEEN c.valid_from AND c.valid_to
WHERE y.batch_date = %(batch_date)s
  AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                  WHERE r.source_table = 'payments' AND r.row_id = y._row_id);
