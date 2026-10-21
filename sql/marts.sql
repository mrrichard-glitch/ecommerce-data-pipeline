-- Reporting views for Metabase / Power BI. Revenue counts completed orders only.
-- revenue_ghs is goods sold; delivery fees are reported separately, so
-- revenue_ghs + delivery_fees_ghs is what customers paid for completed orders.
-- New columns go at the end: CREATE OR REPLACE VIEW cannot reorder existing ones.

CREATE OR REPLACE VIEW mart.daily_sales AS
WITH fees AS (
    SELECT date_key, sum(delivery_fee) AS delivery_fees_ghs
    FROM dw.fact_orders
    WHERE order_status = 'completed'
    GROUP BY date_key
)
SELECT d.full_date,
       d.day_name,
       d.is_weekend,
       count(DISTINCT f.order_id)                                       AS orders,
       sum(f.quantity)                                                  AS units_sold,
       sum(f.line_amount)                                               AS revenue_ghs,
       round(sum(f.line_amount) / nullif(count(DISTINCT f.order_id), 0), 2) AS avg_order_value_ghs,
       coalesce(max(fees.delivery_fees_ghs), 0)                         AS delivery_fees_ghs
FROM dw.fact_sales f
JOIN dw.dim_date d USING (date_key)
LEFT JOIN fees USING (date_key)
WHERE f.order_status = 'completed'
GROUP BY d.full_date, d.day_name, d.is_weekend;

-- Uses the customer version valid at the time of the sale (Type 2 history).
CREATE OR REPLACE VIEW mart.sales_by_region AS
SELECT d.year, d.month, d.month_name,
       c.region,
       count(DISTINCT f.order_id)          AS orders,
       count(DISTINCT c.customer_id)       AS customers,
       sum(f.line_amount)                  AS revenue_ghs
FROM dw.fact_sales f
JOIN dw.dim_date d     USING (date_key)
JOIN dw.dim_customer c USING (customer_key)
WHERE f.order_status = 'completed'
GROUP BY d.year, d.month, d.month_name, c.region;

CREATE OR REPLACE VIEW mart.product_performance AS
SELECT p.category,
       p.product_name,
       sum(f.quantity)                                              AS units_sold,
       sum(f.line_amount)                                           AS revenue_ghs,
       rank() OVER (ORDER BY sum(f.line_amount) DESC)               AS revenue_rank,
       round(100.0 * sum(f.line_amount)
             / sum(sum(f.line_amount)) OVER (), 2)                  AS revenue_share_pct
FROM dw.fact_sales f
JOIN dw.dim_product p USING (product_key)
WHERE f.order_status = 'completed'
GROUP BY p.category, p.product_name;

CREATE OR REPLACE VIEW mart.payment_method_mix AS
SELECT payment_method,
       count(*)                                                        AS payments,
       count(*) FILTER (WHERE payment_status = 'success')              AS successful,
       round(100.0 * count(*) FILTER (WHERE payment_status = 'success')
             / count(*), 1)                                            AS success_rate_pct,
       sum(amount) FILTER (WHERE payment_status = 'success')           AS collected_ghs
FROM dw.fact_payments
GROUP BY payment_method;

CREATE OR REPLACE VIEW mart.order_status_daily AS
SELECT d.full_date,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'completed') AS completed,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'pending')   AS pending,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'cancelled') AS cancelled,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'returned')  AS returned
FROM dw.fact_sales f
JOIN dw.dim_date d USING (date_key)
GROUP BY d.full_date;

CREATE OR REPLACE VIEW mart.data_quality_daily AS
SELECT q.batch_date,
       q.source_table,
       q.total_rows,
       q.rejected_rows,
       round(100 * q.reject_rate, 2) AS reject_rate_pct,
       bool_and(r.passed)            AS reconciliation_passed
FROM audit.dq_results q
LEFT JOIN audit.reconciliation r USING (batch_date)
GROUP BY q.batch_date, q.source_table, q.total_rows, q.rejected_rows, q.reject_rate;
