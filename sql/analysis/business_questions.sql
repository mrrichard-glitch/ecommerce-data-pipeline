-- Analyst queries on the warehouse: questions a Ghanaian e-commerce business would ask.
-- Run them in Metabase, Power BI (as native queries) or psql.

-- 1. How is revenue trending week on week?
WITH weekly AS (
    SELECT date_trunc('week', full_date)::date AS week_start,
           sum(revenue_ghs)                    AS revenue_ghs
    FROM mart.daily_sales
    GROUP BY 1
)
SELECT week_start,
       revenue_ghs,
       round(100.0 * (revenue_ghs - lag(revenue_ghs) OVER (ORDER BY week_start))
             / nullif(lag(revenue_ghs) OVER (ORDER BY week_start), 0), 1) AS wow_change_pct
FROM weekly
ORDER BY week_start;

-- 2. Do weekends sell more, and do people spend more per order?
SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       round(avg(orders), 1)              AS avg_orders_per_day,
       round(avg(revenue_ghs), 2)         AS avg_revenue_per_day_ghs,
       round(avg(avg_order_value_ghs), 2) AS avg_order_value_ghs
FROM mart.daily_sales
GROUP BY 1;

-- 3. Which payment methods fail or get cancelled most? (MoMo vs card vs cash on delivery)
SELECT payment_method,
       count(*) AS payments,
       round(100.0 * count(*) FILTER (WHERE payment_status IN ('failed', 'refunded')) / count(*), 1)
           AS failed_or_refunded_pct
FROM dw.fact_payments
GROUP BY payment_method
ORDER BY failed_or_refunded_pct DESC;

-- 4. Repeat customers: what share of customers ordered more than once, and what do they bring in?
WITH per_customer AS (
    SELECT c.customer_id, count(DISTINCT f.order_id) AS orders, sum(f.line_amount) AS revenue_ghs
    FROM dw.fact_sales f
    JOIN dw.dim_customer c USING (customer_key)
    WHERE f.order_status = 'completed'
    GROUP BY c.customer_id
)
SELECT CASE WHEN orders > 1 THEN 'Repeat' ELSE 'One-time' END AS customer_type,
       count(*)                                                AS customers,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)      AS share_of_customers_pct,
       round(100.0 * sum(revenue_ghs) / sum(sum(revenue_ghs)) OVER (), 1) AS share_of_revenue_pct
FROM per_customer
GROUP BY 1;

-- 5. Top 3 products in each category.
SELECT category, product_name, revenue_ghs
FROM (
    SELECT category, product_name, revenue_ghs,
           row_number() OVER (PARTITION BY category ORDER BY revenue_ghs DESC) AS rn
    FROM mart.product_performance
) ranked
WHERE rn <= 3
ORDER BY category, revenue_ghs DESC;

-- 6. Regional reach outside Greater Accra (delivery planning).
--    Share is of revenue from outside Greater Accra.
SELECT region,
       sum(orders)                                                      AS orders,
       round(100.0 * sum(revenue_ghs) / sum(sum(revenue_ghs)) OVER (), 1) AS revenue_share_pct
FROM mart.sales_by_region
WHERE region <> 'Greater Accra'
GROUP BY region
ORDER BY orders DESC;

-- 7. Customers who moved region (Type 2 history in action).
SELECT customer_id, region, city, valid_from, valid_to, is_current
FROM dw.dim_customer
WHERE customer_id IN (SELECT customer_id FROM dw.dim_customer WHERE NOT is_current)
ORDER BY customer_id, valid_from;
