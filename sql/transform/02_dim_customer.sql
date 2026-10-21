-- Customers: Type 2 history on region/city, Type 1 overwrite on name/email/phone.
--
-- Each staged customer is compared with the version valid ON THE BATCH DATE, not
-- simply the current one. Loading days in order behaves like a classic SCD2, and
-- re-running or back-filling an older day slots into the right place in history
-- instead of overwriting newer versions.

-- 1. Cleanse the valid staged customers.
CREATE TEMP TABLE tmp_clean_customers ON COMMIT DROP AS
SELECT trim(s.customer_id)                                   AS customer_id,
       initcap(trim(s.full_name))                            AS full_name,
       CASE WHEN s.email ~* '^[^@[:space:]]+@[^@[:space:]]+\.[a-z]{2,}$'
            THEN lower(trim(s.email)) END                    AS email,   -- bad emails become NULL
       regexp_replace(s.phone, '[^0-9]', '', 'g')            AS phone,
       trim(s.region)                                        AS region,
       trim(s.city)                                          AS city,
       s.signup_date::date                                   AS signup_date
FROM staging.customers s
WHERE s.batch_date = %(batch_date)s
  AND NOT EXISTS (SELECT 1 FROM staging.rejected_records r
                  WHERE r.source_table = 'customers' AND r.row_id = s._row_id);

-- 2. Pair each customer with the version covering the batch date (if any) and with
--    their earliest version (used when the batch date is before all of them).
CREATE TEMP TABLE tmp_customer_match ON COMMIT DROP AS
SELECT c.*,
       v.customer_key AS v_key, v.valid_from AS v_from, v.valid_to AS v_to,
       v.is_current AS v_current, v.email AS v_email,
       (v.region, v.city) IS DISTINCT FROM (c.region, c.city) AS v_changed,
       e.customer_key AS e_key, e.valid_from AS e_from, e.email AS e_email,
       (e.region, e.city) IS DISTINCT FROM (c.region, c.city) AS e_changed
FROM tmp_clean_customers c
LEFT JOIN dw.dim_customer v
       ON v.customer_id = c.customer_id
      AND %(batch_date)s::date BETWEEN v.valid_from AND v.valid_to
LEFT JOIN LATERAL (
    SELECT * FROM dw.dim_customer d
    WHERE d.customer_id = c.customer_id
    ORDER BY d.valid_from LIMIT 1
) e ON v.customer_key IS NULL;

-- 3. Type 1 attributes (name, email, phone): overwrite the current row in place,
--    unless this batch is older than the current version (a back-fill must not
--    replace newer details). A NULL (malformed) email never overwrites a good one.
UPDATE dw.dim_customer d
SET full_name = c.full_name,
    email     = coalesce(c.email, d.email),
    phone     = c.phone
FROM tmp_clean_customers c
WHERE d.customer_id = c.customer_id
  AND d.is_current
  AND d.valid_from <= %(batch_date)s::date
  AND (d.full_name, d.email, d.phone) IS DISTINCT FROM (c.full_name, coalesce(c.email, d.email), c.phone);

-- 4. Region/city changed on the very day a version starts: correct it in place.
UPDATE dw.dim_customer d
SET region = m.region, city = m.city
FROM tmp_customer_match m
WHERE d.customer_key = m.v_key AND m.v_changed AND m.v_from = %(batch_date)s::date;

-- 5. Region/city changed part-way through a version: close it the day before the
--    batch, then open a version from the batch date to where the old one ended.
UPDATE dw.dim_customer d
SET valid_to = %(batch_date)s::date - 1,
    is_current = false
FROM tmp_customer_match m
WHERE d.customer_key = m.v_key AND m.v_changed AND m.v_from < %(batch_date)s::date;

INSERT INTO dw.dim_customer
    (customer_id, full_name, email, phone, region, city, signup_date, valid_from, valid_to, is_current)
SELECT m.customer_id, m.full_name, coalesce(m.email, m.v_email), m.phone, m.region, m.city,
       m.signup_date, %(batch_date)s::date, m.v_to, m.v_current
FROM tmp_customer_match m
WHERE m.v_changed AND m.v_from < %(batch_date)s::date;

-- 6. No version covers the batch date, so the batch is earlier than everything
--    loaded so far (or the customer is new). If nothing changed, stretch the
--    earliest version back; otherwise open a version that ends where it begins.
UPDATE dw.dim_customer d
SET valid_from = %(batch_date)s::date
FROM tmp_customer_match m
WHERE d.customer_key = m.e_key AND NOT m.e_changed;

INSERT INTO dw.dim_customer
    (customer_id, full_name, email, phone, region, city, signup_date, valid_from, valid_to, is_current)
SELECT m.customer_id, m.full_name, coalesce(m.email, m.e_email), m.phone, m.region, m.city,
       m.signup_date, %(batch_date)s::date,
       coalesce(m.e_from - 1, '9999-12-31'::date),
       m.e_key IS NULL
FROM tmp_customer_match m
WHERE m.v_key IS NULL AND (m.e_key IS NULL OR m.e_changed);
