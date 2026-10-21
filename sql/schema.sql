-- Idempotent schema setup: safe to run before every pipeline run.

CREATE SCHEMA IF NOT EXISTS staging;  -- raw source values, kept as text
CREATE SCHEMA IF NOT EXISTS dw;       -- cleaned star schema
CREATE SCHEMA IF NOT EXISTS audit;    -- data quality and reconciliation results
CREATE SCHEMA IF NOT EXISTS mart;     -- reporting views for BI tools

-- ---------------------------------------------------------------- staging
CREATE TABLE IF NOT EXISTS staging.customers (
    _row_id bigserial PRIMARY KEY, batch_date date NOT NULL,
    customer_id text, full_name text, email text, phone text,
    region text, city text, signup_date text,
    _loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.products (
    _row_id bigserial PRIMARY KEY, batch_date date NOT NULL,
    product_id text, product_name text, category text, unit_price text,
    _loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.orders (
    _row_id bigserial PRIMARY KEY, batch_date date NOT NULL,
    order_id text, customer_id text, order_ts text, status text, delivery_fee text,
    _loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.order_items (
    _row_id bigserial PRIMARY KEY, batch_date date NOT NULL,
    order_id text, line_number text, product_id text, quantity text, unit_price text,
    _loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.payments (
    _row_id bigserial PRIMARY KEY, batch_date date NOT NULL,
    payment_id text, order_id text, payment_method text, amount text,
    payment_status text, paid_at text,
    _loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staging.rejected_records (
    batch_date date NOT NULL,
    source_table text NOT NULL,
    row_id bigint NOT NULL,
    rule text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stg_customers_batch ON staging.customers (batch_date, customer_id);
CREATE INDEX IF NOT EXISTS ix_stg_products_batch ON staging.products (batch_date, product_id);
CREATE INDEX IF NOT EXISTS ix_stg_orders_batch ON staging.orders (batch_date, order_id);
CREATE INDEX IF NOT EXISTS ix_stg_items_batch ON staging.order_items (batch_date, order_id);
CREATE INDEX IF NOT EXISTS ix_stg_payments_batch ON staging.payments (batch_date, order_id);
CREATE INDEX IF NOT EXISTS ix_rejected_lookup ON staging.rejected_records (source_table, row_id);

-- ---------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key int PRIMARY KEY,          -- yyyymmdd
    full_date date NOT NULL UNIQUE,
    day_name text NOT NULL,
    day_of_week smallint NOT NULL,     -- 1 = Monday
    month smallint NOT NULL,
    month_name text NOT NULL,
    quarter smallint NOT NULL,
    year smallint NOT NULL,
    is_weekend boolean NOT NULL
);
-- Runs before every batch, so the calendar always reaches three full years ahead.
INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d, to_char(d, 'FMDay'), extract(isodow FROM d),
       extract(month FROM d), to_char(d, 'FMMonth'), extract(quarter FROM d),
       extract(year FROM d), extract(isodow FROM d) >= 6
FROM generate_series('2024-01-01'::date,
                     (date_trunc('year', current_date) + interval '4 years - 1 day')::date,
                     interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Type 1: product changes overwrite the row.
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key serial PRIMARY KEY,
    product_id text NOT NULL UNIQUE,
    product_name text NOT NULL,
    category text NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Type 2: a change of region or city closes the old row and opens a new one,
-- so past sales stay attributed to where the customer lived at the time.
-- Versions of one customer never overlap and leave no gaps after the first.
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key serial PRIMARY KEY,
    customer_id text NOT NULL,
    full_name text NOT NULL,
    email text,
    phone text,
    region text NOT NULL,
    city text NOT NULL,
    signup_date date,
    valid_from date NOT NULL,
    valid_to date NOT NULL DEFAULT '9999-12-31',
    is_current boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
    ON dw.dim_customer (customer_id) WHERE is_current;

-- ---------------------------------------------------------------- facts
-- Grain: one row per order. Holds order-level amounts such as the delivery fee,
-- which would be double counted if spread across order lines.
CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_id text PRIMARY KEY,
    date_key int NOT NULL REFERENCES dw.dim_date,
    order_ts timestamp NOT NULL,
    customer_key int NOT NULL REFERENCES dw.dim_customer,
    order_status text NOT NULL,
    payment_method text,
    delivery_fee numeric(12, 2) NOT NULL,
    batch_date date NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_batch ON dw.fact_orders (batch_date);

-- Grain: one row per order line.
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_key bigserial PRIMARY KEY,
    order_id text NOT NULL,
    line_number int NOT NULL,
    date_key int NOT NULL REFERENCES dw.dim_date,
    order_ts timestamp NOT NULL,
    customer_key int NOT NULL REFERENCES dw.dim_customer,
    product_key int NOT NULL REFERENCES dw.dim_product,
    quantity int NOT NULL CHECK (quantity > 0),
    unit_price numeric(12, 2) NOT NULL,
    line_amount numeric(12, 2) NOT NULL,
    order_status text NOT NULL,
    payment_method text,
    batch_date date NOT NULL,
    UNIQUE (order_id, line_number)
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_batch ON dw.fact_sales (batch_date);

-- Grain: one row per payment.
CREATE TABLE IF NOT EXISTS dw.fact_payments (
    payment_id text PRIMARY KEY,
    order_id text NOT NULL,
    date_key int NOT NULL REFERENCES dw.dim_date,
    paid_at timestamp NOT NULL,
    customer_key int NOT NULL REFERENCES dw.dim_customer,
    payment_method text NOT NULL,
    amount numeric(12, 2) NOT NULL,
    payment_status text NOT NULL,
    batch_date date NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_payments_batch ON dw.fact_payments (batch_date);

-- ---------------------------------------------------------------- audit
CREATE TABLE IF NOT EXISTS audit.dq_results (
    batch_date date NOT NULL,
    source_table text NOT NULL,
    total_rows int NOT NULL,
    rejected_rows int NOT NULL,
    reject_rate numeric(6, 4) NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_date, source_table)
);
CREATE TABLE IF NOT EXISTS audit.reconciliation (
    batch_date date NOT NULL,
    check_name text NOT NULL,
    source_value numeric NOT NULL,
    target_value numeric NOT NULL,
    passed boolean NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_date, check_name)
);
