# Ghana E-commerce Data Pipeline

An end-to-end batch data pipeline for a Ghanaian online shop. Apache Airflow moves each day's orders, payments, customers and products from the source system through a validated **staging → star schema → reporting** flow in PostgreSQL. The result is analytics-ready data for Metabase or Power BI.

The data reflects how Ghanaians actually shop online: MTN MoMo, Telecel Cash and AirtelTigo Money payments, cash on delivery, prices in GH₵ and customers across seven regions. About 3% of source records are deliberately broken, so the pipeline has to catch them.

> This project is a public replica of the architecture I use in production for institutional reporting. That code and its data are private, so everything here runs on generated data.

## Architecture

```mermaid
flowchart LR
    A[Source system<br/>daily CSV extract] --> B[staging<br/>raw text, per batch]
    B --> C{Data quality rules}
    C -- bad rows --> R[(rejected_records<br/>+ audit.dq_results)]
    C -- valid rows --> D[dw.dim_customer · SCD2<br/>dw.dim_product · SCD1<br/>dw.dim_date]
    D --> F[dw.fact_orders<br/>dw.fact_sales<br/>dw.fact_payments]
    F --> G{Reconciliation<br/>staging vs warehouse}
    G --> M[mart views]
    M --> BI[Metabase / Power BI]
```

The Airflow DAG `ecommerce_daily_pipeline` runs daily with these tasks:

`ensure_schema → extract → load_staging → validate → load_dimensions → load_facts → reconcile → refresh_marts → purge_old_batches`

## What it demonstrates

| Area | How |
| --- | --- |
| Orchestration | Airflow TaskFlow DAG, daily schedule, catch-up backfill, one run at a time (`max_active_runs=1`); retries for transient errors, while quality and reconciliation failures fail fast |
| Idempotency | Every step deletes and rebuilds its own batch, so any day can be re-run without duplicates, even after later days have loaded (tested) |
| Staging design | Raw values land as text with batch date and load time, so nothing is lost to a bad cast before validation |
| Data quality | 27 SQL rules across 5 tables cover every column the warehouse requires; rejected rows are logged with the rule they broke, not deleted |
| Quality gate | The batch fails if any table rejects more than 10% of rows (configurable) |
| Dimensional modelling | Star schema at order and order-line grain; Type 2 history on customer region/city that handles re-runs and back-fills, Type 1 on products |
| Cleansing | Name casing, phone normalisation, malformed emails nulled without overwriting a good one |
| Reconciliation | Row counts and GH₵ totals must match between validated staging and the warehouse, or the run fails |
| Analytics | Reporting views plus business-question SQL using CTEs and window functions |
| Engineering practice | Docker Compose, unit and integration tests, GitHub Actions CI against a real PostgreSQL |

## Data model

| Table | Grain / purpose |
| --- | --- |
| `dw.fact_orders` | One row per order: status, payment method, delivery fee |
| `dw.fact_sales` | One row per order line: quantity, price, line amount, order status, payment method |
| `dw.fact_payments` | One row per payment: method, amount, status |
| `dw.dim_customer` | Type 2: a new version when a customer moves region, with `valid_from` / `valid_to` / `is_current` |
| `dw.dim_product` | Type 1: 24 products in 6 categories, overwritten on change |
| `dw.dim_date` | Calendar from 2024, extended to three years ahead on every run, with weekday, month, quarter and weekend flag |
| `audit.dq_results` | Rows checked and rejected per table per day |
| `audit.reconciliation` | Each staging-vs-warehouse check per day and whether it passed |

Reporting views in `mart`: `daily_sales`, `sales_by_region`, `product_performance`, `payment_method_mix`, `order_status_daily` and `data_quality_daily`.

Because of the Type 2 history, every fact row points at the customer version valid on its batch date, so `mart.sales_by_region` credits each sale to the region the customer lived in *at the time*, not where they live now.

In `mart.daily_sales`, `revenue_ghs` is goods sold and `delivery_fees_ghs` is shown separately; together they make up what customers paid for completed orders.

## Data quality rules

| Table | Rules |
| --- | --- |
| customers | missing id, missing name/region/city, invalid signup date, duplicate customer |
| products | missing id, missing name/category, invalid price, duplicate product |
| orders | missing id, missing customer, unknown or rejected customer, invalid timestamp, unknown status, invalid delivery fee, duplicate order |
| order_items | duplicate line, invalid line number, zero/negative quantity, invalid price, unknown product, parent order rejected |
| payments | missing id, missing method/status, non-numeric amount, invalid timestamp, duplicate payment, parent order rejected |

Dates and timestamps must have the expected format *and* be real calendar values, so `2026-13-45` is rejected rather than failing the load. Ids are compared with surrounding spaces trimmed, the same way the warehouse stores them.

Rules run parents first, so an order line is rejected when its order was rejected. A typical day's log looks like this:

```
customers      133 rows,   1 rejected (duplicate_customer=1)
products        24 rows,   0 rejected
orders         144 rows,   4 rejected (missing_customer_id=4)
order_items    259 rows,  16 rejected (duplicate_line=4, invalid_quantity=4, unknown_product=2, parent_order_rejected=6)
payments       144 rows,   8 rejected (invalid_amount=4, parent_order_rejected=4)
order_row_count: staging=140 warehouse=140 OK
delivery_fee_ghs: staging=2655.00 warehouse=2655.00 OK
sales_row_count: staging=243 warehouse=243 OK
sales_amount_ghs: staging=177731.00 warehouse=177731.00 OK
```

## Run it

You need [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
git clone https://github.com/mrrichard-glitch/ghana-ecommerce-data-pipeline.git
cd ghana-ecommerce-data-pipeline
docker compose up -d
```

1. Open Airflow at http://localhost:8080 and log in with `admin` / `admin`. The DAG is unpaused and backfills daily from 1 Oct 2026. If port 8080 is taken, start with `AIRFLOW_PORT=8081 docker compose up -d` and use that port instead.
2. Query the warehouse on `localhost:5433` (database, user and password are all `warehouse`), or run `docker compose exec postgres psql -U warehouse -d warehouse`.
3. Optional: start Metabase with `docker compose --profile bi up -d` and open http://localhost:3000. Add a PostgreSQL connection with host `postgres`, port `5432`, and `warehouse` for the database, user and password.

On Linux, run `echo "AIRFLOW_UID=$(id -u)" > .env` before the first `docker compose up` so Airflow can write to `./data`.

Airflow runs as separate `airflow-webserver` and `airflow-scheduler` services, after a one-off `airflow-init` that sets up its database. All passwords here are local demo defaults; change them before exposing anything beyond your machine.

**Without Airflow** (Python 3.10+ and PostgreSQL 16+):

```bash
pip install -r requirements.txt
export WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse
python -m pipeline.run --start 2026-10-01 --end 2026-10-07
pytest -v
```

## Analysis

`sql/analysis/business_questions.sql` answers the questions an e-commerce team asks:

- weekly revenue trend with week-on-week change
- weekday vs weekend demand and order value
- failure and refund rates by payment method (MoMo vs card vs cash on delivery)
- repeat vs one-time customers and their share of revenue
- top 3 products per category
- regional reach outside Greater Accra
- customers who moved region (the SCD2 history)

<!-- Add a screenshot of your Metabase or Power BI dashboard here:
![Dashboard](docs/dashboard.png) -->

## Project structure

```
dags/ecommerce_daily_pipeline.py   Airflow DAG
pipeline/
  generate.py                      source data generator (deterministic, with dirty records)
  staging.py                       CSV -> staging via COPY
  quality.py                       data quality rules, reject logging, threshold gate
  warehouse.py                     dimension/fact loads, reconciliation, marts
  run.py                           run without Airflow (local and CI)
sql/
  schema.sql                       staging, dw, audit and mart schemas
  transform/                       01 dim_product · 02 dim_customer · 03 fact_orders · 04 fact_sales · 05 fact_payments · 06 reconcile
  marts.sql                        reporting views
  analysis/business_questions.sql  analyst queries
tests/                             unit tests + end-to-end integration tests
docker-compose.yml                 PostgreSQL 16, Airflow 2.9, Metabase
```

## Design notes

- **Why text in staging?** A failed cast at load time loses the row and the evidence. Keeping raw text means every bad value is still there to inspect, and the rejection log says which rule it broke.
- **Why reject instead of delete?** Analysts can always answer "why is this order missing from the dashboard?" by querying `staging.rejected_records`.
- **Re-runs and back-fills.** Each staged customer is compared with the version valid *on the batch date*, not just the current one. A re-run of an older day therefore leaves later history alone, and a back-fill before the first load either stretches the earliest version back or adds an earlier one. Versions never overlap.
- **Why `max_active_runs=1`?** Every batch writes to the same customer dimension, so two batches must never load at once.
- **Retention.** Staged rows, their reject log and raw CSVs are purged after `STAGING_RETENTION_DAYS` (default 30) relative to the batch date. Per-day counts stay in `audit.dq_results`, and a purged day can still be re-run because extraction is reproducible.
- **Scaling out.** `extract` passes file paths to `load_staging`, so `PIPELINE_DATA_DIR` must be readable by every Airflow worker: a local folder with the LocalExecutor used here, or a shared volume or object store with Celery or Kubernetes workers.

## Author

**Richard Kugbadzor**, Data Engineer and Data Analyst, Accra · [LinkedIn](https://linkedin.com/in/richard-kugbadzor-b43b6428b)
