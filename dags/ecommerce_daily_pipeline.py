"""
### Ghana e-commerce daily pipeline

Moves one day of shop data from source to reporting:

1. **extract** – land the day's orders, items, payments, customers and products as CSV
2. **load_staging** – copy raw values into `staging` as text (re-runnable per batch)
3. **validate** – apply data quality rules; log rejects; fail if any table exceeds the reject limit
4. **load_dimensions** – `dim_product` (Type 1) and `dim_customer` (Type 2)
5. **load_facts** – `fact_orders` (order grain), `fact_sales` (order-line grain) and `fact_payments`
6. **reconcile** – staging vs warehouse row counts and GH₵ totals must match
7. **refresh_marts** – reporting views for Metabase / Power BI
8. **purge_old_batches** – drop staged rows and raw CSVs older than the retention window

Every task is idempotent, so any day can be cleared and re-run safely, including
days older than ones already loaded. Data quality and reconciliation failures fail
the task without retrying, since the same data would fail again.

`extract` hands file paths to `load_staging`, so `PIPELINE_DATA_DIR` must be storage
every worker can read: a local folder with the LocalExecutor used here, a shared
volume or object store with Celery or Kubernetes workers.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from pipeline import config, generate, quality, staging, warehouse
from pipeline.db import connect


@dag(
    dag_id="ecommerce_daily_pipeline",
    description="Source -> staging -> validated star schema -> reporting marts",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,  # batches share the customer dimension, so never load two at once
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ecommerce", "etl", "postgres"],
    doc_md=__doc__,
)
def ecommerce_daily_pipeline():
    @task
    def ensure_schema() -> None:
        with connect(config.WAREHOUSE_DSN) as conn:
            warehouse.ensure_schema(conn)

    @task
    def extract(ds: str | None = None) -> dict[str, str]:
        return generate.extract(date.fromisoformat(ds), config.RAW_DIR)

    @task
    def load_staging(files: dict[str, str], ds: str | None = None) -> dict[str, int]:
        with connect(config.WAREHOUSE_DSN) as conn:
            return staging.load_to_staging(conn, date.fromisoformat(ds), files)

    @task
    def validate(ds: str | None = None) -> dict[str, float]:
        with connect(config.WAREHOUSE_DSN) as conn:
            results = quality.run_checks(conn, date.fromisoformat(ds))
        print(quality.summarise(results))
        try:
            quality.enforce_thresholds(results, config.MAX_REJECT_RATE)
        except quality.DataQualityError as exc:
            raise AirflowFailException(str(exc)) from exc
        return {r.table: round(r.reject_rate, 4) for r in results}

    @task
    def load_dimensions(ds: str | None = None) -> None:
        with connect(config.WAREHOUSE_DSN) as conn:
            warehouse.load_dimensions(conn, date.fromisoformat(ds))

    @task
    def load_facts(ds: str | None = None) -> None:
        with connect(config.WAREHOUSE_DSN) as conn:
            warehouse.load_facts(conn, date.fromisoformat(ds))

    @task
    def reconcile(ds: str | None = None) -> None:
        try:
            with connect(config.WAREHOUSE_DSN) as conn:
                checks = warehouse.reconcile(conn, date.fromisoformat(ds))
        except warehouse.ReconciliationError as exc:
            raise AirflowFailException(str(exc)) from exc
        for name, source, target, _ in checks:
            print(f"{name}: staging={source} warehouse={target} OK")

    @task
    def refresh_marts() -> None:
        with connect(config.WAREHOUSE_DSN) as conn:
            warehouse.refresh_marts(conn)

    @task
    def purge_old_batches(ds: str | None = None) -> None:
        cutoff = date.fromisoformat(ds) - timedelta(days=config.STAGING_RETENTION_DAYS)
        with connect(config.WAREHOUSE_DSN) as conn:
            print(f"Staging rows purged before {cutoff}: {staging.purge_old_batches(conn, cutoff)}")
        print(f"Raw folders purged: {generate.purge_raw(config.RAW_DIR, cutoff)}")

    files = extract()
    ensure_schema() >> files
    (
        load_staging(files)
        >> validate()
        >> load_dimensions()
        >> load_facts()
        >> reconcile()
        >> refresh_marts()
        >> purge_old_batches()
    )


ecommerce_daily_pipeline()
