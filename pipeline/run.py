"""Run the pipeline without Airflow, for local development and CI.

    python -m pipeline.run --start 2026-10-01 --end 2026-10-07

Each step commits on its own, mirroring how the Airflow tasks behave.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pipeline import config, generate, quality, staging, warehouse
from pipeline.db import connect

log = logging.getLogger("pipeline")


def run_batch(batch_date: date, dsn: str = config.WAREHOUSE_DSN) -> None:
    log.info("=== Batch %s ===", batch_date)
    files = generate.extract(batch_date, config.RAW_DIR)

    with connect(dsn) as conn:
        loaded = staging.load_to_staging(conn, batch_date, files)
    log.info("Staged: %s", loaded)

    with connect(dsn) as conn:
        results = quality.run_checks(conn, batch_date)
    log.info("Data quality:\n%s", quality.summarise(results))
    quality.enforce_thresholds(results, config.MAX_REJECT_RATE)

    with connect(dsn) as conn:
        warehouse.load_dimensions(conn, batch_date)
    with connect(dsn) as conn:
        warehouse.load_facts(conn, batch_date)
    with connect(dsn) as conn:
        checks = warehouse.reconcile(conn, batch_date)
    log.info("Reconciliation passed: %s", ", ".join(f"{c[0]}={c[2]}" for c in checks))

    cutoff = batch_date - timedelta(days=config.STAGING_RETENTION_DAYS)
    with connect(dsn) as conn:
        staging.purge_old_batches(conn, cutoff)
    generate.purge_raw(config.RAW_DIR, cutoff)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, help="inclusive; defaults to --start")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with connect(config.WAREHOUSE_DSN) as conn:
        warehouse.ensure_schema(conn)

    day = args.start
    while day <= (args.end or args.start):
        run_batch(day)
        day += timedelta(days=1)

    with connect(config.WAREHOUSE_DSN) as conn:
        warehouse.refresh_marts(conn)
    log.info("Marts refreshed.")


if __name__ == "__main__":
    main()
