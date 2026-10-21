"""Thin PostgreSQL helpers. Each pipeline step runs in one transaction."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg2


@contextmanager
def connect(dsn: str) -> Iterator["psycopg2.extensions.connection"]:
    """Open a connection; commit on success, roll back on any error."""
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_sql_file(conn, path: Path, params: dict | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"), params)
