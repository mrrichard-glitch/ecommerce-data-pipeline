"""Runtime settings, read from environment variables with local defaults."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

WAREHOUSE_DSN = os.getenv(
    "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5433/warehouse"
)
RAW_DIR = Path(os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data")) / "raw"
SQL_DIR = Path(os.getenv("PIPELINE_SQL_DIR", PROJECT_ROOT / "sql"))

# A batch fails if more than this share of any table's rows is rejected.
MAX_REJECT_RATE = float(os.getenv("MAX_REJECT_RATE", "0.10"))

# Staged rows, their reject log and raw CSVs are kept this many days before the
# batch date, then purged. Summary counts stay in audit.dq_results for good.
STAGING_RETENTION_DAYS = int(os.getenv("STAGING_RETENTION_DAYS", "30"))
