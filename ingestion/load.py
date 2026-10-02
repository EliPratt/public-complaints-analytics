"""
Load raw CFPB complaint partitions into DuckDB.
 
Reads the daily partitions written by extract.py:
    data/raw/date_received=YYYY-MM-DD/complaints.csv
    data/raw/date_received=YYYY-MM-DD/manifest.json
and loads them into raw.complaints in a local DuckDB file.
 
Design
------
- Raw stays raw. Every source column is loaded as text. The only change is
  converting headers like "Consumer consent provided?" into snake_case
  column names. Typing and cleaning happen in dbt staging models.
- Idempotent by partition. Each day is loaded in one transaction that deletes
  that day's rows and inserts the new file, so re-running or re-pulling a day
  never creates duplicates.
- Incremental. raw._load_log records which extraction of each day was loaded.
  A partition is skipped unless extract.py has written a newer version of it
  (the lookback window re-pulls recent days, so those reload automatically).
- Schema drift. If the CFPB adds a column, it is added to the table and a
  warning is logged. If a column disappears, its values load as NULL.
- Reconciliation. Rows loaded must equal the manifest's row count, or the
  transaction is rolled back and the run fails.
 
Usage
-----
    python ingestion/load.py                       # load new/changed partitions
    python ingestion/load.py --start 2025-09-01    # limit to a date range
    python ingestion/load.py --force               # reload everything in range
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_DB_PATH = Path("data/warehouse/complaints.duckdb")
TABLE = "raw.complaints"
LOAD_LOG = "raw._load_log"
PARTITION_PATTERN = re.compile(r"^date_received=(\d{4}-\d{2}-\d{2})$")

log = logging.getLogger("cfpb.load")

class LoadReconciliationError(Exception):
    """Rows loaded do not match the manifest for a partition."""

@dataclass
class Partition:
    day: date
    csv_path: Path
    extracted_at: str
    row_count: int

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def normalize_column(name: str) -> str:
    """'Consumer consent provided?' -> 'consumer_consent_provided'."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower()).strip("_")
    if not cleaned:
        raise ValueError(f"Column name {name!r} normalizes to nothing")
    return cleaned

def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def discover_partitions(
        raw_dir: Path, start: date | None = None, end: date | None = None
) -> list[Partition]:
    """Find partitions that have both a CSV and a manifest, sorted by date."""
    partitions = []
    for path in sorted(raw_dir.glob("date_received=*")):
        match = PARTITION_PATTERN.match(path.name)
        if not match or not path.is_dir():
            continue
        day = date.fromisoformat(match.group(1))
        if (start and day < start) or (end and day > end):
            continue

        manifest_path = path / "manifest.json"
        csv_path = path / "complaints.csv"
        if not manifest_path.exists() or not csv_path.exists():
            # extract.py writes the manifest last, so a missing one means the pull for this day never finished
            log.warning("%s: incomplete partition, skipping", path.name)
            continue

        manifest = json.loads(manifest_path.read_text())
        partitions.append(
            Partition(day, csv_path, manifest["extracted_at"], int(manifest["row_count"]))
        )
    return partitions

# ----------------------------------------------------------------------
# Database
# ----------------------------------------------------------------------

def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    _partition_date DATE NOT NULL,
                    _source_file VARCHAR NOT NULL,
                    _extracted_at TIMESTAMPTZ NOT NULL,
                    _loaded_at TIMESTAMPTZ NOT NULL
                )                
            """)
    con.execute(f"""
                CREATE TABLE IF NOT EXISTS {LOAD_LOG} (
                    partition_date DATE PRIMARY KEY,
                    extracted_at VARCHAR NOT NULL,
                    row_count INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL
                )
            """)
    
def table_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'complaints'"
    ).fetchall()
    return {r[0] for r in rows}

def file_columns(con: duckdb.DuckDBPyConnection, csv_path: Path) -> list[str]:
    """Original header names from a CSV file.
    hive_partitioning is off because DuckDB would otherwise read the
    date_received=YYYY-MM-DD folder name as an extra column."""
    rel = con.execute(
        f"DESCRIBE SELECT * FROM read_csv({sql_literal(str(csv_path))}, "
        f"header = true, all_varchar = true, hive_partitioning = false)"
    ).fetchall()
    return [r[0] for r in rel]

def sync_schema(con: duckdb.DuckDBPyConnection, normalized: list[str]) -> list[str]:
    """Add any new source columns to the table. Returns the ones added."""
    existing = table_columns(con)
    added = [c for c in normalized if c not in existing]
    for col in added:
        con.execute(f"ALTER TABLE {TABLE} ADD COLUMN {quote_ident(col)} VARCHAR")
    return added

def already_loaded(con: duckdb.DuckDBPyConnection, p: Partition) -> bool:
    row = con.execute(
        f"SELECT extracted_at FROM {LOAD_LOG} WHERE partition_date = ?", [p.day]
    ).fetchone()
    return row is not None and row[0] == p.extracted_at

def load_partition(con: duckdb.DuckDBPyConnection, p: Partition, first_load: bool) -> int:
    """Replace one day's rows in a single transaction. Returns rows loaded."""
    if p.row_count > 0:
        original = file_columns(con, p.csv_path)
        normalized = [normalize_column(c) for c in original]
        if len(set(normalized)) != len(normalized):
            raise ValueError(f"{p.day}: duplicate column names after normalizing: {original}")

    added: list[str] = []
    con.execute("BEGIN TRANSACTION")
    try:
        # Schema changes share the transaction, so a failed load leaves no new columns behind
        if p.row_count > 0:
            added = sync_schema(con, normalized)

        con.execute(f"DELETE FROM {TABLE} WHERE _partition_date = ?", [p.day])
 
        if p.row_count > 0:
            select_list = ", ".join(
                f"{quote_ident(o)} AS {quote_ident(n)}" for o, n in zip(original, normalized)
            )
            con.execute(
                f"""
                INSERT INTO {TABLE} BY NAME
                SELECT
                    {select_list},
                    ?::DATE        AS _partition_date,
                    ?              AS _source_file,
                    ?::TIMESTAMPTZ AS _extracted_at,
                    now()          AS _loaded_at
                FROM read_csv({sql_literal(str(p.csv_path))}, header = true, all_varchar = true, hive_partitioning = false)
                """,
                [p.day, str(p.csv_path), p.extracted_at],
            )
 
        loaded = con.execute(
            f"SELECT count(*) FROM {TABLE} WHERE _partition_date = ?", [p.day]
        ).fetchone()[0]
        if loaded != p.row_count:
            raise LoadReconciliationError(
                f"{p.day}: loaded {loaded} rows, manifest says {p.row_count}"
            )
 
        con.execute(
            f"INSERT OR REPLACE INTO {LOAD_LOG} VALUES (?, ?, ?, now())",
            [p.day, p.extracted_at, loaded],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    if added and not first_load:
        log.warning("%s: new source columns added to %s: %s", p.day, TABLE, added)
    return loaded

# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
 
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load raw CFPB partitions into DuckDB.")
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--force", action="store_true", help="Reload even if unchanged")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)
 
 
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
 
    partitions = discover_partitions(args.raw_dir, args.start, args.end)
    if not partitions:
        log.info("No partitions found in %s", args.raw_dir)
        return 0
 
    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db_path))
    try:
        ensure_tables(con)
        first_load = len(table_columns(con)) <= 4  # only metadata columns so far
        loaded_days = skipped = total_rows = 0
 
        for p in partitions:
            if not args.force and already_loaded(con, p):
                skipped += 1
                log.debug("%s: unchanged, skipping", p.day)
                continue
            try:
                rows = load_partition(con, p, first_load)
            except Exception as exc:
                log.error("Stopping at %s: %s", p.day, exc)
                return 1
            first_load = False
            loaded_days += 1
            total_rows += rows
            log.info("%s: loaded %d rows", p.day, rows)
 
        log.info(
            "Done. Loaded %d day(s), %d rows. Skipped %d unchanged.",
            loaded_days, total_rows, skipped,
        )
        return 0
    finally:
        con.close()
 
 
if __name__ == "__main__":
    sys.exit(main())