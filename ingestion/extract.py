"""
Extract CFPB consumer complaints into a local raw landing zone.
 
How it works
------------
The CFPB API caps normal paging (100 records per page, and offset paging
breaks down past 10,000 results). Its export mode (format=csv) returns every
record matching a filter in one response, so this script requests one day of
complaints at a time and skips pagination entirely.
 
Each day is written unmodified to:
    data/raw/date_received=YYYY-MM-DD/complaints.csv
alongside a manifest.json with the row count, the API's reported total, and
when the pull happened. Raw files are never edited; cleaning happens in dbt.
 
Why the lookback window
-----------------------
The CFPB publishes complaints on a delay, so new records keep showing up for
dates that were already pulled, and existing records can change (for example
when a company responds). Every incremental run re-pulls the last N days
(default 30) and overwrites those partitions. Downstream models dedupe on
complaint_id, and dbt snapshots track field changes over time.
 
Reconciliation
--------------
Before each export, the script asks the search endpoint how many complaints
exist for that day and compares it with the rows actually received. A
mismatch means the export was truncated or the data shifted mid-pull, and the
day fails loudly instead of loading partial data.
 
Usage
-----
    # One-time backfill
    python extract.py --start 2024-01-01 --end 2024-12-31
 
    # Daily incremental run (uses state file + lookback)
    python extract.py
"""

from __future__ import annotations
import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_OUTPUT_DIR = Path("data/raw")
DEFAULT_STATE_FILE = Path("data/state/extract_state.json")
PAUSE_BETWEEN_DAYS_SECONDS = 1.0 # Public API - keep requests low
MISMATCH_RETRIES = 1

log = logging.getLogger("cfpb.extract")

class ReconciliationError(Exception):
    """Rows received do not match the API's reported total for the day."""

@dataclass
class DayManifest:
    date_received: str
    row_count: int
    api_total: int
    extracted_at: str
    file: str

# -------------------------------------------------------------------------
# HTTP
# -------------------------------------------------------------------------

def build_session() -> requests.Session:
    """Session with retries and backoff for rate limits and server errors."""
    retry = Retry(
        total = 5,
        backoff_factor = 2, # 2s, 4s, 8s, ...
        status_forcelist = (429, 500, 502, 503, 504),
        allowed_methods = ("GET",),
        respect_retry_after_header = True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries = retry))
    session.headers["User-Agent"] = "cfpb-complaints-pipeline (portfolio project)"
    return session

def day_filter(day: date) -> dict[str, str]:
    """API date filters: min is inclusive, max is exclusive."""
    return {
        "date_received_min": day.isoformat(),
        "date_received_max": (day + timedelta(days = 1)).isoformat(),
    }

def fetch_api_total (session: requests.Session, day: date) -> int:
    """Ask the search endpoint how many complaints exist for one day."""
    params = {**day_filter(day), "size": 1, "no_aggs": "true", "no_highlight": "true"}
    resp = session.get (API_URL, params=params, timeout = 60)
    resp.raise_for_status()
    total = resp.json()["hits"]["total"]
    # OpenSearch returns {"value": n, "relation": "eq"}; older versions return an int
    return int(total["value"] if isinstance(total, dict) else total)

def fetch_day_export (session: requests.Session, day: date) -> bytes:
    """Download every complaint for one day as CSV bytes"""
    params = {**day_filter(day), "format": "csv", "no_aggs": "true"}
    resp = session.get(API_URL, params=params, timeout=300)
    resp.raise_for_status()
    return resp.content

def count_csv_rows(raw: bytes) -> int:
    """Count data rows. Uses the csv module because narratives contain newlines."""
    text = raw.decode("utf-8-sig")
    if not text.strip():
        return 0
    reader = csv.reader(io.StringIO(text))
    next(reader, None) # header
    return sum(1 for row in reader if row)


# --------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------

def write_atomic(path: Path, data: bytes) -> None:
    """Write to a temp file then rename, so a crash never leaves a half file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)

def extract_day(session: requests.Session, day: date, output_dir: Path) -> DayManifest:
    """Pull reconcile, and land one day of complaints"""
    for attempt in range(MISMATCH_RETRIES + 1):
        api_total = fetch_api_total(session, day)
        raw = fetch_day_export(session, day)
        row_count = count_csv_rows(raw)
        if row_count == api_total:
            break
        log.warning(
            "%s: received %d rows but API reports %d (attempt %d)",
            day, row_count, api_total, attempt + 1,
        )
    else:
        raise ReconciliationError(
            f"{day}: received {row_count} rows, API reports {api_total}"
            )
    
    partition = output_dir / f"date_received={day.isoformat()}"
    data_file = partition / "complaints.csv"
    write_atomic(data_file, raw)

    manifest = DayManifest(
        date_received=day.isoformat(),
        row_count=row_count,
        api_total=api_total,
        extracted_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        file=str(data_file),
    )
    write_atomic(partition / "manifest.json", json.dumps(asdict(manifest), indent=2).encode())
    return manifest

# -------------------------------------------------------------------
# State and date planning
# -------------------------------------------------------------------

def load_state(state_file: Path) -> date | None:
    if not state_file.exists():
        return None
    data = json.loads(state_file.read_text())
    return date.fromisoformat(data["last_complete_date"])

def save_state(state_file: Path, last_complete: date) -> None:
    payload = {
        "last_complete_date": last_complete.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    write_atomic(state_file, json.dumps(payload, indent=2).encode())


def plan_date_range(
        last_complete: date | None,
        end: date,
        lookback_days: int,
        start_override: date | None = None,
) -> list[date]:
    """ Decide which days to pull.
    - An explicit start (backfill) wins.
    - Otherwise re-pull the lookback window, reaching further back if the pipeline has been down longer than the window.
    """
    if start_override is not None:
        start = start_override
    elif last_complete is None:
        raise ValueError("No saved state. Run a backfill first with --start YYYY-MM-DD.")
    else:
        lookback_start = end - timedelta(days=lookback_days - 1)
        gap_start = last_complete + timedelta(days=1)
        start = min(lookback_start, gap_start)

    if start > end:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

# ------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract CFBP complaints by day.")
    p.add_argument("--start", type=date.fromisoformat, help="Backfill start date (YYYY-MM-DD)")
    p.add_argument("--end", type=date.fromisoformat, help="End date, inclusive (default: yesterday UTC)")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)

def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Today is still filling in, so stop at yesterday by default
    end = args.end or (datetime.now(timezone.utc).date() - timedelta(days=1))
    last_complete = load_state(args.state_file)

    try:
        days = plan_date_range(last_complete, end, args.lookback_days, args.start)
    except ValueError as exc:
        log.error(str(exc))
        return 2
    
    if not days:
        log.info("Nothing to do.")
        return 0
    
    log.info("Extracting %d day(s): %s to %s", len(days), days[0], days[-1])
    session = build_session()
    total_rows = 0

    for day in days:
        try:
            manifest = extract_day(session, day, args.output_dir)
        except (requests.RequestException, ReconciliationError) as exc:
            # Days run in order, so the saved state still points at the last good day
            log.error("Stopping at %s: %s", day, exc)
            return 1

        total_rows += manifest.row_count
        log.info("%s: %d rows", day, manifest.row_count)

        # Only move the watermark forward; lookback re-pulls must not rewind it
        if last_complete is None or day > last_complete:
            last_complete = day
            save_state(args.state_file, last_complete)

        time.sleep(PAUSE_BETWEEN_DAYS_SECONDS)

    log.info("Done. %d rows across %d day(s).", total_rows, len(days))
    return 0

if __name__ == "__main__":
    sys.exit(main())