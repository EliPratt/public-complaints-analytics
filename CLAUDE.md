# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A data pipeline for CFPB consumer complaints: extract from the public CFPB API → land raw daily CSV partitions on disk → load into a local DuckDB warehouse → transform in dbt (planned; dbt is not set up yet).

## Commands

Run everything from the **repo root**: default paths (`data/raw`, `data/state`, `data/warehouse`) are relative to the current directory.

```bash
source .venv/bin/activate
pip install -r ingestion/requirements.txt

# Extract
python ingestion/extract.py --start 2025-09-01 --end 2025-09-07   # backfill
python ingestion/extract.py                                        # incremental (needs saved state)

# Load into DuckDB (data/warehouse/complaints.duckdb)
python ingestion/load.py                  # only new/changed partitions
python ingestion/load.py --force          # reload everything in range

# Tests (run from repo root — test_extract.py imports `ingestion.extract`,
# test_load.py imports `load` via pythonpath in ingestion/pytest.ini)
python -m pytest ingestion/tests
python -m pytest ingestion/tests/test_extract.py::test_backfill_start_wins
```

No linter or formatter is configured.

## Architecture

### The contract between extract and load

`extract.py` and `load.py` communicate only through the filesystem layout:

```
data/raw/date_received=YYYY-MM-DD/complaints.csv   # unmodified API export
data/raw/date_received=YYYY-MM-DD/manifest.json    # row_count, api_total, extracted_at
```

- The manifest is written **after** the CSV (both via atomic temp-file + rename). `load.py` treats a partition without a manifest as an unfinished pull and skips it.
- `extracted_at` in the manifest is the version marker: `load.py` records it in `raw._load_log` and reloads a day only when a newer extraction exists.
- `row_count` is reconciled twice: extract checks it against the API's search total, and load checks it against the rows actually inserted. Either mismatch fails the run instead of loading partial data.

### extract.py

- Pulls one day per request using the API's CSV export mode (`format=csv`), which avoids the API's 10k-offset paging limit.
- Incremental runs re-pull a lookback window (default 30 days) because CFPB publishes late and updates existing complaints. Start = `min(end - lookback + 1, last_complete + 1)`, so a stale state file causes a full gap backfill.
- State lives in `data/state/extract_state.json` (`last_complete_date`). The marker only moves forward and is saved after each successful day, so a failed run resumes where it stopped.
- `end` defaults to yesterday (UTC). The most recent ~week of partitions is always incomplete because of publication lag. That's expected.

### load.py

- Raw stays raw: every source column is loaded as VARCHAR (`all_varchar`), and headers are only snake_cased (`normalize_column`). Typing and cleaning belong in dbt.
- Idempotent per day: one transaction does a DELETE of the partition's rows, an INSERT, a reconcile and a `_load_log` upsert. Rerunning or re-pulling a day never duplicates rows.
- Schema drift: new CSV columns are added to `raw.complaints` with `ALTER TABLE` (with a warning after the first load). Missing columns load as NULL because of `INSERT ... BY NAME`.
- `read_csv` must keep `hive_partitioning = false`, or DuckDB adds `date_received` as a column taken from the folder name.
- Metadata columns are prefixed `_` (`_partition_date`, `_source_file`, `_extracted_at`, `_loaded_at`). The first-load check counts on there being exactly 4 of them.

### Downstream assumptions

Complaints can appear in more than one partition over time (late updates). Downstream models are expected to dedupe on `complaint_id`, and dbt snapshots are intended to track field changes.

## Conventions

- Design decisions and their trade-offs are documented in module docstrings. Keep those up to date when behavior changes. Non-obvious architectural choices should also get a `DECISIONS.md` entry (Decision / Why / Trade-off / Alternatives considered).
- `data/` is gitignored. Never commit extracted data or the DuckDB file.
