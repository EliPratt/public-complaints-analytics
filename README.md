# public-complaints-analytics

A batch ELT pipeline for the [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/): about 15 million complaints a year, pulled incrementally from a public API, landed as raw daily partitions, and loaded into a local DuckDB warehouse for modeling in dbt.

```
CFPB API ──extract.py──▶ data/raw/date_received=YYYY-MM-DD/ ──load.py──▶ DuckDB raw.complaints ──dbt──▶ models
            (daily CSV export,      complaints.csv                (idempotent,            (planned)
             reconciled)            manifest.json                  reconciled)
```

## Current status

| Stage | Status |
| --- | --- |
| Extract (`ingestion/extract.py`) | Done: backfill + incremental with lookback |
| Load (`ingestion/load.py`) | Done: incremental, idempotent, handles schema changes |
| Transform (dbt staging, marts, snapshots) | Planned |

The first full run covered 395 days (Sept 2025 – Sept 2026): **14.9M rows**. That's 4.7 GB of raw CSV, which compresses to a 411 MB DuckDB file. The load takes about 2 minutes on a laptop.

## Quickstart

Developed on Python 3.13. Run all commands from the repo root.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r ingestion/requirements.txt

# 1. Backfill a date range (one API request per day)
python ingestion/extract.py --start 2026-09-01 --end 2026-09-07

# 2. Load into data/warehouse/complaints.duckdb
python ingestion/load.py

# 3. Afterwards, daily runs need no arguments
python ingestion/extract.py && python ingestion/load.py
```

Query the result:

```bash
python -c "import duckdb; print(duckdb.connect('data/warehouse/complaints.duckdb').sql('SELECT product, count(*) n FROM raw.complaints GROUP BY 1 ORDER BY 2 DESC LIMIT 5'))"
```

### Options

| Script | Flag | Purpose |
| --- | --- | --- |
| `extract.py` | `--start`, `--end` | Backfill a specific range (`--end` defaults to yesterday, UTC) |
| | `--lookback-days N` | How many recent days each incremental run re-pulls (default 30) |
| `load.py` | `--start`, `--end` | Limit loading to a date range |
| | `--force` | Reload partitions even if they haven't changed |
| both | `-v` | Debug logging |

## How it works

### Extract

- **One request per day, no paging.** The CFPB API's normal search caps paging at 10,000 results. Its CSV export mode returns every match for a filter in one response, so the extractor asks for one `date_received` day at a time.
- **Lookback window.** CFPB publishes complaints with a delay and updates existing ones (for example, when a company responds). Each incremental run re-pulls the last 30 days and overwrites those partitions. If the pipeline has been down longer than that, it reaches back to fill the whole gap.
- **Count check.** Before each export, the extractor asks the API how many complaints exist for that day. If the downloaded row count differs, it retries once and then fails, instead of saving a partial day.
- **Crash-safe writes.** Files are written to a temp path and then renamed. `manifest.json` is written last, so a partition with a manifest is always complete.

### Load

- **Raw stays raw.** Every column is loaded as text. Header names are only converted to snake_case. Typing and cleaning are left to dbt.
- **Idempotent per day.** Each partition loads in one transaction: delete that day's rows, insert the file, check the count, record it in `raw._load_log`. Re-running or re-pulling a day never creates duplicates.
- **Incremental.** A day is reloaded only when `extract.py` has written a newer version of it (based on `extracted_at` in the manifest).
- **Schema changes.** New source columns are added to the table automatically, with a warning. Columns that disappear load as NULL.

### Data layout

```
data/
├── raw/date_received=2026-09-30/
│   ├── complaints.csv        # unmodified API export
│   └── manifest.json         # row_count, api_total, extracted_at
├── state/extract_state.json  # last fully extracted day
└── warehouse/complaints.duckdb
```

`data/` is gitignored. Everything in it can be rebuilt from the API.

## Design decisions

| Decision | Why | Trade-off |
| --- | --- | --- |
| Raw files on disk between extract and load | Loads can be replayed or rebuilt without calling the API again. The two stages can fail and retry independently. | Keeps about 4.7 GB per year of raw CSV on disk |
| Re-pull 30 days every run | Picks up late-arriving and updated complaints with no change tracking from the API | About 1.3M rows downloaded per run, mostly unchanged |
| Delete-and-insert per day (not upsert by `complaint_id`) | Simple to reason about, and a day's rows always match exactly one file | A complaint updated after the window closes keeps its old values. dbt snapshots are meant to cover history. |
| Count checks at both stages | A truncated export or partial load fails loudly instead of quietly losing rows | A day can fail if the API's count changes during the pull (retried once) |
| DuckDB | A single-file warehouse with no server. Fast analytical queries and native CSV reading. | Single writer, local only. The same layout would move to a cloud warehouse later. |

## Tests

```bash
python -m pytest ingestion/tests
```

The tests use a fake API session and temporary directories. They cover CSV counting with multiline narratives, date-range planning, count mismatches, idempotent reloads, schema changes and rollback on failure.

## Roadmap

- dbt project: staging models (type casting, dedupe on `complaint_id`), snapshots for complaint status changes, marts for company and product trends
- Scheduled daily runs
- Data quality tests in dbt

## Data source

The data comes from the Consumer Financial Protection Bureau's public Consumer Complaint Database. Complaints are published after the company responds or after 15 days, whichever comes first. Narratives appear only when the consumer opts in. See the CFPB's [notes on the data](https://www.consumerfinance.gov/complaint/data-use/) for limitations: complaints are not a statistical sample.
