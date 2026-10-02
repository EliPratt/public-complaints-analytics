import json
import duckdb
import load

HEADER = "Date received,Product,Consumer complaint narrative,Consumer consent provided?,Complaint ID\n"


def write_partition(raw_dir, day, rows, extracted_at="2025-09-08T00:00:00+00:00", header=HEADER):
    part = raw_dir / f"date_received={day}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "complaints.csv").write_text(header + "".join(rows))
    (part / "manifest.json").write_text(
        json.dumps({"date_received": day, "row_count": len(rows), "extracted_at": extracted_at})
    )


def run(tmp_path, *extra):
    db = tmp_path / "wh.duckdb"
    code = load.main(["--raw-dir", str(tmp_path / "raw"), "--db-path", str(db), *extra])
    return code, db


def query(db, sql):
    con = duckdb.connect(str(db))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


ROWS = [
    '2025-09-01,Mortgage,"First line\nsecond line, with a comma",Consent provided,1\n',
    "2025-09-01,Debt collection,,N/A,2\n",
]


def test_normalize_column():
    assert load.normalize_column("Consumer consent provided?") == "consumer_consent_provided"
    assert load.normalize_column("Sub-product") == "sub_product"
    assert load.normalize_column("ZIP code") == "zip_code"


def test_loads_rows_with_multiline_narratives(tmp_path):
    write_partition(tmp_path / "raw", "2025-09-01", ROWS)
    code, db = run(tmp_path)
    assert code == 0
    result = query(db, "SELECT complaint_id, consumer_complaint_narrative FROM raw.complaints ORDER BY 1")
    assert result[0] == ("1", "First line\nsecond line, with a comma")
    assert len(result) == 2


def test_rerun_is_idempotent_and_skips_unchanged(tmp_path):
    write_partition(tmp_path / "raw", "2025-09-01", ROWS)
    run(tmp_path)
    run(tmp_path)
    run(tmp_path, "--force")
    assert query(tmp_path / "wh.duckdb", "SELECT count(*) FROM raw.complaints")[0][0] == 2


def test_new_extraction_replaces_partition(tmp_path):
    raw = tmp_path / "raw"
    write_partition(raw, "2025-09-01", ROWS)
    run(tmp_path)
    # Lookback re-pull found a late-arriving complaint
    write_partition(raw, "2025-09-01", ROWS + ["2025-09-01,Mortgage,,N/A,3\n"],
                    extracted_at="2025-09-09T00:00:00+00:00")
    _, db = run(tmp_path)
    assert query(db, "SELECT count(*) FROM raw.complaints")[0][0] == 3


def test_schema_drift_adds_column(tmp_path):
    raw = tmp_path / "raw"
    write_partition(raw, "2025-09-01", ROWS)
    write_partition(
        raw, "2025-09-02", ["2025-09-02,Mortgage,,N/A,4,Web\n"],
        header=HEADER.strip() + ",Submitted via\n",
    )
    code, db = run(tmp_path)
    assert code == 0
    result = query(db, "SELECT complaint_id, submitted_via FROM raw.complaints ORDER BY 1")
    assert result == [("1", None), ("2", None), ("4", "Web")]


def test_manifest_mismatch_rolls_back(tmp_path):
    raw = tmp_path / "raw"
    write_partition(raw, "2025-09-01", ROWS)
    # Corrupt the manifest so it claims more rows than the file has
    manifest = raw / "date_received=2025-09-01" / "manifest.json"
    data = json.loads(manifest.read_text())
    data["row_count"] = 5
    manifest.write_text(json.dumps(data))

    code, db = run(tmp_path)
    assert code == 1
    assert query(db, "SELECT count(*) FROM raw.complaints")[0][0] == 0
    assert query(db, "SELECT count(*) FROM raw._load_log")[0][0] == 0


def test_incomplete_partition_is_skipped(tmp_path):
    part = tmp_path / "raw" / "date_received=2025-09-01"
    part.mkdir(parents=True)
    (part / "complaints.csv").write_text(HEADER + ROWS[1])  # no manifest
    code, _ = run(tmp_path)
    assert code == 0