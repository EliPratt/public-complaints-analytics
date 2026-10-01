import json
from datetime import date

import pytest

import ingestion.extract as extract


# ---- CSV counting --------------------------------------------------------

def test_count_rows_handles_multiline_narratives():
    raw = (
        'Date received,Consumer complaint narrative,Complaint ID\n'
        '2024-01-02,"Line one\nline two\nline three",101\n'
        '2024-01-02,Short,102\n'
    ).encode()
    assert extract.count_csv_rows(raw) == 2


def test_count_rows_empty_and_header_only():
    assert extract.count_csv_rows(b"") == 0
    assert extract.count_csv_rows(b"Date received,Complaint ID\n") == 0


# ---- Date planning -------------------------------------------------------

def test_backfill_start_wins():
    days = extract.plan_date_range(None, date(2024, 1, 3), 30, date(2024, 1, 1))
    assert days == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]


def test_incremental_uses_lookback_window():
    days = extract.plan_date_range(date(2024, 3, 9), date(2024, 3, 10), 5)
    assert days[0] == date(2024, 3, 6)
    assert days[-1] == date(2024, 3, 10)


def test_incremental_reaches_back_after_outage():
    # Last good day was long before the lookback window: fill the whole gap
    days = extract.plan_date_range(date(2024, 1, 31), date(2024, 3, 10), 5)
    assert days[0] == date(2024, 2, 1)


def test_no_state_and_no_start_raises():
    with pytest.raises(ValueError):
        extract.plan_date_range(None, date(2024, 3, 10), 30)


# ---- extract_day with a fake API ----------------------------------------

class FakeResponse:
    def __init__(self, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, total, csv_bytes):
        self.total = total
        self.csv_bytes = csv_bytes

    def get(self, url, params, timeout):
        if params.get("format") == "csv":
            return FakeResponse(content=self.csv_bytes)
        return FakeResponse(payload={"hits": {"total": {"value": self.total, "relation": "eq"}}})


CSV_TWO_ROWS = b"Date received,Complaint ID\n2024-01-02,1\n2024-01-02,2\n"


def test_extract_day_writes_partition_and_manifest(tmp_path):
    manifest = extract.extract_day(FakeSession(2, CSV_TWO_ROWS), date(2024, 1, 2), tmp_path)

    partition = tmp_path / "date_received=2024-01-02"
    assert (partition / "complaints.csv").read_bytes() == CSV_TWO_ROWS
    saved = json.loads((partition / "manifest.json").read_text())
    assert saved["row_count"] == 2 == manifest.api_total


def test_extract_day_fails_on_count_mismatch(tmp_path):
    with pytest.raises(extract.ReconciliationError):
        extract.extract_day(FakeSession(5, CSV_TWO_ROWS), date(2024, 1, 2), tmp_path)
    assert not (tmp_path / "date_received=2024-01-02").exists()