import json
from datetime import date

import pytest
import requests

import ingestion.extract as extract


# ---- CSV counting --------------------------------------------------------

def test_count_rows_handles_multiline_narratives():
    raw = (
        'Date received,Consumer complaint narrative,Complaint ID\n'
        '2024-01-02,"Line one\nline two\nline three",101\n'
        '2024-01-02,Short,102\n'
    ).encode()
    assert extract.count_csv_rows(raw, date(2024, 1, 2)) == 2


def test_count_rows_empty_and_header_only():
    assert extract.count_csv_rows(b"", date(2024, 1, 2)) == 0
    assert extract.count_csv_rows(b"Date received,Complaint ID\n", date(2024, 1, 2)) == 0


def test_count_rows_rejects_rows_from_other_days():
    # What the old inclusive-max filter produced: the requested day plus the next
    raw = (
        b"Date received,Complaint ID\n"
        b"2024-01-02T10:00:00.000Z,1\n"
        b"2024-01-03T09:00:00.000Z,2\n"
    )
    with pytest.raises(extract.ReconciliationError, match="1 of 2 rows"):
        extract.count_csv_rows(raw, date(2024, 1, 2))


def test_count_rows_requires_date_column():
    with pytest.raises(extract.ReconciliationError):
        extract.count_csv_rows(b"Complaint ID\n1\n", date(2024, 1, 2))


# ---- API filter ----------------------------------------------------------

def test_day_filter_covers_exactly_one_day():
    # The API's date_received_max is inclusive; max = day + 1 pulls two days
    assert extract.day_filter(date(2026, 3, 11)) == {
        "date_received_min": "2026-03-11",
        "date_received_max": "2026-03-11",
    }


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


class FlakyExportSession(FakeSession):
    """Drops the connection mid-download for the first `failures` exports."""
    def __init__(self, total, csv_bytes, failures):
        super().__init__(total, csv_bytes)
        self.failures = failures
        self.export_calls = 0

    def get(self, url, params, timeout):
        if params.get("format") == "csv":
            self.export_calls += 1
            if self.export_calls <= self.failures:
                raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
        return super().get(url, params, timeout)


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(extract.time, "sleep", lambda seconds: None)


def test_export_retries_dropped_connection(no_sleep):
    session = FlakyExportSession(2, CSV_TWO_ROWS, failures=2)
    assert extract.fetch_day_export(session, date(2024, 1, 2)) == CSV_TWO_ROWS
    assert session.export_calls == 3


def test_export_gives_up_after_max_retries(no_sleep, tmp_path):
    session = FlakyExportSession(2, CSV_TWO_ROWS, failures=extract.DOWNLOAD_RETRIES + 1)
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        extract.extract_day(session, date(2024, 1, 2), tmp_path)
    assert session.export_calls == extract.DOWNLOAD_RETRIES + 1
    assert not (tmp_path / "date_received=2024-01-02").exists()


CSV_ONE_ROW =b"Date received,Complaint ID\n2024-01-02,1\n"
CSV_FIVE_ROWS = b"Date received,Complaint ID\n" + b"".join(
    b"2024-01-02,%d\n" % i for i in range(5)
)


def test_extract_day_refuses_large_shrink(tmp_path):
    day = date(2024, 1, 2)
    extract.extract_day(FakeSession(5, CSV_FIVE_ROWS), day, tmp_path)

    # The API agrees with itself (total 1, one row) but the day already holds 5
    with pytest.raises(extract.ShrinkError):
        extract.extract_day(FakeSession(1, CSV_ONE_ROW), day, tmp_path)
    partition = tmp_path / "date_received=2024-01-02"
    assert (partition / "complaints.csv").read_bytes() == CSV_FIVE_ROWS


def test_extract_day_allows_small_shrink_and_override(tmp_path):
    day = date(2024, 1, 2)
    extract.extract_day(FakeSession(5, CSV_FIVE_ROWS), day, tmp_path)
    extract.extract_day(FakeSession(2, CSV_TWO_ROWS), day, tmp_path, allow_shrink=True)
    # 5 -> 2 rows was allowed by the flag; 2 -> 1 is exactly half, so it passes on its own
    manifest = extract.extract_day(FakeSession(1, CSV_ONE_ROW), day, tmp_path)
    assert manifest.row_count == 1