import pytest

from extract.main import extract_object


class FakeSalesforce:
    """Pretends to be SalesforceClient. Returns the records we give it."""

    def __init__(self, records=None, error=None):
        self.records = records or []
        self.error = error
        self.since_used = "never called"

    def fetch_records(self, object_name, since):
        self.since_used = since
        if self.error:
            raise self.error
        return self.records


class FakeLoader:
    """Pretends to be SnowflakeLoader. Remembers what we asked it to do."""

    def __init__(self, watermark=None):
        self.watermark = watermark
        self.saved_watermark = None
        self.audits = []

    def create_object_table(self, object_name):
        pass

    def get_watermark(self, object_name):
        return self.watermark

    def load_records(self, object_name, records):
        return len(records)

    def save_watermark(self, object_name, new_watermark, rows_inserted):
        self.saved_watermark = new_watermark

    def write_audit(self, run_id, object_name, status, fetched, inserted, started_at, error=None):
        self.audits.append(
            {"status": status, "fetched": fetched, "inserted": inserted, "error": error}
        )


def make_record(timestamp):
    return {"Id": "001000000000001", "SystemModstamp": timestamp, "IsDeleted": False}


def test_saves_latest_timestamp_as_watermark():
    records = [
        make_record("2026-09-24T10:00:00.000+0000"),
        make_record("2026-09-25T08:30:00.000+0000"),  # the latest one
        make_record("2026-09-24T12:00:00.000+0000"),
    ]
    loader = FakeLoader()

    extract_object(FakeSalesforce(records), loader, "Account", "run1")

    assert loader.saved_watermark == "2026-09-25 08:30:00.000000"
    assert loader.audits == [{"status": "SUCCESS", "fetched": 3, "inserted": 3, "error": None}]


def test_uses_existing_watermark_when_fetching():
    sf = FakeSalesforce()
    extract_object(sf, FakeLoader(watermark="2026-09-24 10:00:00"), "Account", "run1")
    assert sf.since_used == "2026-09-24 10:00:00"


def test_no_new_records_does_not_move_watermark():
    loader = FakeLoader(watermark="2026-09-24 10:00:00")
    extract_object(FakeSalesforce([]), loader, "Account", "run1")
    assert loader.saved_watermark is None
    assert loader.audits[0]["status"] == "SUCCESS"


def test_failure_is_audited_and_watermark_not_moved():
    sf = FakeSalesforce(error=RuntimeError("Salesforce is down"))
    loader = FakeLoader()

    with pytest.raises(RuntimeError):
        extract_object(sf, loader, "Account", "run1")

    assert loader.saved_watermark is None
    assert loader.audits[0]["status"] == "FAILED"
    assert "Salesforce is down" in loader.audits[0]["error"]