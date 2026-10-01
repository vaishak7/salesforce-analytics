from extract.salesforce_client import to_utc


def test_to_utc_keeps_utc_times():
    assert to_utc("2026-09-24T10:09:24.000+0000") == "2026-09-24 10:09:24.000000"


def test_to_utc_converts_other_timezones():
    # 10:09 in India (UTC+05:30) is 04:39 UTC
    assert to_utc("2026-09-24T10:09:24.000+0530") == "2026-09-24 04:39:24.000000"