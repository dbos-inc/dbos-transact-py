"""Regression for #885: cron_timezone fire times across DST transitions."""
from datetime import datetime
from zoneinfo import ZoneInfo

from dbos._croniter import croniter


def test_europe_berlin_daily_0900_across_spring_forward() -> None:
    tz = ZoneInfo("Europe/Berlin")
    start = datetime(2026, 3, 28, tzinfo=tz)
    it = croniter("0 0 9 * * *", start, second_at_beginning=True)
    got = [it.get_next(datetime) for _ in range(3)]
    assert [g.isoformat() for g in got] == [
        "2026-03-28T09:00:00+01:00",
        "2026-03-29T09:00:00+02:00",
        "2026-03-30T09:00:00+02:00",
    ]


def test_europe_berlin_daily_0900_across_fall_back() -> None:
    tz = ZoneInfo("Europe/Berlin")
    start = datetime(2026, 10, 24, tzinfo=tz)
    it = croniter("0 0 9 * * *", start, second_at_beginning=True)
    got = [it.get_next(datetime) for _ in range(3)]
    assert [g.isoformat() for g in got] == [
        "2026-10-24T09:00:00+02:00",
        "2026-10-25T09:00:00+01:00",
        "2026-10-26T09:00:00+01:00",
    ]
