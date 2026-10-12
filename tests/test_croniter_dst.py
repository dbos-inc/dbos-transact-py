# type: ignore
"""DST wall-clock correctness for the vendored croniter (issue #885)."""

from datetime import datetime
from zoneinfo import ZoneInfo

from dbos._croniter import croniter


def test_daily_0900_europe_berlin_across_dst() -> None:
    """A daily 09:00 schedule must stay on 09:00 local through spring/fall."""
    tz = ZoneInfo("Europe/Berlin")
    spring = croniter(
        "0 0 9 * * *",
        datetime(2026, 3, 28, tzinfo=tz),
        second_at_beginning=True,
    )
    assert [spring.get_next(datetime).isoformat() for _ in range(3)] == [
        "2026-03-28T09:00:00+01:00",
        "2026-03-29T09:00:00+02:00",
        "2026-03-30T09:00:00+02:00",
    ]

    fall = croniter(
        "0 0 9 * * *",
        datetime(2026, 10, 24, tzinfo=tz),
        second_at_beginning=True,
    )
    assert [fall.get_next(datetime).isoformat() for _ in range(3)] == [
        "2026-10-24T09:00:00+02:00",
        "2026-10-25T09:00:00+01:00",
        "2026-10-26T09:00:00+01:00",
    ]
