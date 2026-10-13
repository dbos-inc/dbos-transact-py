# type: ignore
# Vendored from croniter 6.2.4 (https://pypi.org/project/croniter/)
# Updated to fix DST fire times with cron_timezone (dbos-inc/dbos-transact-py#885).

#!/usr/bin/env python
import binascii
import calendar
import copy
import datetime
import math
import platform
import random
import re
import struct
import sys
import traceback as _traceback
from time import time
from typing import Any, Literal, Optional, Union

from dateutil.relativedelta import relativedelta
from dateutil.tz import datetime_exists, tzutc

ExpandedExpression = list[Union[int, Literal["*", "l"]]]


def is_32bit() -> bool:
    """
    Detect if Python is running in 32-bit mode.
    Returns True if running on 32-bit Python, False for 64-bit.
    """
    # Method 1: Check pointer size
    bits = struct.calcsize("P") * 8

    # Method 2: Check platform architecture string
    try:
        architecture = platform.architecture()[0]
    except RuntimeError:
        architecture = None

    # Method 3: Check maxsize
    is_small_maxsize = sys.maxsize <= 2**32

    # Evaluate all available methods
    is_32 = False

    if bits == 32:
        is_32 = True
    elif architecture and "32" in architecture:
        is_32 = True
    elif is_small_maxsize:
        is_32 = True

    return is_32


try:
    # https://github.com/python/cpython/issues/101069 detection
    if is_32bit():
        datetime.datetime.fromtimestamp(3999999999)
    OVERFLOW32B_MODE = False
except OverflowError:
    OVERFLOW32B_MODE = True


UTC_DT = datetime.timezone.utc
EPOCH = datetime.datetime.fromtimestamp(0, UTC_DT)

M_ALPHAS: dict[str, Union[int, str]] = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
DOW_ALPHAS: dict[str, Union[int, str]] = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}

MINUTE_FIELD = 0
HOUR_FIELD = 1
DAY_FIELD = 2
MONTH_FIELD = 3
DOW_FIELD = 4
SECOND_FIELD = 5
YEAR_FIELD = 6

UNIX_FIELDS = (MINUTE_FIELD, HOUR_FIELD, DAY_FIELD, MONTH_FIELD, DOW_FIELD)
SECOND_FIELDS = (MINUTE_FIELD, HOUR_FIELD, DAY_FIELD, MONTH_FIELD, DOW_FIELD, SECOND_FIELD)
YEAR_FIELDS = (
    MINUTE_FIELD,
    HOUR_FIELD,
    DAY_FIELD,
    MONTH_FIELD,
    DOW_FIELD,
    SECOND_FIELD,
    YEAR_FIELD,
)

step_search_re = re.compile(r"^([^-]+)-([^-/]+)(/(\d+))?$")
only_int_re = re.compile(r"^\d+$")

DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
WEEKDAYS = "|".join(DOW_ALPHAS.keys())
MONTHS = "|".join(M_ALPHAS.keys())
star_or_int_re = re.compile(r"^(\d+|\*)$")
special_dow_re = re.compile(
    rf"^(?P<pre>((?P<he>(({WEEKDAYS})(-({WEEKDAYS}))?)"
    rf"|(({MONTHS})(-({MONTHS}))?)|\w+)#)|l)(?P<last>\d+)$"
)
nearest_weekday_re = re.compile(r"^(?:(\d+)w|w(\d+))$")
re_star = re.compile("[*]")
hash_expression_re = re.compile(
    r"^(?P<hash_type>h|r)(\((?P<range_begin>\d+)-(?P<range_end>\d+)\))?(\/(?P<divisor>\d+))?$"
)

CRON_FIELDS = {
    "unix": UNIX_FIELDS,
    "second": SECOND_FIELDS,
    "year": YEAR_FIELDS,
    len(UNIX_FIELDS): UNIX_FIELDS,
    len(SECOND_FIELDS): SECOND_FIELDS,
    len(YEAR_FIELDS): YEAR_FIELDS,
}
UNIX_CRON_LEN = len(UNIX_FIELDS)
SECOND_CRON_LEN = len(SECOND_FIELDS)
YEAR_CRON_LEN = len(YEAR_FIELDS)
# retrocompat
VALID_LEN_EXPRESSION = {a for a in CRON_FIELDS if isinstance(a, int)}

MARKER = object()


def datetime_to_timestamp(d):
    if d.tzinfo is not None:
        d = d.replace(tzinfo=None) - d.utcoffset()

    return (d - datetime.datetime(1970, 1, 1)).total_seconds()


def _is_leap(year: int) -> bool:
    return year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)


def _last_day_of_month(year: int, month: int) -> int:
    """Calculate the last day of the given month (honor leap years)."""
    last_day = DAYS[month - 1]
    if month == 2 and _is_leap(year):
        last_day += 1
    return last_day


def _is_successor(
    date: datetime.datetime, previous_date: datetime.datetime, is_prev: bool
) -> bool:
    """Check if the given date is a successor (after/before) of the previous date."""
    if is_prev:
        return date.astimezone(UTC_DT) < previous_date.astimezone(UTC_DT)
    return date.astimezone(UTC_DT) > previous_date.astimezone(UTC_DT)


def _timezone_delta(date1: datetime.datetime, date2: datetime.datetime) -> datetime.timedelta:
    """Calculate the timezone difference of the given dates."""
    offset1 = date1.utcoffset()
    offset2 = date2.utcoffset()
    assert offset1 is not None
    assert offset2 is not None
    return offset2 - offset1


def _add_tzinfo(
    date: datetime.datetime, previous_date: datetime.datetime, is_prev: bool
) -> tuple[datetime.datetime, bool]:
    """Add the tzinfo from the previous date to the given date.

    In case the new date is ambiguous, determine the correct date
    based on it being closer to the previous date but still a successor
    (after/before based on `is_prev`).

    In case the date does not exist, jump forward to the next existing date.
    """
    localize = getattr(previous_date.tzinfo, "localize", None)
    if localize is not None:
        # pylint: disable-next=import-outside-toplevel
        import pytz

        try:
            result = localize(date, is_dst=None)
        except pytz.NonExistentTimeError:
            while True:
                date += datetime.timedelta(minutes=1)
                try:
                    result = localize(date, is_dst=None)
                except pytz.NonExistentTimeError:
                    continue
                break
            return result, False
        except pytz.AmbiguousTimeError:
            closer = localize(date, is_dst=not is_prev)
            farther = localize(date, is_dst=is_prev)
            # TODO: Check negative DST
            assert (closer.astimezone(UTC_DT) > farther.astimezone(UTC_DT)) == is_prev
            if _is_successor(closer, previous_date, is_prev):
                result = closer
            else:
                assert _is_successor(farther, previous_date, is_prev)
                result = farther
        return result, True

    result = date.replace(fold=1 if is_prev else 0, tzinfo=previous_date.tzinfo)
    if not datetime_exists(result):
        while not datetime_exists(result):
            result += datetime.timedelta(minutes=1)
        return result, False

    # result is closer to the previous date
    farther = date.replace(fold=0 if is_prev else 1, tzinfo=previous_date.tzinfo)
    # Comparing the UTC offsets in the check for the date being ambiguous.
    if result.utcoffset() != farther.utcoffset():
        # TODO: Check negative DST
        assert (result.astimezone(UTC_DT) > farther.astimezone(UTC_DT)) == is_prev
        if not _is_successor(result, previous_date, is_prev):
            assert _is_successor(farther, previous_date, is_prev)
            result = farther
    return result, True


class CroniterError(ValueError):
    """General top-level Croniter base exception"""


class CroniterBadTypeRangeError(TypeError):
    """."""


class CroniterBadCronError(CroniterError):
    """Syntax, unknown value, or range error within a cron expression"""


class CroniterUnsupportedSyntaxError(CroniterBadCronError):
    """Valid cron syntax, but likely to produce inaccurate results"""

    # Extending CroniterBadCronError, which may be contridatory, but this allows
    # catching both errors with a single exception.  From a user perspective
    # these will likely be handled the same way.


class CroniterBadDateError(CroniterError):
    """Unable to find next/prev timestamp match"""


class CroniterNotAlphaError(CroniterBadCronError):
    """Cron syntax contains an invalid day or month abbreviation"""


class croniter:
    MONTHS_IN_YEAR = 12

    # This helps with expanding `*` fields into `lower-upper` ranges. Each item
    # in this tuple maps to the corresponding field index
    RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6), (0, 59), (1970, 2099))

    ALPHACONV: tuple[dict[str, Union[int, str]], ...] = (
        {},  # 0: min
        {},  # 1: hour
        {"l": "l"},  # 2: dom
        # 3: mon
        copy.deepcopy(M_ALPHAS),
        # 4: dow
        copy.deepcopy(DOW_ALPHAS),
        # 5: second
        {},
        # 6: year
        {},
    )

    LOWMAP: tuple[dict[int, int], ...] = ({}, {}, {0: 1}, {0: 1}, {7: 0}, {}, {})

    LEN_MEANS_ALL = (60, 24, 31, 12, 7, 60, 130)

    def __init__(
        self,
        expr_format: str,
        start_time: Optional[Union[datetime.datetime, float]] = None,
        ret_type: type = float,
        day_or: bool = True,
        max_years_between_matches: Optional[int] = None,
        is_prev: bool = False,
        hash_id: Optional[Union[bytes, str]] = None,
        implement_cron_bug: bool = False,
        second_at_beginning: bool = False,
        expand_from_start_time: bool = False,
    ) -> None:
        self._ret_type = ret_type
        self._day_or = day_or
        self._implement_cron_bug = implement_cron_bug
        self.second_at_beginning = bool(second_at_beginning)
        self._expand_from_start_time = expand_from_start_time

        if hash_id is not None:
            if not isinstance(hash_id, (bytes, str)):
                raise TypeError("hash_id must be bytes or UTF-8 string")
            if not isinstance(hash_id, bytes):
                hash_id = hash_id.encode("UTF-8")

        self._max_years_btw_matches_explicitly_set = max_years_between_matches is not None
        if max_years_between_matches is None:
            max_years_between_matches = 50
        self._max_years_between_matches = max(int(max_years_between_matches), 1)

        if start_time is None:
            start_time = time()

        self.tzinfo: Optional[datetime.tzinfo] = None

        self.start_time = 0.0
        self.dst_start_time = 0.0
        self.cur = 0.0
        self.set_current(start_time, force=True)

        self.expanded, self.nth_weekday_of_month, self.expressions, self.nearest_weekday = self._expand(
            expr_format,
            hash_id=hash_id,
            from_timestamp=self.dst_start_time if self._expand_from_start_time else None,
            second_at_beginning=second_at_beginning,
        )
        self.fields = CRON_FIELDS[len(self.expanded)]
        self._is_prev = is_prev

    @classmethod
    def _alphaconv(cls, index, key, expressions):
        try:
            return cls.ALPHACONV[index][key]
        except KeyError:
            raise CroniterNotAlphaError(f"[{' '.join(expressions)}] is not acceptable")

    def get_next(self, ret_type=None, start_time=None, update_current=True):
        if start_time and self._expand_from_start_time:
            raise ValueError(
                "start_time is not supported when using expand_from_start_time = True."
            )
        return self._get_next(
            ret_type=ret_type, start_time=start_time, is_prev=False, update_current=update_current
        )

    def get_prev(self, ret_type=None, start_time=None, update_current=True):
        return self._get_next(
            ret_type=ret_type, start_time=start_time, is_prev=True, update_current=update_current
        )

    def get_current(self, ret_type=None):
        ret_type = ret_type or self._ret_type
        if issubclass(ret_type, datetime.datetime):
            return self.timestamp_to_datetime(self.cur)
        return self.cur

    def set_current(
        self, start_time: Optional[Union[datetime.datetime, float]], force: bool = True
    ) -> float:
        if (force or (self.cur is None)) and start_time is not None:
            if isinstance(start_time, datetime.datetime):
                self.tzinfo = start_time.tzinfo
                start_time = self.datetime_to_timestamp(start_time)

            self.start_time = start_time
            self.dst_start_time = start_time
            self.cur = start_time
        return self.cur

    @staticmethod
    def datetime_to_timestamp(d: datetime.datetime) -> float:
        """
        Converts a `datetime` object `d` into a UNIX timestamp.
        """
        return datetime_to_timestamp(d)

    _datetime_to_timestamp = datetime_to_timestamp  # retrocompat

    def timestamp_to_datetime(self, timestamp: float, tzinfo: Any = MARKER) -> datetime.datetime:
        """
        Converts a UNIX `timestamp` into a `datetime` object.
        """
        if tzinfo is MARKER:  # allow to give tzinfo=None even if self.tzinfo is set
            tzinfo = self.tzinfo
        if OVERFLOW32B_MODE:
            # degraded mode to workaround Y2038
            # see https://github.com/python/cpython/issues/101069
            result = EPOCH.replace(tzinfo=None) + datetime.timedelta(seconds=timestamp)
        else:
            result = datetime.datetime.fromtimestamp(timestamp, tz=tzutc()).replace(tzinfo=None)
        if tzinfo:
            result = result.replace(tzinfo=UTC_DT).astimezone(tzinfo)
        return result

    _timestamp_to_datetime = timestamp_to_datetime  # retrocompat

    def _get_next(self, ret_type=None, start_time=None, is_prev=None, update_current=None):
        if update_current is None:
            update_current = True
        self.set_current(start_time, force=True)
        if is_prev is None:
            is_prev = self._is_prev
        self._is_prev = is_prev

        ret_type = ret_type or self._ret_type

        if not issubclass(ret_type, (float, datetime.datetime)):
            raise TypeError("Invalid ret_type, only 'float' or 'datetime' is acceptable.")

        result = self._calc_next(is_prev)
        timestamp = self.datetime_to_timestamp(result)
        if update_current:
            self.cur = timestamp
        if issubclass(ret_type, datetime.datetime):
            return result
        return timestamp

    # iterator protocol, to enable direct use of croniter
    # objects in a loop, like "for dt in croniter("5 0 * * *'): ..."
    # or for combining multiple croniters into single
    # dates feed using 'itertools' module
    def all_next(self, ret_type=None, start_time=None, update_current=None):
        """
        Returns a generator yielding consecutive dates.

        May be used instead of an implicit call to __iter__ whenever a
        non-default `ret_type` needs to be specified.
        """
        # In a Python 3.7+ world:  contextlib.suppress and contextlib.nullcontext could
        # be used instead
        try:
            while True:
                self._is_prev = False
                yield self._get_next(
                    ret_type=ret_type, start_time=start_time, update_current=update_current
                )
                start_time = None
        except CroniterBadDateError:
            if self._max_years_btw_matches_explicitly_set:
                return
            raise

    def all_prev(self, ret_type=None, start_time=None, update_current=None):
        """
        Returns a generator yielding previous dates.
        """
        try:
            while True:
                self._is_prev = True
                yield self._get_next(
                    ret_type=ret_type, start_time=start_time, update_current=update_current
                )
                start_time = None
        except CroniterBadDateError:
            if self._max_years_btw_matches_explicitly_set:
                return
            raise

    def iter(self, *args, **kwargs):
        return self.all_prev if self._is_prev else self.all_next

    def __iter__(self):
        return self

    __next__ = next = _get_next

    def _calc_next(self, is_prev: bool) -> datetime.datetime:
        current = self.timestamp_to_datetime(self.cur)
        expanded = self.expanded[:]
        nth_weekday_of_month = self.nth_weekday_of_month.copy()

        # exception to support day of month and day of week as defined in cron
        if (expanded[DAY_FIELD][0] != "*" and expanded[DOW_FIELD][0] != "*") and self._day_or:
            # If requested, handle a bug in vixie cron/ISC cron where day_of_month and
            # day_of_week form an intersection (AND) instead of a union (OR) if either
            # field is an asterisk or starts with an asterisk (https://crontab.guru/cron-bug.html)
            if self._implement_cron_bug and (
                re_star.match(self.expressions[DAY_FIELD])
                or re_star.match(self.expressions[DOW_FIELD])
            ):
                # To produce a schedule identical to the cron bug, we'll bypass the code
                # that makes a union of DOM and DOW, and instead skip to the code that
                # does an intersect instead
                pass
            else:
                bak = expanded[DOW_FIELD]
                expanded[DOW_FIELD] = ["*"]
                t1 = self._calc(current, expanded, nth_weekday_of_month, is_prev)
                expanded[DOW_FIELD] = bak
                expanded[DAY_FIELD] = ["*"]

                t2 = self._calc(current, expanded, nth_weekday_of_month, is_prev)
                if is_prev:
                    return t1 if t1 > t2 else t2
                return t1 if t1 < t2 else t2

        return self._calc(current, expanded, nth_weekday_of_month, is_prev)

    def _calc(
        self,
        now: datetime.datetime,
        expanded: list[ExpandedExpression],
        nth_weekday_of_month: dict[int, set[int]],
        is_prev: bool,
    ) -> datetime.datetime:
        if is_prev:
            nearest_diff_method = self._get_prev_nearest_diff
            offset = relativedelta(microseconds=-1)
        else:
            nearest_diff_method = self._get_next_nearest_diff
            if len(expanded) > UNIX_CRON_LEN:
                offset = relativedelta(seconds=1)
            else:
                offset = relativedelta(minutes=1)
        # Calculate the next cron time in local time a.k.a. timezone unaware time.
        unaware_time = now.replace(tzinfo=None) + offset
        if len(expanded) > UNIX_CRON_LEN:
            unaware_time = unaware_time.replace(microsecond=0)
        else:
            unaware_time = unaware_time.replace(second=0, microsecond=0)

        month = unaware_time.month
        year = current_year = unaware_time.year

        def proc_year(d):
            if len(expanded) == YEAR_CRON_LEN:
                try:
                    expanded[YEAR_FIELD].index("*")
                except ValueError:
                    # use None as range_val to indicate no loop
                    diff_year = nearest_diff_method(d.year, expanded[YEAR_FIELD], None)
                    if diff_year is None:
                        return None, d
                    if diff_year != 0:
                        if is_prev:
                            d += relativedelta(
                                years=diff_year, month=12, day=31, hour=23, minute=59, second=59
                            )
                        else:
                            d += relativedelta(
                                years=diff_year, month=1, day=1, hour=0, minute=0, second=0
                            )
                        return True, d
            return False, d

        def proc_month(d):
            try:
                expanded[MONTH_FIELD].index("*")
            except ValueError:
                diff_month = nearest_diff_method(
                    d.month, expanded[MONTH_FIELD], self.MONTHS_IN_YEAR
                )
                reset_day = 1

                if diff_month is not None and diff_month != 0:
                    if is_prev:
                        d += relativedelta(months=diff_month)
                        reset_day = _last_day_of_month(d.year, d.month)
                        d += relativedelta(day=reset_day, hour=23, minute=59, second=59)
                    else:
                        d += relativedelta(
                            months=diff_month, day=reset_day, hour=0, minute=0, second=0
                        )
                    return True, d
            return False, d

        def proc_day_of_month(d):
            try:
                expanded[DAY_FIELD].index("*")
            except ValueError:
                days = _last_day_of_month(year, month)
                if "l" in expanded[DAY_FIELD] and days == d.day:
                    return False, d

                if is_prev:
                    prev_month = (month - 2) % self.MONTHS_IN_YEAR + 1
                    prev_year = year - 1 if month == 1 else year
                    days_in_prev_month = _last_day_of_month(prev_year, prev_month)
                    diff_day = nearest_diff_method(d.day, expanded[DAY_FIELD], days_in_prev_month)
                else:
                    diff_day = nearest_diff_method(d.day, expanded[DAY_FIELD], days)

                if diff_day is not None and diff_day != 0:
                    if is_prev:
                        d += relativedelta(days=diff_day, hour=23, minute=59, second=59)
                    else:
                        d += relativedelta(days=diff_day, hour=0, minute=0, second=0)
                    return True, d
            return False, d

        def proc_day_of_week(d):
            try:
                expanded[DOW_FIELD].index("*")
            except ValueError:
                diff_day_of_week = nearest_diff_method(d.isoweekday() % 7, expanded[DOW_FIELD], 7)
                if diff_day_of_week is not None and diff_day_of_week != 0:
                    if is_prev:
                        d += relativedelta(days=diff_day_of_week, hour=23, minute=59, second=59)
                    else:
                        d += relativedelta(days=diff_day_of_week, hour=0, minute=0, second=0)
                    return True, d
            return False, d

        def proc_day_of_week_nth(d):
            if "*" in nth_weekday_of_month:
                s = nth_weekday_of_month["*"]
                for i in range(0, 7):
                    if i in nth_weekday_of_month:
                        nth_weekday_of_month[i].update(s)
                    else:
                        nth_weekday_of_month[i] = s
                del nth_weekday_of_month["*"]

            candidates = []
            for wday, nth in nth_weekday_of_month.items():
                c = self._get_nth_weekday_of_month(d.year, d.month, wday)
                for n in nth:
                    if n == "l":
                        candidate = c[-1]
                    elif len(c) < n:
                        continue
                    else:
                        candidate = c[n - 1]
                    if (is_prev and candidate <= d.day) or (not is_prev and d.day <= candidate):
                        candidates.append(candidate)

            if not candidates:
                if is_prev:
                    d += relativedelta(days=-d.day, hour=23, minute=59, second=59)
                else:
                    days = _last_day_of_month(year, month)
                    d += relativedelta(days=(days - d.day + 1), hour=0, minute=0, second=0)
                return True, d

            candidates.sort()
            diff_day = (candidates[-1] if is_prev else candidates[0]) - d.day
            if diff_day != 0:
                if is_prev:
                    d += relativedelta(days=diff_day, hour=23, minute=59, second=59)
                else:
                    d += relativedelta(days=diff_day, hour=0, minute=0, second=0)
                return True, d
            return False, d

        def proc_nearest_weekday(d):
            """Process W (nearest weekday) day-of-month entries."""
            candidates = []
            for w_day in self.nearest_weekday:
                candidate = self._get_nearest_weekday(d.year, d.month, w_day)
                if (is_prev and candidate <= d.day) or (not is_prev and d.day <= candidate):
                    candidates.append(candidate)

            if not candidates:
                if is_prev:
                    d += relativedelta(days=-d.day, hour=23, minute=59, second=59)
                else:
                    days = _last_day_of_month(year, month)
                    d += relativedelta(days=(days - d.day + 1), hour=0, minute=0, second=0)
                return True, d

            candidates.sort()
            diff_day = (candidates[-1] if is_prev else candidates[0]) - d.day
            if diff_day != 0:
                if is_prev:
                    d += relativedelta(days=diff_day, hour=23, minute=59, second=59)
                else:
                    d += relativedelta(days=diff_day, hour=0, minute=0, second=0)
                return True, d
            return False, d

        def proc_hour(d):
            try:
                expanded[HOUR_FIELD].index("*")
            except ValueError:
                diff_hour = nearest_diff_method(d.hour, expanded[HOUR_FIELD], 24)
                if diff_hour is not None and diff_hour != 0:
                    if is_prev:
                        d += relativedelta(hours=diff_hour, minute=59, second=59)
                    else:
                        d += relativedelta(hours=diff_hour, minute=0, second=0)
                    return True, d
            return False, d

        def proc_minute(d):
            try:
                expanded[MINUTE_FIELD].index("*")
            except ValueError:
                diff_min = nearest_diff_method(d.minute, expanded[MINUTE_FIELD], 60)
                if diff_min is not None and diff_min != 0:
                    if is_prev:
                        d += relativedelta(minutes=diff_min, second=59)
                    else:
                        d += relativedelta(minutes=diff_min, second=0)
                    return True, d
            return False, d

        def proc_second(d):
            if len(expanded) > UNIX_CRON_LEN:
                try:
                    expanded[SECOND_FIELD].index("*")
                except ValueError:
                    diff_sec = nearest_diff_method(d.second, expanded[SECOND_FIELD], 60)
                    if diff_sec is not None and diff_sec != 0:
                        d += relativedelta(seconds=diff_sec)
                        return True, d
            else:
                d += relativedelta(second=0)
            return False, d

        procs = [
            proc_year,
            proc_month,
            (proc_nearest_weekday if self.nearest_weekday else proc_day_of_month),
            (proc_day_of_week_nth if nth_weekday_of_month else proc_day_of_week),
            proc_hour,
            proc_minute,
            proc_second,
        ]

        while abs(year - current_year) <= self._max_years_between_matches:
            next = False
            stop = False
            for proc in procs:
                (changed, unaware_time) = proc(unaware_time)
                # `None` can be set mostly for year processing
                # so please see proc_year / _get_prev_nearest_diff / _get_next_nearest_diff
                if changed is None:
                    stop = True
                    break
                if changed:
                    month, year = unaware_time.month, unaware_time.year
                    next = True
                    break
            if stop:
                break
            if next:
                continue

            unaware_time = unaware_time.replace(microsecond=0)
            if now.tzinfo is None:
                return unaware_time

            # Add timezone information back and handle DST changes
            aware_time, exists = _add_tzinfo(unaware_time, now, is_prev)

            if not exists and (
                not _is_successor(aware_time, now, is_prev) or "*" in expanded[HOUR_FIELD]
            ):
                # The calculated local date does not exist and moving the time forward
                # to the next valid time isn't the correct solution. Search for the
                # next matching cron time that exists.
                while not exists:
                    unaware_time = self._calc(
                        unaware_time, expanded, nth_weekday_of_month, is_prev
                    )
                    aware_time, exists = _add_tzinfo(unaware_time, now, is_prev)

            offset_delta = _timezone_delta(now, aware_time)
            if not offset_delta:
                # There was no DST change.
                return aware_time

            # There was a DST change. So check if there is a alternative cron time
            # for the other UTC offset.
            alternative_unaware_time = now.replace(tzinfo=None) + offset_delta
            alternative_unaware_time = self._calc(
                alternative_unaware_time, expanded, nth_weekday_of_month, is_prev
            )
            alternative_aware_time, exists = _add_tzinfo(alternative_unaware_time, now, is_prev)

            if not _is_successor(alternative_aware_time, now, is_prev):
                # The alternative time is an ancestor of now. Thus it is not an alternative.
                return aware_time

            if _is_successor(aware_time, alternative_aware_time, is_prev):
                return alternative_aware_time

            return aware_time

        if is_prev:
            raise CroniterBadDateError("failed to find prev date")
        raise CroniterBadDateError("failed to find next date")

    @staticmethod
    def _get_next_nearest_diff(x, to_check, range_val):
        """
        `range_val` is the range of a field.
        If no available time, we can move to next loop(like next month).
        `range_val` can also be set to `None` to indicate that there is no loop.
        ( Currently, should only used for `year` field )
        """
        for i, d in enumerate(to_check):
            if range_val is not None:
                if d == "l":
                    # if 'l' then it is the last day of month
                    # => its value of range_val
                    d = range_val
                elif d > range_val:
                    continue
            if d >= x:
                return d - x
        # When range_val is None and x not exists in to_check,
        # `None` will be returned to suggest no more available time
        if range_val is None:
            return None
        return to_check[0] - x + range_val

    @staticmethod
    def _get_prev_nearest_diff(x, to_check, range_val):
        """
        `range_val` is the range of a field.
        If no available time, we can move to previous loop(like previous month).
        Range_val can also be set to `None` to indicate that there is no loop.
        ( Currently should only used for `year` field )
        """
        candidates = to_check[:]
        candidates.reverse()
        for d in candidates:
            if d != "l" and d <= x:
                return d - x
        if "l" in candidates:
            return -x
        # When range_val is None and x not exists in to_check,
        # `None` will be returned to suggest no more available time
        if range_val is None:
            return None
        candidate = candidates[0]
        for c in candidates:
            # fixed: c < range_val
            # this code will reject all 31 day of month, 12 month, 59 second,
            # 23 hour and so on.
            # if candidates has just a element, this will not harmful.
            # but candidates have multiple elements, then values equal to
            # range_val will rejected.
            if c <= range_val:
                candidate = c
                break
        # fix crontab "0 6 30 3 *" condidates only a element, then get_prev error
        # return 2021-03-02 06:00:00
        if candidate > range_val:
            return -range_val
        return candidate - x - range_val

    @staticmethod
    def _get_nth_weekday_of_month(year: int, month: int, day_of_week: int) -> tuple[int, ...]:
        """For a given year/month return a list of days in nth-day-of-month order.
        The last weekday of the month is always [-1].
        """
        w = (day_of_week + 6) % 7
        c = calendar.Calendar(w).monthdayscalendar(year, month)
        if c[0][0] == 0:
            c.pop(0)
        return tuple(i[0] for i in c)

    @staticmethod
    def _get_nearest_weekday(year, month, day):
        """Get the nearest weekday (Mon-Fri) to the given day in the given month.

        Rules:
        - If the day is a weekday, return it.
        - If Saturday, return Friday (day-1), unless that crosses into previous month,
          then return Monday (day+2).
        - If Sunday, return Monday (day+1), unless that crosses into next month,
          then return Friday (day-2).
        """
        last_day = _last_day_of_month(year, month)
        day = min(day, last_day)
        weekday = calendar.weekday(year, month, day)  # 0=Mon, 6=Sun
        if weekday < 5:  # Mon-Fri
            return day
        if weekday == 5:  # Saturday
            if day > 1:
                return day - 1  # Friday
            else:
                return day + 2  # Monday (1st is Sat, so 3rd is Mon)
        # Sunday
        if day < last_day:
            return day + 1  # Monday
        else:
            return day - 2  # Friday (last day is Sun, go back to Fri)

    @classmethod
    def value_alias(cls, val, field_index, len_expressions=UNIX_CRON_LEN):
        if isinstance(len_expressions, (list, dict, tuple, set)):
            len_expressions = len(len_expressions)
        if val in cls.LOWMAP[field_index] and not (
            # do not support 0 as a month either for classical 5 fields cron,
            # 6fields second repeat form or 7 fields year form
            # but still let conversion happen if day field is shifted
            (field_index in [DAY_FIELD, MONTH_FIELD] and len_expressions == UNIX_CRON_LEN)
            or (field_index in [MONTH_FIELD, DOW_FIELD] and len_expressions == SECOND_CRON_LEN)
            or (
                field_index in [DAY_FIELD, MONTH_FIELD, DOW_FIELD]
                and len_expressions == YEAR_CRON_LEN
            )
        ):
            val = cls.LOWMAP[field_index][val]
        return val

    # Maximum days in each month (non-leap year for Feb)
    DAYS_IN_MONTH = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

    @classmethod
    def _expand(cls, expr_format, hash_id=None, second_at_beginning=False, from_timestamp=None, strict=False, strict_year=None):
        # Split the expression in components, and normalize L -> l, MON -> mon,
        # etc. Keep expr_format untouched so we can use it in the exception
        # messages.
        expr_aliases = {
            "@midnight": ("0 0 * * *", "h h(0-2) * * * h"),
            "@hourly": ("0 * * * *", "h * * * * h"),
            "@daily": ("0 0 * * *", "h h * * * h"),
            "@weekly": ("0 0 * * 0", "h h * * h h"),
            "@monthly": ("0 0 1 * *", "h h h * * h"),
            "@yearly": ("0 0 1 1 *", "h h h h * h"),
            "@annually": ("0 0 1 1 *", "h h h h * h"),
        }

        efl = expr_format.lower()
        hash_id_expr = 1 if hash_id is not None else 0
        try:
            efl = expr_aliases[efl][hash_id_expr]
        except KeyError:
            pass

        expressions = efl.split()

        if len(expressions) not in VALID_LEN_EXPRESSION:
            raise CroniterBadCronError(
                "Exactly 5, 6 or 7 columns has to be specified for iterator expression."
            )

        if len(expressions) > UNIX_CRON_LEN and second_at_beginning:
            # move second to it's own(6th) field to process by same logical
            expressions.insert(SECOND_FIELD, expressions.pop(0))

        expanded = []
        nth_weekday_of_month = {}
        nearest_weekday = set()

        for field_index, expr in enumerate(expressions):
            for expanderid, expander in EXPANDERS.items():
                expr = expander(cls).expand(
                    efl, field_index, expr, hash_id=hash_id, from_timestamp=from_timestamp
                )

            if "?" in expr:
                if expr != "?":
                    raise CroniterBadCronError(
                        f"[{expr_format}] is not acceptable."
                        f" Question mark can not used with other characters"
                    )
                if field_index not in [DAY_FIELD, DOW_FIELD]:
                    raise CroniterBadCronError(
                        f"[{expr_format}] is not acceptable. "
                        f"Question mark can only used in day_of_month or day_of_week"
                    )
                # currently just trade `?` as `*`
                expr = "*"

            e_list = expr.split(",")
            res = []
            seen = set()

            while len(e_list) > 0:
                e = e_list.pop()
                nth = None

                if field_index == DOW_FIELD:
                    # Handle special case in the dow expression: 2#3, l3
                    special_dow_rem = special_dow_re.match(str(e))
                    if special_dow_rem:
                        g = special_dow_rem.groupdict()
                        he, last = g.get("he", ""), g.get("last", "")
                        if he:
                            e = he
                            try:
                                nth = int(last)
                                assert 5 >= nth >= 1
                            except (KeyError, ValueError, AssertionError):
                                raise CroniterBadCronError(
                                    f"[{expr_format}] is not acceptable."
                                    f" Invalid day_of_week value: '{nth}'"
                                )
                        elif last:
                            e = last
                            nth = g["pre"]  # 'l'

                if field_index == DAY_FIELD:
                    # Handle W (nearest weekday) in day-of-month: 15w, w15
                    w_match = nearest_weekday_re.match(str(e))
                    if w_match:
                        w_day = int(w_match.group(1) or w_match.group(2))
                        if w_day < 1 or w_day > 31:
                            raise CroniterBadCronError(
                                f"[{expr_format}] is not acceptable,"
                                f" nearest weekday day value '{w_day}' out of range"
                            )
                        if len(e_list) > 0 or len(res) > 0:
                            raise CroniterBadCronError(
                                f"[{expr_format}] is not acceptable."
                                f" 'W' can only be used with a single day value,"
                                f" not in a list or range"
                            )
                        nearest_weekday.add(w_day)
                        res.append(w_day)
                        continue

                # Before matching step_search_re, normalize "*" to "{min}-{max}".
                # Example: in the minute field, "*/5" normalizes to "0-59/5"
                t = re.sub(
                    r"^\*(\/.+)$",
                    r"%d-%d\1" % (cls.RANGES[field_index][0], cls.RANGES[field_index][1]),
                    str(e),
                )
                m = step_search_re.search(t)

                if not m:
                    # Before matching step_search_re,
                    # normalize "{start}/{step}" to "{start}-{max}/{step}".
                    # Example: in the minute field, "10/5" normalizes to "10-59/5"
                    t = re.sub(r"^(.+)\/(.+)$", r"\1-%d/\2" % (cls.RANGES[field_index][1]), str(e))
                    m = step_search_re.search(t)

                if m:
                    # early abort if low/high are out of bounds
                    (low, high, step) = m.group(1), m.group(2), m.group(4) or 1
                    if field_index == DAY_FIELD and high == "l":
                        high = "31"

                    if not only_int_re.search(low):
                        low = str(cls._alphaconv(field_index, low, expressions))

                    if not only_int_re.search(high):
                        high = str(cls._alphaconv(field_index, high, expressions))

                    # normally, it's already guarded by the RE that should not accept
                    # not-int values.
                    if not only_int_re.search(str(step)):
                        raise CroniterBadCronError(
                            f"[{expr_format}] step '{step}'"
                            f" in field {field_index} is not acceptable"
                        )
                    step = int(step)
                    if step == 0:
                        raise CroniterBadCronError(
                            f"[{expr_format}] step '{step}'"
                            f" in field {field_index} is not acceptable"
                        )

                    for band in low, high:
                        if not only_int_re.search(str(band)):
                            raise CroniterBadCronError(
                                f"[{expr_format}] bands '{low}-{high}'"
                                f" in field {field_index} are not acceptable"
                            )

                    low, high = (
                        cls.value_alias(int(_val), field_index, expressions)
                        for _val in (low, high)
                    )

                    if max(low, high) > max(
                        cls.RANGES[field_index][0], cls.RANGES[field_index][1]
                    ):
                        raise CroniterBadCronError(f"{expr_format} is out of bands")

                    if from_timestamp:
                        low = cls._get_low_from_current_date_number(
                            field_index, int(step), int(from_timestamp)
                        )

                    # Handle when the second bound of the range is in backtracking order:
                    # eg: X-Sun or X-7 (Sat-Sun) in DOW, or X-Jan (Apr-Jan) in MONTH
                    if low > high:
                        whole_field_range = list(
                            range(cls.RANGES[field_index][0], cls.RANGES[field_index][1] + 1, 1)
                        )
                        # Add FirstBound -> ENDRANGE, respecting step
                        rng = list(range(low, cls.RANGES[field_index][1] + 1, step))
                        # Then 0 -> SecondBound, but skipping n first occurences according to step
                        # EG to respect such expressions : Apr-Jan/3
                        to_skip = 0
                        if rng:
                            already_skipped = list(reversed(whole_field_range)).index(rng[-1])
                            curpos = whole_field_range.index(rng[-1])
                            if ((curpos + step) > len(whole_field_range)) and (
                                already_skipped < step
                            ):
                                to_skip = step - already_skipped
                        rng += list(range(cls.RANGES[field_index][0] + to_skip, high + 1, step))
                    # if we include a range type: Jan-Jan, or Sun-Sun,
                    #  it means the whole cycle (all days of week, # all monthes of year, etc)
                    elif low == high:
                        rng = list(
                            range(cls.RANGES[field_index][0], cls.RANGES[field_index][1] + 1, step)
                        )
                    else:
                        try:
                            rng = list(range(low, high + 1, step))
                        except ValueError as exc:
                            raise CroniterBadCronError(f"invalid range: {exc}")

                    if field_index == DOW_FIELD and nth and nth != "l":
                        rng = [f"{item}#{nth}" for item in rng]
                    e_list += [a for a in rng if a not in seen]
                    seen.update(rng)
                else:
                    if t.startswith("-"):
                        raise CroniterBadCronError(
                            f"[{expr_format}] is not acceptable, negative numbers not allowed"
                        )
                    if not star_or_int_re.search(t):
                        t = cls._alphaconv(field_index, t, expressions)

                    try:
                        t = int(t)
                    except ValueError:
                        pass

                    t = cls.value_alias(t, field_index, expressions)

                    if t not in ["*", "l"] and (
                        int(t) < cls.RANGES[field_index][0] or int(t) > cls.RANGES[field_index][1]
                    ):
                        raise CroniterBadCronError(
                            f"[{expr_format}] is not acceptable, out of range"
                        )

                    res.append(t)

                    if field_index == DOW_FIELD and nth:
                        if t not in nth_weekday_of_month:
                            nth_weekday_of_month[t] = set()
                        nth_weekday_of_month[t].add(nth)

            res = set(res)
            res = sorted(res, key=lambda i: f"{i:02}" if isinstance(i, int) else i)
            if len(res) == cls.LEN_MEANS_ALL[field_index]:
                # Make sure the wildcard is used in the correct way (avoid over-optimization)
                if (field_index == DAY_FIELD and "*" not in expressions[DOW_FIELD]) or (
                    field_index == DOW_FIELD and "*" not in expressions[DAY_FIELD]
                ):
                    pass
                else:
                    res = ["*"]

            expanded.append(["*"] if (len(res) == 1 and res[0] == "*") else res)

        # Check to make sure the dow combo in use is supported
        if nth_weekday_of_month:
            dow_expanded_set = set(expanded[DOW_FIELD])
            dow_expanded_set = dow_expanded_set.difference(nth_weekday_of_month.keys())
            dow_expanded_set.discard("*")
            # Skip: if it's all weeks instead of wildcard
            if dow_expanded_set and len(set(expanded[DOW_FIELD])) != cls.LEN_MEANS_ALL[DOW_FIELD]:
                raise CroniterUnsupportedSyntaxError(
                    f"day-of-week field does not support mixing literal values and nth"
                    f" day of week syntax.  Cron: '{expr_format}'"
                    f"    dow={dow_expanded_set} vs nth={nth_weekday_of_month}"
                )

        if strict:
            # Cross-validate day-of-month against month (and optionally year)
            # to reject impossible combinations like "0 0 31 2 *" (Feb 31st).
            days = expanded[DAY_FIELD]
            months = expanded[MONTH_FIELD]
            if days != ["*"] and days != ["l"] and months != ["*"]:
                int_days = [d for d in days if isinstance(d, int)]
                int_months = [m for m in months if isinstance(m, int)]
                if int_days and int_months:
                    # Determine max days per month, accounting for leap years
                    days_in_month = dict(cls.DAYS_IN_MONTH)
                    if 2 in int_months:
                        has_leap_year = True  # assume possible by default
                        if strict_year is not None:
                            # Year explicitly provided as parameter
                            if isinstance(strict_year, int):
                                has_leap_year = calendar.isleap(strict_year)
                            else:
                                has_leap_year = any(calendar.isleap(y) for y in strict_year)
                        elif len(expanded) > YEAR_FIELD:
                            years = expanded[YEAR_FIELD]
                            if years != ["*"]:
                                int_years = [y for y in years if isinstance(y, int)]
                                if int_years:
                                    has_leap_year = any(calendar.isleap(y) for y in int_years)
                        if has_leap_year:
                            days_in_month[2] = 29
                    min_day = min(int_days)
                    max_possible = max(days_in_month[m] for m in int_months)
                    if min_day > max_possible:
                        raise CroniterBadCronError(
                            f"[{expr_format}] is not acceptable. Day(s) {int_days}"
                            f" can never occur in month(s) {int_months}"
                        )

        return expanded, nth_weekday_of_month, expressions, nearest_weekday

    @classmethod
    def expand(
        cls,
        expr_format: str,
        hash_id: Optional[Union[bytes, str]] = None,
        second_at_beginning: bool = False,
        from_timestamp: Optional[float] = None,
        strict: bool = False,
        strict_year: Optional[Union[int, list[int]]] = None,
    ) -> tuple[list[ExpandedExpression], dict[int, set[int]]]:
        """
        Expand a cron expression format into a noramlized format of
        list[list[int | 'l' | '*']]. The first list representing each element
        of the epxression, and each sub-list representing the allowed values
        for that expression component.

        A tuple is returned, the first value being the expanded epxression
        list, and the second being a `nth_weekday_of_month` mapping.

        Examples:

        # Every minute
        >>> croniter.expand('* * * * *')
        ([['*'], ['*'], ['*'], ['*'], ['*']], {})

        # On the hour
        >>> croniter.expand('0 0 * * *')
        ([[0], [0], ['*'], ['*'], ['*']], {})

        # Hours 0-5 and 10 monday through friday
        >>> croniter.expand('0-5,10 * * * mon-fri')
        ([[0, 1, 2, 3, 4, 5, 10], ['*'], ['*'], ['*'], [1, 2, 3, 4, 5]], {})

        Note that some special values such as nth day of week are expanded to a
        special mapping format for later processing:

        # Every minute on the 3rd tuesday of the month
        >>> croniter.expand('* * * * 2#3')
        ([['*'], ['*'], ['*'], ['*'], [2]], {2: {3}})

        # Every hour on the last day of the month
        >>> croniter.expand('0 * l * *')
        ([[0], ['*'], ['l'], ['*'], ['*']], {})

        # On the hour every 15 seconds
        >>> croniter.expand('0 0 * * * */15')
        ([[0], [0], ['*'], ['*'], ['*'], [0, 15, 30, 45]], {})
        """
        try:
            expanded, nth_weekday_of_month, _expressions, _nearest_weekday = cls._expand(
                expr_format,
                hash_id=hash_id,
                second_at_beginning=second_at_beginning,
                from_timestamp=from_timestamp,
                strict=strict,
                strict_year=strict_year,
            )
            return expanded, nth_weekday_of_month
        except (ValueError,) as exc:
            if isinstance(exc, CroniterError):
                raise
            trace = _traceback.format_exc()
            raise CroniterBadCronError(trace)

    @classmethod
    def _get_low_from_current_date_number(cls, field_index, step, from_timestamp):
        dt = datetime.datetime.fromtimestamp(from_timestamp, tz=UTC_DT)
        if field_index == MINUTE_FIELD:
            return dt.minute % step
        if field_index == HOUR_FIELD:
            return dt.hour % step
        if field_index == DAY_FIELD:
            return ((dt.day - 1) % step) + 1
        if field_index == MONTH_FIELD:
            return ((dt.month - 1) % step) + 1
        if field_index == DOW_FIELD:
            return (dt.isoweekday() % 7) % step

        raise ValueError("Can't get current date number for index larger than 4")

    @classmethod
    def is_valid(cls, expression, hash_id=None, encoding="UTF-8", second_at_beginning=False, strict=False, strict_year=None):
        if hash_id:
            if not isinstance(hash_id, (bytes, str)):
                raise TypeError("hash_id must be bytes or UTF-8 string")
            if not isinstance(hash_id, bytes):
                hash_id = hash_id.encode(encoding)
        try:
            cls.expand(expression, hash_id=hash_id, second_at_beginning=second_at_beginning, strict=strict, strict_year=strict_year)
        except CroniterError:
            return False
        return True

    @classmethod
    def match(
        cls,
        cron_expression,
        testdate,
        day_or=True,
        second_at_beginning=False,
        precision_in_seconds=None,
    ):
        return cls.match_range(
            cron_expression, testdate, testdate, day_or, second_at_beginning, precision_in_seconds
        )

    @classmethod
    def match_range(
        cls,
        cron_expression,
        from_datetime,
        to_datetime,
        day_or=True,
        second_at_beginning=False,
        precision_in_seconds=None,
    ):
        cron = cls(
            cron_expression,
            to_datetime,
            ret_type=datetime.datetime,
            day_or=day_or,
            second_at_beginning=second_at_beginning,
        )
        tdp = cron.get_current(datetime.datetime)
        if not tdp.microsecond:
            tdp += relativedelta(microseconds=1)
        cron.set_current(tdp, force=True)
        try:
            tdt = cron.get_prev()
        except CroniterBadDateError:
            return False
        if precision_in_seconds is None:
            precision_in_seconds = 1 if len(cron.expanded) > UNIX_CRON_LEN else 60
        duration_in_second = (to_datetime - from_datetime).total_seconds() + precision_in_seconds
        return (max(tdp, tdt) - min(tdp, tdt)).total_seconds() < duration_in_second


def croniter_range(
    start,
    stop,
    expr_format,
    ret_type=None,
    day_or=True,
    exclude_ends=False,
    _croniter=None,
    second_at_beginning=False,
    expand_from_start_time=False,
):
    """
    Generator that provides all times from start to stop matching the given cron expression.
    If the cron expression matches either 'start' and/or 'stop', those times will be returned as
    well unless 'exclude_ends=True' is passed.

    You can think of this function as sibling to the builtin range function for datetime objects.
    Like range(start,stop,step), except that here 'step' is a cron expression.
    """
    _croniter = _croniter or croniter
    auto_rt = datetime.datetime
    # type is used in first if branch for perfs reasons
    if type(start) is not type(stop) and not (
        isinstance(start, type(stop)) or isinstance(stop, type(start))
    ):
        raise CroniterBadTypeRangeError(
            f"The start and stop must be same type.  {type(start)} != {type(stop)}"
        )
    if isinstance(start, (float, int)):
        start, stop = (
            datetime.datetime.fromtimestamp(t, tzutc()).replace(tzinfo=None) for t in (start, stop)
        )
        auto_rt = float
    if ret_type is None:
        ret_type = auto_rt
    if not exclude_ends:
        ms1 = relativedelta(microseconds=1)
        if start < stop:  # Forward (normal) time order
            start -= ms1
            stop += ms1
        else:  # Reverse time order
            start += ms1
            stop -= ms1
    year_span = math.floor(abs(stop.year - start.year)) + 1
    ic = _croniter(
        expr_format,
        start,
        ret_type=datetime.datetime,
        day_or=day_or,
        max_years_between_matches=year_span,
        second_at_beginning=second_at_beginning,
        expand_from_start_time=expand_from_start_time,
    )
    # define a continue (cont) condition function and step function for the main while loop
    if start < stop:  # Forward

        def cont(v):
            return v < stop

        step = ic.get_next
    else:  # Reverse

        def cont(v):
            return v > stop

        step = ic.get_prev
    try:
        dt = step()
        while cont(dt):
            if ret_type is float:
                yield ic.get_current(float)
            else:
                yield dt
            dt = step()
    except CroniterBadDateError:
        # Stop iteration when this exception is raised; no match found within the given year range
        return


class HashExpander:
    def __init__(self, cronit):
        self.cron = cronit

    def do(self, idx, hash_type="h", hash_id=None, range_end=None, range_begin=None):
        """Return a hashed/random integer given range/hash information"""
        if range_end is None:
            range_end = self.cron.RANGES[idx][1]
        if range_begin is None:
            range_begin = self.cron.RANGES[idx][0]
        if hash_type == "r":
            crc = random.randint(0, 0xFFFFFFFF)
        else:
            crc = binascii.crc32(hash_id) & 0xFFFFFFFF
        return ((crc >> idx) % (range_end - range_begin + 1)) + range_begin

    def match(self, efl, idx, expr, hash_id=None, **kw):
        return hash_expression_re.match(expr)

    def expand(self, efl, idx, expr, hash_id=None, match="", **kw):
        """Expand a hashed/random expression to its normal representation"""
        if match == "":
            match = self.match(efl, idx, expr, hash_id, **kw)
        if not match:
            return expr
        m = match.groupdict()

        if m["hash_type"] == "h" and hash_id is None:
            raise CroniterBadCronError("Hashed definitions must include hash_id")

        if m["range_begin"] and m["range_end"]:
            if int(m["range_begin"]) >= int(m["range_end"]):
                raise CroniterBadCronError("Range end must be greater than range begin")

        if m["range_begin"] and m["range_end"] and m["divisor"]:
            # Example: H(30-59)/10 -> 34-59/10 (i.e. 34,44,54)
            if int(m["divisor"]) == 0:
                raise CroniterBadCronError(f"Bad expression: {expr}")

            x = self.do(
                idx,
                hash_type=m["hash_type"],
                hash_id=hash_id,
                range_begin=int(m["range_begin"]),
                range_end=int(m["divisor"]) - 1 + int(m["range_begin"]),
            )
            return f"{x}-{int(m['range_end'])}/{int(m['divisor'])}"
        if m["range_begin"] and m["range_end"]:
            # Example: H(0-29) -> 12
            return str(
                self.do(
                    idx,
                    hash_type=m["hash_type"],
                    hash_id=hash_id,
                    range_end=int(m["range_end"]),
                    range_begin=int(m["range_begin"]),
                )
            )
        if m["divisor"]:
            # Example: H/15 -> 7-59/15 (i.e. 7,22,37,52)
            if int(m["divisor"]) == 0:
                raise CroniterBadCronError(f"Bad expression: {expr}")

            x = self.do(
                idx,
                hash_type=m["hash_type"],
                hash_id=hash_id,
                range_begin=self.cron.RANGES[idx][0],
                range_end=int(m["divisor"]) - 1 + self.cron.RANGES[idx][0],
            )
            return f"{x}-{self.cron.RANGES[idx][1]}/{int(m['divisor'])}"

        # Example: H -> 32
        return str(self.do(idx, hash_type=m["hash_type"], hash_id=hash_id))


EXPANDERS = {"hash": HashExpander}
