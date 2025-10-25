# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, List, Literal
from zoneinfo import ZoneInfo


WindowKind = Literal["day", "week", "month", "year"]


@dataclass(frozen=True)
class Window:
    """Half-open time window [start, end) in the given timezone."""
    start: datetime
    end: datetime
    kind: WindowKind


def _start_of_day(d: date, tz: ZoneInfo) -> datetime:
    return datetime.combine(d, time.min).replace(tzinfo=tz)


def _start_of_next_day(d: date, tz: ZoneInfo) -> datetime:
    return _start_of_day(d + timedelta(days=1), tz)


def _month_range(start: date, end: date) -> Iterable[date]:
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y < end_y) or (y == end_y and m <= end_m):
        yield date(y, m, 1)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def split_into_windows(
    since: datetime,
    until: datetime,
    tz: ZoneInfo,
    kinds: Iterable[WindowKind],
) -> List[Window]:
    """Produce [start, end) windows for requested kinds in tz.

    ISO week: Monday..Monday.
    Month/Year: calendar boundaries.

    Args:
        since: inclusive lower bound (aware, any tz, will be converted to tz).
        until: exclusive upper bound (aware).
        tz: target timezone (e.g. Europe/Moscow).
        kinds: list of window kinds.

    Returns:
        List of Window sorted by start.
    """
    since_local = since.astimezone(tz)
    until_local = until.astimezone(tz)

    windows: List[Window] = []

    if "day" in kinds:
        d = since_local.date()
        last = until_local.date()
        while d <= last:
            s = _start_of_day(d, tz)
            e = _start_of_next_day(d, tz)
            if e > since_local and s < until_local:
                windows.append(Window(max(s, since_local), min(e, until_local), "day"))
            d = d + timedelta(days=1)

    if "week" in kinds:
        # ISO week starts Monday
        s = since_local
        monday = _start_of_day((s.date() - timedelta(days=(s.isoweekday() - 1))), tz)
        while monday < until_local:
            e = monday + timedelta(days=7)
            if e > since_local and monday < until_local:
                windows.append(Window(max(monday, since_local), min(e, until_local), "week"))
            monday = e

    if "month" in kinds:
        start_month = since_local.date().replace(day=1)
        end_month = until_local.date().replace(day=1)
        for month_start in _month_range(start_month, end_month):
            s = _start_of_day(month_start, tz)
            if month_start.month == 12:
                next_month = date(month_start.year + 1, 1, 1)
            else:
                next_month = date(month_start.year, month_start.month + 1, 1)
            e = _start_of_day(next_month, tz)
            if e > since_local and s < until_local:
                windows.append(Window(max(s, since_local), min(e, until_local), "month"))

    if "year" in kinds:
        y = since_local.year
        last_y = until_local.year
        while y <= last_y:
            s = datetime(y, 1, 1, tzinfo=tz)
            e = datetime(y + 1, 1, 1, tzinfo=tz)
            if e > since_local and s < until_local:
                windows.append(Window(max(s, since_local), min(e, until_local), "year"))
            y += 1

    windows.sort(key=lambda w: w.start)
    return windows
