"""Clip confirmed state intervals into UTC hours."""
from datetime import datetime, timedelta, timezone


def parse(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def iso(value):
    return value.isoformat().replace('+00:00', 'Z')


def hourly(start, end, window_start=None, window_end=None):
    start, end = parse(start), parse(end)
    if window_start:
        start = max(start, parse(window_start))
    if window_end:
        end = min(end, parse(window_end))
    while start < end:
        hour = start.replace(minute=0, second=0, microsecond=0)
        stop = min(end, hour + timedelta(hours=1))
        yield {'hour': iso(hour), 'start_time': iso(start), 'end_time': iso(stop),
               'duration_minutes': (stop - start).total_seconds() / 60}
        start = stop
