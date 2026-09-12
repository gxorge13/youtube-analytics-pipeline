from datetime import datetime, timedelta
import re


ISO_8601_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)

def parse_duration(duration_str):
    """Convert a whole-second YouTube ISO 8601 duration to ``timedelta``."""
    match = ISO_8601_DURATION.fullmatch(duration_str)
    if not match or not any(match.groupdict().values()):
        raise ValueError(f"Unsupported ISO 8601 duration: {duration_str!r}")

    parts = {name: int(value or 0) for name, value in match.groupdict().items()}
    return timedelta(**parts)

def transform_data(row):
    """Return a transformed core-row copy without mutating staging data."""
    transformed = dict(row)
    duration = parse_duration(transformed["Duration"])
    if duration.days:
        raise ValueError("PostgreSQL TIME cannot represent durations of one day or more")

    transformed["Duration"] = (datetime.min + duration).time()
    transformed["Video_Type"] = "short" if duration.total_seconds() <= 60 else "normal"
    return transformed
    
