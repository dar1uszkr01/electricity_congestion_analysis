"""NVE GridTimeSeries client and normalization, independent of Spark."""

from datetime import date, datetime, timedelta
import math
import re

BASE_URL = "https://gts.nve.no/api"
DEFAULT_THEMES = ("swe", "sd", "qtt", "gwb_q", "rr", "tm")


def date_windows(start, end, days=366):
    """Yield non-overlapping, inclusive date windows."""
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start > end or days < 1:
        raise ValueError("Start must be <= end and window days must be positive")
    while start <= end:
        stop = min(end, start + timedelta(days=days - 1))
        yield start.isoformat(), stop.isoformat()
        start = stop + timedelta(days=1)


def create_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), respect_retry_after_header=True,
    )))
    return session


def get_json(session, path):
    response = session.get(f"{BASE_URL}/{path}", timeout=(10, 90))
    response.raise_for_status()
    return response.json()


def validate_locations(locations):
    if not locations:
        raise ValueError("Configure at least one UTM33 location")
    ids, coordinates = set(), set()
    for location in locations:
        name = location.get("location_id")
        x, y = location.get("x"), location.get("y")
        if not isinstance(name, str) or not name.strip() or name in ids:
            raise ValueError("Location IDs must be nonempty and unique")
        if type(x) is not int or type(y) is not int:
            raise ValueError("x and y must be integer UTM33 coordinates, not longitude/latitude")
        if (x, y) in coordinates:
            raise ValueError("Duplicate coordinates: use one location ID per point")
        ids.add(name)
        coordinates.add((x, y))


def select_themes(metadata, themes, start):
    if not themes or len(set(themes)) != len(themes):
        raise ValueError("Choose at least one theme, without duplicates")
    available = {item["Name"]: item for item in metadata}
    for theme in themes:
        if not re.fullmatch(r"[A-Za-z0-9_]+", theme) or theme not in available:
            raise ValueError(f"Unknown NVE theme: {theme}")
        info = available[theme]
        if info["TimeResolutionInMinutes"] != 1440:
            raise ValueError(f"This ingestion supports daily themes only: {theme}")
        if date.fromisoformat(start) < date.fromisoformat(info["FirstDateInTimeSerie"]):
            raise ValueError(f"{theme} starts at {info['FirstDateInTimeSerie']}")
    return {theme: available[theme] for theme in themes}


def parse_source_time(value):
    # NVE's documented response uses dd.MM.yyyy HH:mm:ss; some layers omit seconds.
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported NVE source time: {value!r}")


def normalize(payload, location, theme, start, end):
    """Preserve source wall-clock times and units; never infer UTC or rescale values."""
    if payload["Theme"] != theme or (payload["X"], payload["Y"]) != (location["x"], location["y"]):
        raise ValueError("NVE returned a different theme or coordinate")
    resolution = payload["TimeResolution"]
    if resolution != 1440:
        raise ValueError(f"Expected daily data, received {resolution} minutes")
    first, last = parse_source_time(payload["StartDate"]), parse_source_time(payload["EndDate"])
    if first.date() != date.fromisoformat(start) or last.date() != date.fromisoformat(end):
        raise ValueError("NVE returned a different date range; refusing an incomplete batch")
    data = payload["Data"]
    expected = (last - first).total_seconds() / (resolution * 60) + 1
    if not isinstance(data, list) or len(data) != expected:
        raise ValueError("NVE Data length does not match StartDate/EndDate/TimeResolution")
    sentinel = payload["NoDataValue"]
    rows = []
    for index, raw_value in enumerate(data):
        missing = raw_value is None or raw_value == sentinel
        value = None if missing else float(raw_value)
        if value is not None and not math.isfinite(value):
            raise ValueError("Non-finite NVE observation")
        observed = first + timedelta(minutes=index * resolution)
        rows.append({
            "location_id": location["location_id"], "x": location["x"], "y": location["y"],
            "theme": theme, "theme_name": payload["FullName"],
            "source_time": observed.isoformat(), "observation_date": observed.date(),
            "time_resolution_minutes": resolution, "value": value,
            "is_missing": missing, "no_data_value": float(sentinel),
            "unit": payload["Unit"],
            "altitude_m": None if payload.get("Altitude") is None else float(payload["Altitude"]),
        })
    return rows
