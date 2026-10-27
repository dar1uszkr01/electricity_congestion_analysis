# Databricks notebook source
# MAGIC %md
# MAGIC # MET Norway weather ingestion
# MAGIC Locationforecast 2.0 point forecasts, using the compact endpoint (no API key).
# MAGIC Run on Databricks with Spark, Delta Lake, Unity Catalog and `requests`.
# MAGIC Configure a real application contact, target catalog/schema and WGS84 locations.
# MAGIC Output: a raw Delta history table and a forecast view retaining forecast vintages.
# MAGIC Use one job writer (maximum concurrent runs = 1). Schedule with an offset from the hour.
# MAGIC Forecasts are not historical observations; archive successive runs to build history.
# MAGIC Source: https://api.met.no/doc/locationforecast/HowTO
# MAGIC Attribution: MET Norway, https://www.met.no/ (CC BY 4.0,
# MAGIC https://creativecommons.org/licenses/by/4.0/). Retain attribution downstream.

# COMMAND ----------
# 1. Import libraries

import hashlib
import json
import math
import random
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------
# 2. Set variables

WIDGET_DEFAULTS = {
    "catalog": "dbw_dariusz",
    "schema": "bronze",
    "application_contact": "",  # Real company email or URL with contact information.
    "locations_json": json.dumps([
        {"location_id": "stavanger_example", "latitude": 58.9700, "longitude": 5.7331},
    ]),
}
for widget_name, default_value in WIDGET_DEFAULTS.items():
    dbutils.widgets.text(widget_name, default_value)

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
APPLICATION_CONTACT = dbutils.widgets.get("application_contact").strip()
LOCATIONS = json.loads(dbutils.widgets.get("locations_json"))

API_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
USER_AGENT = f"WeatherForecastIngestion/1.0 {APPLICATION_CONTACT}"
REQUEST_TIMEOUT = (10, 60)  # Connect timeout and read timeout, seconds.
REQUEST_INTERVAL_SECONDS = 1
CACHE_FALLBACK_MINUTES = 10  # Used only when MET omits Expires.

RAW_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`met_norway_weather_raw`"
FORECAST_VIEW = f"`{CATALOG}`.`{SCHEMA}`.`met_norway_weather_forecasts`"
RAW_SCHEMA = """
    record_id STRING, request_key STRING, location_id STRING,
    latitude DOUBLE, longitude DOUBLE, source_url STRING,
    source_updated_at TIMESTAMP, first_ingested_at TIMESTAMP,
    last_checked_at TIMESTAMP, expires_at TIMESTAMP,
    last_modified STRING, http_status INT, raw_payload STRING
"""
FORECAST_JSON_SCHEMA = """
    STRUCT<properties: STRUCT<
        meta: STRUCT<units: MAP<STRING, STRING>>,
        timeseries: ARRAY<STRUCT<time: STRING, data: MAP<STRING,
            STRUCT<details: MAP<STRING, DOUBLE>, summary: MAP<STRING, STRING>>>
        >>
    >>
"""

# COMMAND ----------
# 3. Define common functions

def utc_now():
    return datetime.now(timezone.utc)


def parse_http_date(value):
    if not value:
        return None
    result = parsedate_to_datetime(value)
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def prepare_locations(locations):
    """Validate all configuration before sending requests or creating tables."""
    if not isinstance(locations, list) or not locations:
        raise ValueError("locations_json must be a nonempty JSON array")
    result, ids, coordinates = [], set(), set()
    for location in locations:
        location_id = location.get("location_id")
        if not isinstance(location_id, str) or not location_id.strip() or location_id in ids:
            raise ValueError("Each location_id must be a unique, nonempty string")
        lat, lon = location.get("latitude"), location.get("longitude")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in (lat, lon)):
            raise ValueError(f"{location_id}: latitude/longitude must be numbers")
        if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f"{location_id}: invalid WGS84 latitude/longitude")
        # MET permits at most four decimal places in request coordinates.
        lat, lon = round(lat, 4), round(lon, 4)
        if (lat, lon) in coordinates:
            raise ValueError("Use one location_id per coordinate pair")
        ids.add(location_id)
        coordinates.add((lat, lon))
        result.append({"location_id": location_id, "latitude": lat, "longitude": lon})
    return result


def create_session():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    retries = Retry(
        total=4, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def latest_cached_record(request_key):
    timestamp_columns = (
        "source_updated_at", "first_ingested_at", "last_checked_at", "expires_at",
    )
    rows = (
        spark.table(RAW_TABLE)
        .where(F.col("request_key") == request_key)
        .orderBy(F.col("last_checked_at").desc(), F.col("record_id").desc())
        .select(*[
            F.col(name).cast("long").alias(name) if name in timestamp_columns else F.col(name)
            for name in spark.table(RAW_TABLE).columns
        ])
        .limit(1).collect()
    )
    if not rows:
        return None
    record = rows[0].asDict()
    # Epoch conversion avoids the driver's local timezone during collection.
    for name in timestamp_columns:
        epoch = record[name]
        record[name] = datetime.fromtimestamp(epoch, timezone.utc) if epoch is not None else None
    return record


def save_record(record):
    source = spark.createDataFrame([record], schema=RAW_SCHEMA)
    (
        DeltaTable.forName(spark, RAW_TABLE).alias("target")
        .merge(source.alias("source"), "target.record_id = source.record_id")
        .whenMatchedUpdate(set={
            "last_checked_at": "source.last_checked_at",
            "expires_at": "source.expires_at",
            "last_modified": "source.last_modified",
            "http_status": "source.http_status",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


def ingest_location(session, location):
    params = {"lat": f"{location['latitude']:.4f}", "lon": f"{location['longitude']:.4f}"}
    source_url = requests.Request("GET", API_URL, params=params).prepare().url
    # Include the location label so renaming a location cannot reuse stale metadata.
    request_key = hashlib.sha256(
        f"{location['location_id']}|{source_url}".encode("utf-8")
    ).hexdigest()
    cached = latest_cached_record(request_key)
    now = utc_now()
    if cached and cached["expires_at"] and now < cached["expires_at"]:
        print(f"{location['location_id']}: cached until {cached['expires_at'].isoformat()}; skipped")
        return "cached"

    headers = {}
    if cached and cached["last_modified"]:
        headers["If-Modified-Since"] = cached["last_modified"]
    time.sleep(REQUEST_INTERVAL_SECONDS)
    with session.get(source_url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        checked_at = utc_now()
        expires_at = parse_http_date(response.headers.get("Expires"))
        if expires_at is None:
            expires_at = checked_at + timedelta(minutes=CACHE_FALLBACK_MINUTES)
            print(f"{location['location_id']}: missing Expires; using a conservative fallback")

        if response.status_code == 304:
            if cached is None:
                raise ValueError("Received HTTP 304 without a stored payload")
            record = dict(cached)
            # Preserve the original payload and its first ingestion timestamp.
            # Only these metadata fields are updated by the Delta merge.
            record.update(last_checked_at=checked_at, expires_at=expires_at, http_status=304)
            record["last_modified"] = response.headers.get("Last-Modified") or cached["last_modified"]
            save_record(record)
            print(f"{location['location_id']}: HTTP 304; cache metadata updated")
            return "unchanged"

        if response.status_code not in (200, 203):
            raise ValueError(f"Unexpected HTTP status: {response.status_code}")
        if response.status_code == 203:
            print("WARNING: MET returned HTTP 203; review product deprecation or beta status")
        payload = response.json()
        properties = payload["properties"]
        if not isinstance(properties.get("timeseries"), list) or not properties["timeseries"]:
            raise ValueError("Response has no forecast timeseries")
        updated_at = datetime.fromisoformat(properties["meta"]["updated_at"].replace("Z", "+00:00"))
        if updated_at.tzinfo is None:
            raise ValueError("Forecast update timestamp must include a timezone")
        canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        record = {
            "record_id": hashlib.sha256(f"{request_key}|{canonical_payload}".encode("utf-8")).hexdigest(),
            "request_key": request_key,
            **location,
            "source_url": source_url,
            "source_updated_at": updated_at,
            "first_ingested_at": checked_at,
            "last_checked_at": checked_at,
            "expires_at": expires_at,
            "last_modified": response.headers.get("Last-Modified"),
            "http_status": response.status_code,
            "raw_payload": response.text,
        }
        save_record(record)
        print(f"{location['location_id']}: stored forecast updated {updated_at.isoformat()}")
        return "received"

# COMMAND ----------
# 4. Main code

for identifier in (CATALOG, SCHEMA):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError("Catalog/schema must contain only letters, digits and underscores")
if not APPLICATION_CONTACT or "example.com" in APPLICATION_CONTACT.lower():
    raise ValueError("Set application_contact to a real company email or contact website")
locations = prepare_locations(LOCATIONS)

spark.conf.set("spark.sql.session.timeZone", "UTC")
# The catalog must already exist; the job identity needs schema/table/view permissions.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE TABLE IF NOT EXISTS {RAW_TABLE} ({RAW_SCHEMA}) USING DELTA")
spark.sql(f"""
    CREATE OR REPLACE VIEW {FORECAST_VIEW} AS
    WITH parsed AS (
        SELECT *, from_json(raw_payload, '{FORECAST_JSON_SCHEMA}') AS forecast
        FROM {RAW_TABLE}
    ), expanded AS (
        SELECT record_id, location_id, latitude, longitude, source_updated_at,
               first_ingested_at, forecast.properties.meta.units AS units,
               explode(forecast.properties.timeseries) AS point
        FROM parsed
    )
    SELECT record_id, location_id, latitude, longitude, source_updated_at,
           first_ingested_at, to_timestamp(point.time) AS forecast_time_utc, units,
           point.data['instant'].details AS instant_details,
           point.data['next_1_hours'].details AS next_1_hour_details,
           point.data['next_6_hours'].details AS next_6_hours_details,
           point.data['next_12_hours'].details AS next_12_hours_details,
           point.data['next_1_hours'].summary['symbol_code'] AS next_1_hour_symbol
    FROM expanded
""")

print(f"Starting MET Norway ingestion for {len(locations)} location(s)")
print(f"Raw table: {RAW_TABLE}; forecast view: {FORECAST_VIEW}")
counts = {"cached": 0, "unchanged": 0, "received": 0}
failures = []
# Spread starts across a small window in addition to offsetting the job schedule.
time.sleep(random.uniform(0, 30))
with create_session() as session:
    for location in locations:
        try:
            counts[ingest_location(session, location)] += 1
        except Exception as exc:
            failures.append(location["location_id"])
            print(f"ERROR: {location['location_id']}: {type(exc).__name__}: {exc}")

print(f"Finished: {counts}; failed locations: {len(failures)}")
if failures:
    raise RuntimeError(f"Ingestion failed for {failures}; successful writes are retained. Rerun to retry.")
