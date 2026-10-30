# MET Norway weather ingestion

Import `met_norway_weather.ipynb` into Databricks, or open the equivalent
`met_norway_weather.py` source notebook in a Databricks Git folder. Both are
self-contained and use exactly four code sections:

1. Import libraries
2. Set variables
3. Define common functions
4. Main code

Use a Spark/Delta runtime with Unity Catalog and `requests`. Configure the
`application_contact` widget with your real company email or a website where
MET can contact you. The API does not require a key. Set `catalog`, `schema`
and `locations_json`; the catalog must exist and the job identity must have
permission to create the schema, table and view. The default Stavanger point
is an example, not a bidding-zone average. Coordinates are WGS84 latitude and
longitude, rounded to four decimal places.

Outputs in the selected catalog/schema (default `dbw_dariusz.bronze`):

- `met_norway_weather_raw`: one row per distinct payload and configured location,
  with raw JSON, request URL, forecast issue time, ingestion timestamps and
  persistent HTTP cache metadata. Identical payloads update cache metadata
  without adding duplicate rows. Previous forecast versions remain available.
- `met_norway_weather_forecasts`: a view with one row per forecast timestamp and
  version. Weather values are maps, including `instant_details['air_temperature']`,
  `instant_details['wind_speed']` and `next_1_hour_details['precipitation_amount']`.
  The `units` map preserves MET's units. Missing intervals remain null. Each
  precipitation map describes its named interval starting at `forecast_time_utc`;
  do not sum overlapping 1, 6 and 12 hour intervals.

```sql
SELECT location_id, source_updated_at, forecast_time_utc,
       instant_details['air_temperature'] AS air_temperature,
       units['air_temperature'] AS temperature_unit,
       next_1_hour_details['precipitation_amount'] AS precipitation_next_hour
FROM dbw_dariusz.bronze.met_norway_weather_forecasts
ORDER BY location_id, source_updated_at DESC, forecast_time_utc;
```

The view contains all archived forecast versions. Filter to the desired issue
time before aggregating. This endpoint supplies forecasts, not historical
observations or backfills. Polling from now onward builds forecast history.

Run a single job writer with maximum concurrent runs set to 1. Schedule with an
offset from the hour; the notebook also adds a small random initial delay and
spaces requests. It skips requests until cached `Expires`, then uses the stored
`Last-Modified` in `If-Modified-Since`. HTTP 304 refreshes metadata without
replacing the payload. Retryable errors use backoff and honor `Retry-After`.
Each successful location commits separately. Failures print details and fail
the job after remaining locations have been attempted; rerun to resume.
All application status messages use `print`.

References: [Locationforecast HOWTO](https://api.met.no/doc/locationforecast/HowTO),
[API terms](https://docs.api.met.no/doc/TermsOfService).
Attribute MET Norway in downstream outputs under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and indicate changes.
