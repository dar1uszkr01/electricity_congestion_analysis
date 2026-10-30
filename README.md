# electricity_congestion_analysis
Hobby project that focuses on electricity congestion analysis.

## NVE ingestion

- `ingestion/nve/nve_reservoir_statistics.ipynb`: reservoir statistics and area metadata.
- [Snow and hydrology notebook](ingestion/nve/nve_snow_hydrology.ipynb): daily snow water equivalent, snow depth, rain plus snowmelt, modelled runoff, precipitation, and temperature from [NVE GridTimeSeries](https://api.nve.no/doc/gridtimeseries-data-gts/).

Import the snow notebook and its companion `nve_snow_hydrology.py` into the same Databricks Git folder. Run on a Spark/Delta runtime with Unity Catalog and `requests`. Configure the notebook widgets for catalog/schema (default `dbw_dariusz.bronze`), inclusive dates, themes, and UTM33 sampling locations. The included point is an example, not a representative bidding-zone average. No API key is needed.

The default refresh covers the previous 30 days through yesterday in Europe/Oslo. Set explicit dates for backfills; schedule daily with rolling dates for refreshes. Daily themes are checked against NVE's live catalogue, with a maximum of 366 days per request. The notebook upserts `nve_snow_hydrology` and `nve_snow_hydrology_raw`; rerunning overlapping dates updates existing observations and preserves older dates. Raw responses retain request details and source metadata. Writes commit per request, so rerun a failed job to finish it. Run a single writer for these tables.

Missing sentinels become null and are flagged. Units and source wall-clock times are preserved as returned by the standard API endpoint. These are modelled grid-cell data; station observations through HydAPI and catchment/bidding-zone aggregation are outside this ingestion.

Run local normalization tests (no Spark or network required):

```powershell
.venv\Scripts\python.exe -m unittest discover -s ingestion/nve -p 'test_*.py'
```

Attribute NVE in downstream outputs; its data is provided under NLOD, compatible with CC BY 3.0 Norway.
