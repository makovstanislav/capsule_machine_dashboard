# Reject breakdowns

Import `visualizations/rejects.ndjson` in Dashboards → Stack Management → Saved
Objects. It adds **Reject reasons** (Pareto) and **Rejects by station**. Both use
`capsule_units`, the same release-time range and line filters as the reject KPI.
You can also paste the individual JSON specs into a Vega visualization.

Each released FAIL capsule contributes exactly once. The primary defect is the
first failure by event timestamp, with event ID breaking ties. Its reason and
station stay paired, regardless of delivery order or duplicate records. A later
PASS cannot clear the failure. Production now carries `reject_station_id` from
the same scenario as its failed QC check and reject event.

Unknown or missing codes become `UNKNOWN_REASON` / `UNKNOWN_STATION`, including
in raw reject documents. Previously stored outcomes without a station appear in
the unknown bucket; replay their events to apply normalization and primary-defect
selection. An older producer without `reject_station_id` also uses UNKNOWN_STATION
for a failed production event. Restart the producer after upgrading.

For a checked breakdown: `python3 rejects.py line_1 --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z`.
It verifies both bucket sums against total rejects. The Pareto sorts by descending
count and ends at 100%; an empty range has no bars or percentage.
