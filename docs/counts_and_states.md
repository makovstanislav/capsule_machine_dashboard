# Capsule counts

Each production event now describes one capsule. Its ID includes the producer run
and line, so a producer restart cannot reuse it. Production, inspections and rejects
share this ID, batch and material lot. PASS/FAIL and the reject reason come from the
same simulated outcome. Restart both Python processes when upgrading; production
messages from the older counter-only producer do not satisfy this contract.

`capsule_units` holds one outcome per `(line_id, unit_id)`. FAIL wins over PASS,
including when a reject arrives before production. Reject-only placeholders are
excluded until a production event confirms release. Counts come from these stored
units, so replaying production or reject records does not increase the result.
Cycles are stored separately in `cycle_events` using a stable cycle ID.

Query a line and optional half-open release-time range:

```sh
python3 production.py line_1 --start 2026-01-01T09:00:00Z --end 2026-01-01T10:00:00Z
```

The result includes total produced, good, rejected and reject percentage. An empty
range returns `null` for the percentage (N/A). Ten capsules with two rejects give
8 good and 20% rejects. Ten more good capsules reduce the percentage to 10%.
Consumer summaries cover all stored releases for each line and update on both
production and rejects. Filtered queries use release time, including for late rejects.

# Line states

State is tracked separately per line. Intervals start at `state_start_time` and
end at the next transition. An open interval only extends to its latest confirmed
heartbeat; silence contributes no additional duration. Document IDs are stable,
so repeated heartbeats update the interval instead of adding another one.
A DOWN reason is selected once on entry and reused until the next transition.

`state_hours` splits confirmed intervals into UTC hours. In Dashboards, create a
Vega visualization and paste `visualizations/downtime.json`. Add a `line_id` filter
and choose the desired time range. The chart clips each segment to that range
before summing minutes. For DOWN 08:50–09:10 the bars show 10 and 10 minutes;
08:55–09:05 gives 10 minutes total. Both ranges use `[start, end)` boundaries.

Freshness is checked every second between bounded Kafka polls and while processing
messages. After 30 seconds without an up-to-date heartbeat, only that line becomes
STALE. Its `last_known_state` and confirmed interval remain intact. A new current
heartbeat restores the reported state. HTTP failures stop processing visibly.

# Checks

```sh
python3 -m unittest discover -s tests -v
RUN_INTEGRATION=1 python3 -m unittest discover -s tests -p test_storage.py -v
```

The second command uses local OpenSearch and creates and removes only indices and
an index template with a random `coffee_test_` prefix. It checks unique counts,
reject conflicts, replay, release-time filters, and the chart aggregation.
