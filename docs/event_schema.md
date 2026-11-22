# Input contract

Every Kafka value must be a UTF-8 JSON object with nonempty `event_id`,
`event_type`, `line_id`, and `event_time`. IDs are strings. New timestamps use
UTC ISO 8601 (for example `2026-01-01T08:50:00Z`). Legacy numeric epoch seconds
are accepted at input and normalized to UTC strings before indexing. Naive
local timestamps are rejected. Counters must be nonnegative integers; booleans
and fractional numbers are rejected.

Required fields by type:

| Type | Additional required fields | Allowed values |
| --- | --- | --- |
| line_state | state, state_start_time, reason_code | RUN, IDLE, DOWN; reason_code may be null |
| production | station_id, batch_id, good_count_inc, cycles_count_inc | Integer counters >= 0 |
| reject | station_id, batch_id, unit_id, reject_reason, material_lot_id | Nonempty strings |
| qc_inspection | station_id, batch_id, unit_id, inspection_type, result | VISION or CHECKWEIGHER; PASS or FAIL |
| station_state | station_id, batch_id, unit_id, action | ENTER or EXIT |

Unknown types, invalid JSON, invalid fields and timestamps go to
`dead_letter_queue`. Each document contains `raw_message`, `rejection_reason`,
`rejected_at`, and `kafka` coordinates (`topic`, `partition`, `offset`). Its ID
uses the original nonempty event_id if available, otherwise `topic:partition:offset`.
Replaying the same malformed record overwrites that document.
Line-state temporal checks also route rejected events to this index.

The consumer installs the `coffee_events` index template before reading Kafka.
Identifiers and enum fields are keywords; dates use `strict_date_optional_time`.
The template applies to newly created indices; existing indices with incompatible
mappings must be reindexed before using this contract.

Run offline contract and routing checks with `python3 -m unittest discover -s tests -v`.

# Events

#### `dead_letter_queue`
Rejected events with reason. Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 (original event) |
| event_type | string | Original event type |
| line_id | string | |
| state | string | Original state |
| state_start_time | date | |
| event_time | date | |
| reason_code | string | |
| rejection_reason | string | Why it was rejected |
| rejected_at | date | When consumer rejected it |

#### `line_status`
Sent every 5 seconds
1 document per line, always current state. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "line_state" |
| line_id | string | e.g. "line_1" |
| state | string | RUN / IDLE / DOWN / STALE |
| state_start_time | date | UTC ISO 8601 |
| event_time | date | UTC ISO 8601 |
| reason_code | string | Only when DOWN, null otherwise |
| time_in_state | float | Seconds in current state |

#### `production`
Sent every 5 seconds when state=RUN
| Field | Type | Description |
|------|------|-------------|
| event_id | string | uuid4, e.g. "39f272d2-d65b-4bee-b29a-c0a5becd5726" |
| event_type | string | value "production" |
| line_id | string | Ex: "line_2" |
| station_id | string | Ex: "station_2" |
| good_count_inc | int | Incremental |
| cycles_count_inc | int | Incremental |
| batch_id | string | Ex: "batch_001" |
| event_time | date | UTC ISO 8601 |

#### `production_summary`
1 document per line, running totals. Upsert.
 
| Field | Type | Description |
|-------|------|-------------|
| line_id | string | |
| total_good_units | long | Cumulative units produced |
| total_cycles | long | Cumulative cycles |
| last_updated | date | UTC ISO 8601 |

#### `reject_events`
Sent when a unit fails quality check (~5% chance in simulator; if state=RUN)
One document per rejected unit. Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "reject" |
| line_id | string | |
| station_id | string | |
| unit_id | string | e.g. "unit_000042" |
| reject_reason | string | e.g. "UNDERFILL", "SEAL_DEFECT" |
| material_lot_id | string | e.g. "lot_001" |
| batch_id | string | |
| event_time | date | UTC ISO 8601 |

#### `reject_summary`
1 document per line. Upsert.
 
| Field | Type | Description |
|-------|------|-------------|
| line_id | string | |
| total_rejects | long | Cumulative rejects |
| total_good_units | long | Cumulative good units |
| reject_rate | float | Percentage |
| last_updated | date | UTC ISO 8601 |

#### `state_intervals`
One document per state transition. Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| line_id | keyword | |
| state | keyword | RUN / IDLE / DOWN |
| start_time | date (UTC ISO 8601) | Interval start |
| end_time | date (UTC ISO 8601) | Interval end |
| duration_seconds | float | |
| duration_minutes | float | |
| reason_code | keyword | Only for DOWN, null otherwise |

#### `qc_events`
Sent for each unit inspected (VISION / CHECKWEIGHER)
Two documents per unit (VISION + CHECKWEIGHER). Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "qc_inspection" |
| line_id | string | |
| station_id | string | "WEIGHER" or "SEALER" |
| unit_id | string | |
| inspection_type | string | "VISION" or "CHECKWEIGHER" |
| result | string | "PASS" or "FAIL" |
| batch_id | string | |
| event_time | date | UTC ISO 8601 |

#### `station_state`
Sent when a unit enters/exits a station. One event per action per station.

| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "station_state" |
| line_id | string | e.g. "line_1" |
| station_id | string | FILLER / SEALER / WEIGHER / PACKER |
| unit_id | string | e.g. "unit_000042" |
| batch_id | string | e.g. "batch_001" |
| action | string | "ENTER" or "EXIT" |
| event_time | date | UTC ISO 8601 |

<br><br>

# Summary Indexes

These are upserted documents (1 per line), not raw events.

#### `throughput_summary`
1 document per line. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| line_id | string | e.g. "line_1" |
| throughput_per_min | float | Actual units per RUN minute |
| spec_performance_pct | float | (actual / declared) * 100 |
| declared_output_per_min | int | From config (26) |
| total_run_minutes | float | Cumulative RUN minutes |
| last_updated | date | UTC ISO 8601 |

#### `cycles_summary`
1 document per line. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| line_id | string | e.g. "line_1" |
| avg_cycles_per_min | float | total_cycles / total_run_minutes |
| cycles_vs_spec_pct | float | (actual / declared) * 100 |
| last_updated | date | UTC ISO 8601 |

#### `qc_summary`
1 document per line. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| line_id | string | e.g. "line_1" |
| qc_coverage_pct | float | (fully_inspected / produced) * 100 |
| fully_inspected_count | int | Units with BOTH VISION and CHECKWEIGHER |
| total_produced | int | Total good units produced |
| last_updated | date | UTC ISO 8601 |

#### `station_events`
One document per station action. Append.

| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "station_state" |
| line_id | string | |
| station_id | string | FILLER / SEALER / WEIGHER / PACKER |
| unit_id | string | |
| batch_id | string | |
| action | string | ENTER / EXIT |
| event_time | date (UTC ISO 8601) | |