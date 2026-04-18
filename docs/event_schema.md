# Events

#### `dead_letter_queue`
Rejected events with reason. Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 (original event) |
| event_type | string | Original event type |
| line_id | string | |
| state | string | Original state |
| state_start_time | float | |
| event_time | float | |
| reason_code | string | |
| rejection_reason | string | Why it was rejected |
| rejected_at | float | When consumer rejected it |

#### `line_status`
Sent every 5 seconds
1 document per line, always current state. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| event_type | string | "line_state" |
| line_id | string | e.g. "line_1" |
| state | string | RUN / IDLE / DOWN / STALE |
| state_start_time | float | Unix timestamp |
| event_time | float | Unix timestamp |
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
| event_time | float | Ex: 1773493218.304907 |

#### `production_summary`
1 document per line, running totals. Upsert.
 
| Field | Type | Description |
|-------|------|-------------|
| line_id | string | |
| total_good_units | long | Cumulative units produced |
| total_cycles | long | Cumulative cycles |
| last_updated | float | Unix timestamp |

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
| event_time | float | Unix timestamp |

#### `reject_summary`
1 document per line. Upsert.
 
| Field | Type | Description |
|-------|------|-------------|
| line_id | string | |
| total_rejects | long | Cumulative rejects |
| total_good_units | long | Cumulative good units |
| reject_rate | float | Percentage |
| last_updated | float | Unix timestamp |

#### `state_intervals`
One document per state transition. Append.
 
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | uuid4 |
| line_id | keyword | |
| state | keyword | RUN / IDLE / DOWN |
| start_time | date (epoch_second) | Interval start |
| end_time | date (epoch_second) | Interval end |
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
| event_time | float | Unix timestamp |

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
| event_time | float | Unix timestamp |

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
| last_updated | float | Unix timestamp |

#### `cycles_summary`
1 document per line. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| line_id | string | e.g. "line_1" |
| avg_cycles_per_min | float | total_cycles / total_run_minutes |
| cycles_vs_spec_pct | float | (actual / declared) * 100 |
| last_updated | float | Unix timestamp |

#### `qc_summary`
1 document per line. Upsert.

| Field | Type | Description |
|-------|------|-------------|
| line_id | string | e.g. "line_1" |
| qc_coverage_pct | float | (fully_inspected / produced) * 100 |
| fully_inspected_count | int | Units with BOTH VISION and CHECKWEIGHER |
| total_produced | int | Total good units produced |
| last_updated | float | Unix timestamp |

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
| event_time | date (epoch_second) | |