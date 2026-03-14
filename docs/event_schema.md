# Events

#### `line_state`
sent every 5 seconds

| Field| Type | Description | 
|-----------|------------|--------|
|event_id|string|uuid4, e.g. "39f272d2-d65b-4bee-b29a-c0a5becd5726"|
|event_type|string|value "line_state"|
|line_id|string|Ex: "line_1"|
|state|string|RUN / IDLE / DOWN / STALE (if no message for 30 sec)|
|state_start_time|float|Ex: 1773493218.3043132|
|event_time|float|Ex: 1773493218.304907|
|reason_code|string|only when DOWN, null otherwise. E.g. "MECHANICAL_FAILURE", "CLEANING", "ELECTRICAL_FAULT"|
|time_in_state|float|if no message for 30 sec|

#### `production`
sent every 5 seconds when state=RUN
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



#### `reject`
sent when a unit fails quality check (~5% chance in simulator; if state=RUN)
TBD when implementing metrics 7-12

#### `qc_inspection`
sent for each unit inspected (VISION / CHECKWEIGHER)
TBD when implementing metrics 7-12

#### `station_state`
sent when a unit enters/exits a station
TBD when implementing metrics 7-12
