from kafka import KafkaConsumer
import requests, json, time, uuid

# Config
with open("config/reason_codes.json") as f:
    reason_codes = json.load(f)
with open("config/line_spec.json") as f:
    line_spec = json.load(f)

VALID_REASON_CODES = {r["code"] for r in reason_codes}

# State (per line)
last_event_time = {}    # for out-of-order check
last_seen = {}          # last time a message arrived (for STALE detection)
previous_state = {}     # {state, start_time, reason_code} for interval computation
total_good_units = {}   
total_cycles = {}       
total_rejects = {}      
seen_ids = set()        # for dedup. Resets to 0 after 10k for memory saving
total_run_minutes = {}  # for throughput deviation

# Kafka
consumer = KafkaConsumer(
    "line_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    consumer_timeout_ms=30000,
)

# OpenSearch utils
def upsert(event, index_name):
    doc_id = event["line_id"]
    url = f"http://localhost:9200/{index_name}/_update/{doc_id}"
    body = {"doc": event, "doc_as_upsert": True}
    r = requests.post(url, json=body)
    print(r.json())

def append(event, index_name):
    doc_id = event["event_id"]
    url = f"http://localhost:9200/{index_name}/_doc/{doc_id}"
    r = requests.put(url, json=event)
    print(r.json())

def send_to_dlq(event, reason):
    event["rejection_reason"] = reason
    event["rejected_at"] = time.time()
    append(event, "dead_letter_queue")

# Validation (line_state only)
def validate(event):
    t_start = event["state_start_time"]
    t_event = event["event_time"]
    t_now = time.time()
    line_id = event["line_id"]

    if t_start > t_now:
        return False, "state_start_time is in the future"
    if t_event < t_start:
        return False, "event_time is before state_start_time"
    if line_id in last_event_time and t_event <= last_event_time[line_id]:
        return False, "event_time is older than last seen"
    return True, None

# Helpers
def _inc(d, key, value=1):
    # Increment a counter in a dictionary
    d[key] = d.get(key, 0) + value

def _validate_reason_code(reason_code, state):
    # Return a reason_code (UNKNOWN_REASON if DOWN and invalid)
    if state == "DOWN" and reason_code not in VALID_REASON_CODES:
        return "UNKNOWN_REASON"
    return reason_code

# Handlers
def handle_line_state(event):
    is_valid, reason = validate(event)
    if not is_valid:
        send_to_dlq(event, reason)
        return

    line_id = event["line_id"]
    event["time_in_state"] = event["event_time"] - event["state_start_time"]
    upsert(event, "line_status")

    last_event_time[line_id] = event["event_time"]
    last_seen[line_id] = time.time()

    # Build interval on state change
    if previous_state:
        if event["state"] != previous_state["state"]:
            duration = event["event_time"] - previous_state["start_time"]
            interval = {
                "event_id": str(uuid.uuid4()),
                "line_id": line_id,
                "state": previous_state["state"],
                "start_time": previous_state["start_time"],
                "end_time": event["event_time"],
                "duration_seconds": duration,
                "duration_minutes": duration / 60,
                "reason_code": _validate_reason_code(
                    previous_state["reason_code"], previous_state["state"]
                ),
            }
            append(interval, "state_intervals")
            if interval["state"] == "RUN":
                _inc(total_run_minutes, line_id, interval["duration_minutes"])
            previous_state.update({
                "state": event["state"],
                "start_time": event["state_start_time"],
                "reason_code": event["reason_code"],
            })
    else:
        previous_state.update({
            "line_id": line_id,
            "state": event["state"],
            "start_time": event["event_time"],
            "reason_code": _validate_reason_code(
                event["reason_code"], event["state"]
            ),
        })

def handle_production(event):
    line_id = event["line_id"]
    _inc(total_good_units, line_id, event["good_count_inc"])
    _inc(total_cycles, line_id)

    append(event, "production_events")
    upsert({
        "line_id": line_id,
        "total_good_units": total_good_units[line_id],
        "total_cycles": total_cycles[line_id],
        "last_updated": time.time(),
    }, "production_summary")
    
    run_min = total_run_minutes.get(line_id, 0)
    if run_min > 0:
        throughput = total_good_units[line_id] / run_min
        declared = line_spec["declared_output_per_min"]
        spec_pct = round(throughput / declared * 100, 1)
        if spec_pct > 110:
            print(f"WARNING: throughput {spec_pct}% exceeds 110% of spec")
        print(f"Throughput: {spec_pct}% of spec")
        upsert({
            "line_id": line_id,
            "throughput_per_min": throughput,
            "spec_performance_pct": spec_pct,
            "declared_output_per_min": declared,
            "total_run_minutes": run_min,
            "last_updated": time.time()
        }, "throughput_summary")
    
    # Cycles vs spec
    if run_min > 0:
        avg_cycles = total_cycles[line_id] / run_min
        declared_cycles = line_spec["declared_cycles_per_min"]
        cycles_pct = round(avg_cycles / declared_cycles * 100, 1)
        if total_cycles[line_id] > 0 and total_good_units[line_id] == 0:
            print(f"WARNING: {total_cycles[line_id]} cycles but 0 units --> suspicious")
        print(f"Cycles: {cycles_pct}% of spec")
        upsert({
            "line_id": line_id,
            "avg_cycles_per_min": round(avg_cycles, 2),
            "cycles_vs_spec_pct": cycles_pct,
            "last_updated": time.time(),
        }, "cycles_summary")
        

def handle_reject(event):
    line_id = event["line_id"]
    _inc(total_rejects, line_id)

    good = total_good_units.get(line_id, 0)
    rejects = total_rejects[line_id]
    total = good + rejects
    reject_rate = round(rejects / total * 100, 1) if total > 0 else 0

    append(event, "reject_events")
    upsert({
        "line_id": line_id,
        "total_rejects": rejects,
        "total_good_units": good,
        "reject_rate": reject_rate,
        "last_updated": time.time(),
    }, "reject_summary")

def handle_qc(event):
    append(event, "qc_events")

# Routing
HANDLERS = {
    "line_state": handle_line_state,
    "production": handle_production,
    "reject": handle_reject,
    "qc_inspection": handle_qc,
}

# Main loop
while True:
    for message in consumer:
        event = message.value
        event_id = event.get("event_id")

        # Dedup
        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)
        if len(seen_ids) > 10000:
            seen_ids.clear()

        # Routing
        event_type = event.get("event_type", "unknown")
        handler = HANDLERS.get(event_type)
        if handler:
            handler(event)
        else:
            print(f"Unknown event_type: {event_type}")

    # Stale detection
    now = time.time()
    for line_id, ts in last_seen.items():
        if now - ts > 30:
            upsert({
                "line_id": line_id,
                "state": "STALE",
                "event_time": now,
                "state_start_time": ts,
                "time_in_state": now - ts,
            }, "line_status")