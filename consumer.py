from kafka import KafkaConsumer
import requests, json, time
from datetime import datetime, timezone
from event_contract import validate_event
from production import counts, save_unit, document_id
from intervals import hourly
from rejects import normalize

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
seen_ids = set()        # for dedup. Resets to 0 after 10k for memory saving
total_run_minutes = {}  # for throughput deviation
inspected_units = {}    # set of inspection_types seen per unit_id
fully_inspected_count = 0

def parse_ts(ts):
    """Конвертирует ISO строку обратно в объект datetime"""
    if not ts: return None
    if isinstance(ts, (int, float)): return datetime.fromtimestamp(ts, tz=timezone.utc)
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))

def get_now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

# OpenSearch utils
def upsert(event, index_name):
    doc_id = event["line_id"]
    url = f"http://localhost:9200/{index_name}/_update/{doc_id}"
    body = {"doc": event, "doc_as_upsert": True}
    response = requests.post(url, json=body, timeout=10)
    response.raise_for_status()
    print(response.json())

def append(event, index_name):
    doc_id = event["event_id"]
    url = f"http://localhost:9200/{index_name}/_doc/{doc_id}"
    response = requests.put(url, json=event, timeout=10)
    response.raise_for_status()
    print(response.json())

def send_to_dlq(event, reason):
    event["rejection_reason"] = reason
    event["rejected_at"] = get_now_iso()
    append(event, "dead_letter_queue")

# Validation (line_state only)
def validate(event):
    t_start = parse_ts(event["state_start_time"])
    t_event = parse_ts(event["event_time"])
    t_now = datetime.now(timezone.utc)
    line_id = event["line_id"]

    if t_start > t_now:
        return False, "state_start_time is in the future"
    if t_event < t_start:
        return False, "event_time is before state_start_time"
    if line_id in last_event_time:
        if t_event <= parse_ts(last_event_time[line_id]):
            return False, "event_time is older than last seen"
    prior = previous_state.get(line_id)
    if prior and (prior["state"] != event["state"] or prior["start_time"] != event["state_start_time"]):
        if t_start < parse_ts(last_event_time[line_id]):
            return False, "state transition predates the last confirmed heartbeat"
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
    
    t_event = parse_ts(event["event_time"])
    t_start = parse_ts(event["state_start_time"])
    event["time_in_state"] = (t_event - t_start).total_seconds()
    
    prior = previous_state.get(line_id)
    if prior and prior["state"] == event["state"] and prior["start_time"] == event["state_start_time"]:
        event["reason_code"] = prior["reason_code"]
    else:
        event["reason_code"] = _validate_reason_code(event["reason_code"], event["state"])
    event["last_known_state"] = event["state"]
    event["last_heartbeat"] = event["event_time"]
    upsert(event, "line_status")

    last_event_time[line_id] = event["event_time"]
    last_seen[line_id] = event["event_time"]

    prior = previous_state.get(line_id)
    changed = prior and (prior["state"] != event["state"] or
                         prior["start_time"] != event["state_start_time"])
    if changed:
        write_interval(line_id, prior, event["state_start_time"], closed=True)
    if prior is None or changed:
        prior = {"state": event["state"], "start_time": event["state_start_time"],
                 "reason_code": _validate_reason_code(event["reason_code"], event["state"]),
                 "confirmed_until": event["state_start_time"]}
        previous_state[line_id] = prior
    write_interval(line_id, prior, event["event_time"], closed=False)


def write_interval(line_id, state, end_time, closed):
    start = parse_ts(state["start_time"])
    end = parse_ts(end_time)
    confirmed = parse_ts(state["confirmed_until"])
    if state["state"] == "RUN":
        _inc(total_run_minutes, line_id, max(0, (end - confirmed).total_seconds()) / 60)
    state["confirmed_until"] = end_time
    interval_id = document_id(line_id, state["start_time"], state["state"])
    duration = (end - start).total_seconds()
    interval = {"event_id": interval_id, "line_id": line_id, "state": state["state"],
                "start_time": state["start_time"], "end_time": end_time,
                "confirmed_until": end_time, "closed": closed,
                "duration_seconds": duration, "duration_minutes": duration / 60,
                "reason_code": state["reason_code"]}
    append(interval, "state_intervals")
    for segment in hourly(state["start_time"], end_time):
        segment.update(event_id=document_id(interval_id, segment["hour"]),
                       line_id=line_id, state=state["state"], reason_code=state["reason_code"])
        append(segment, "state_hours")


def check_stale(now_dt=None):
    now_dt = now_dt or datetime.now(timezone.utc)
    for line_id, timestamp in list(last_seen.items()):
        diff = (now_dt - parse_ts(timestamp)).total_seconds()
        if diff >= 30:
            state = previous_state[line_id]
            upsert({"line_id": line_id, "state": "STALE", "last_known_state": state["state"],
                    "event_time": now_dt.isoformat().replace('+00:00', 'Z'),
                    "state_start_time": state["start_time"], "last_heartbeat": timestamp,
                    "time_in_state": (parse_ts(timestamp) - parse_ts(state["start_time"])).total_seconds()},
                   "line_status")


def handle_production(event):
    event = normalize(event)
    line_id = event["line_id"]
    append(event, "production_events")
    save_unit(requests, event, produced=True)
    if event["cycles_count_inc"]:
        cycle = {"event_id": document_id(line_id, event["cycle_id"]),
                 "line_id": line_id, "event_time": event["event_time"],
                 "cycles_count_inc": event["cycles_count_inc"]}
        response = requests.put(f"http://localhost:9200/cycle_events/_doc/{cycle['event_id']}?refresh=wait_for",
                                json=cycle, timeout=10)
        response.raise_for_status()
    update_production_summary(line_id)
    response = requests.post("http://localhost:9200/cycle_events/_search", json={
        "size": 0, "query": {"term": {"line_id": line_id}},
        "aggs": {"cycles": {"sum": {"field": "cycles_count_inc"}}}}, timeout=10)
    if response.status_code == 404:
        total_cycles[line_id] = 0
    else:
        response.raise_for_status()
        total_cycles[line_id] = response.json()["aggregations"]["cycles"]["value"]
    upsert({
        "line_id": line_id,
        "total_good_units": total_good_units[line_id],
        "total_cycles": total_cycles[line_id],
        "last_updated": get_now_iso(),
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
            "last_updated": get_now_iso()
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
            "last_updated": get_now_iso(),
        }, "cycles_summary")

def update_production_summary(line_id):
    summary = counts(requests, line_id)
    total_good_units[line_id] = summary["total_good_units"]
    upsert(dict(summary, line_id=line_id, last_updated=get_now_iso()), "reject_summary")
    upsert(dict(summary, line_id=line_id, last_updated=get_now_iso()), "production_summary")


def handle_reject(event):
    event = normalize(event)
    append(event, "reject_events")
    save_unit(requests, event, produced=False)
    update_production_summary(event["line_id"])

def handle_qc(event):
    global fully_inspected_count

    unit_id = event["unit_id"]
    line_id = event["line_id"]

    # Track inspections per unit
    if unit_id not in inspected_units:
        inspected_units[unit_id] = set()

    was_complete = inspected_units[unit_id] >= {"VISION", "CHECKWEIGHER"}
    inspected_units[unit_id].add(event["inspection_type"])
    is_complete = inspected_units[unit_id] >= {"VISION", "CHECKWEIGHER"}

    # Count newly completed units
    if is_complete and not was_complete:
        fully_inspected_count += 1

    # Compute coverage
    produced = total_good_units.get(line_id, 0)
    if produced > 0:
        coverage = round(fully_inspected_count / produced * 100, 1)
        if coverage > 100:
            print(f"WARNING: QC coverage {coverage}% > 100% — possible duplicates")
        if coverage < 95:
            print(f"WARNING: QC coverage {coverage}% below 95% threshold")

        upsert({
            "line_id": line_id,
            "qc_coverage_pct": coverage,
            "fully_inspected_count": fully_inspected_count,
            "total_produced": produced,
            "last_updated": get_now_iso(),
        }, "qc_summary")

    append(event, "qc_events")

    # Memory limitation (same like seen_ids)
    if len(inspected_units) > 10000:
        inspected_units.clear()
        fully_inspected_count = 0  # reset — documented tradeoff
    
def handle_station_state(event):
    append(event, "station_events")
    
# Routing
HANDLERS = {
    "line_state": handle_line_state,
    "production": handle_production,
    "reject": handle_reject,
    "qc_inspection": handle_qc,
    "station_state": handle_station_state
}

def process_message(message):
    try:
        event = validate_event(json.loads(message.value.decode("utf-8")))
        if event["event_id"] in seen_ids:
            return
        if event["event_type"] == "line_state":
            valid, reason = validate(event)
            if not valid:
                raise ValueError(reason)
    except (ValueError, UnicodeError) as exc:
        coordinates = {"topic": message.topic, "partition": message.partition,
                       "offset": message.offset}
        doc_id = f"{message.topic}:{message.partition}:{message.offset}"
        try:
            original = json.loads(message.value.decode("utf-8"))
        except (ValueError, UnicodeError):
            original = None
        if isinstance(original, dict) and isinstance(original.get("event_id"), str) and original["event_id"]:
            doc_id = original["event_id"]
        append({"event_id": doc_id, "raw_message": message.value.decode("utf-8", errors="replace"),
                "kafka": coordinates, "rejection_reason": str(exc),
                "rejected_at": get_now_iso()}, "dead_letter_queue")
        return
    if event["event_id"] in seen_ids:
        return
    HANDLERS[event["event_type"]](event)
    seen_ids.add(event["event_id"])
    if len(seen_ids) > 10000:
        seen_ids.clear()


def install_mappings():
    properties = {key: {"type": "keyword"} for key in (
        "event_id", "event_type", "line_id", "unit_id", "batch_id", "cycle_id", "last_known_state",
        "material_lot_id", "station_id", "state", "reason_code", "reject_reason",
        "inspection_type", "result", "action", "reject_station_id", "reject_key")}
    properties.update({key: {"type": "date", "format": "strict_date_optional_time"}
                       for key in ("event_time", "state_start_time", "start_time",
                                   "end_time", "last_updated", "rejected_at", "hour", "confirmed_until", "last_heartbeat")})
    response = requests.put("http://localhost:9200/_index_template/coffee_events", json={
        "index_patterns": ["capsule_units", "line_status", "*_events", "*_summary", "state_intervals", "state_hours", "dead_letter_queue"],
        "template": {"mappings": {"properties": properties}}}, timeout=10)
    response.raise_for_status()
    response = requests.put("http://localhost:9200/capsule_units/_mapping", json={
        "properties": {name: {"type": "keyword"} for name in ("reject_station_id", "reject_key")}}, timeout=10)
    if response.status_code != 404:
        response.raise_for_status()


def main():
    install_mappings()
    consumer = KafkaConsumer("line_events", bootstrap_servers="localhost:9092",
                             auto_offset_reset="latest")
    next_check = time.monotonic()
    try:
        while True:
            for messages in consumer.poll(timeout_ms=1000, max_records=100).values():
                for message in messages:
                    process_message(message)
                    if time.monotonic() >= next_check:
                        check_stale()
                        next_check = time.monotonic() + 1
            if time.monotonic() >= next_check:
                check_stale()
                next_check = time.monotonic() + 1
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
