from kafka import KafkaConsumer
import requests, json, time, uuid

consumer = KafkaConsumer(
    "line_events",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="latest",
    consumer_timeout_ms=30000
)

last_event_time = {} # clear after restart
# last time received message per line
last_seen = {}
total_good_units = {}
cycles_count_inc = {}
previous_state = {}
with open("config/reason_codes.json") as f: 
    reason_codes = json.load(f)
valid_codes = {r["code"] for r in reason_codes}
total_rejects = {}

# Validates line_state event only
def validate(event):
    event_start_time = event['state_start_time']
    event_time = event['event_time']
    curr_time = time.time()
    if event_start_time > curr_time:
        print(f"WARNING: event_start_time > curr_time")
        return False, "WARNING: event_start_time > curr_time"
    elif event_time < event_start_time:
        print(f"WARNING: event_time is BEFORE event_start_time")
        return False, "WARNING: event_time is BEFORE event_start_time"
    elif event["line_id"] in last_event_time:
        if event_time < last_event_time[event["line_id"]]:
            print(f"WARNING: event_time is OLDER than last_event_time")
            return False, "WARNING: event_time is OLDER than last_event_time"
        else:
            return True, None
    else:
        return True, None

def save_to_opensearch(event, index_name):
    # Configuration
    doc_id = event["line_id"]
    url = f"http://localhost:9200/{index_name}/_update/{doc_id}"
    headers = {"Content-Type": "application/json"}
    
    # Body
    payload = {
        "doc": event,
        "doc_as_upsert": True
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    print(response.json())
    
def append_to_opensearch(event, index_name):
    # Configuration
    doc_id = event["event_id"]
    url = f"http://localhost:9200/{index_name}/_doc/{doc_id}"
    response = requests.put(url, json=event)
    print(response.json())
    
def handle_line_state(event, event_type):
    is_valid, reason = validate(event)
    if is_valid:
        
        # Save to line_status
        line_id = event["line_id"]
        
        event["time_in_state"] = event["event_time"] - event["state_start_time"]
        save_to_opensearch(event, "line_status")
        
        last_event_time[line_id] = event["event_time"]
        last_seen[line_id] = time.time()
        print(f"[{event_type}] {event}")
        
        # Save to state_intervals
        if previous_state: 
            if event["state"] != previous_state["state"]:
                duration = (event["event_time"] - previous_state["start_time"])
                interval = {
                    "event_id": str(uuid.uuid4()),
                    "line_id": line_id,
                    "state": previous_state["state"],
                    "start_time": previous_state["start_time"],
                    "end_time": event["event_time"],
                    "duration_seconds": duration,
                    "duration_minutes": duration / 60,
                    "reason_code": previous_state["reason_code"]
                }
                # Validation of reason codes vs config
                is_down = interval["state"] == "DOWN"
                invalid_code = interval["reason_code"] not in valid_codes

                if is_down and invalid_code:
                    interval["reason_code"] = "UNKNOWN_REASON"
                
                # Save to OpenSearch
                append_to_opensearch(interval, "state_intervals")
                
                # Update previous state
                previous_state["state"] = event["state"]
                previous_state["start_time"] = event["state_start_time"]
                previous_state["reason_code"] = event["reason_code"]
        else:
            # Pre-create previous state
            previous_state["line_id"] = line_id
            previous_state["state"] = event["state"]
            previous_state["start_time"] = event["event_time"]
            previous_state["reason_code"] = event["reason_code"]
            
            # Validation of reason codes vs config
            is_down = previous_state["state"] == "DOWN"
            invalid_code = previous_state["reason_code"] not in valid_codes
          
            if is_down and invalid_code:
                    previous_state["reason_code"] = "UNKNOWN_REASON"
    else:
        send_to_dlq(event, reason)

def handle_production(event, event_type):
        if event["line_id"] in total_good_units:
            total_good_units[event["line_id"]] += event["good_count_inc"]
        else:
            total_good_units[event["line_id"]] = event["good_count_inc"]
    
        if event["line_id"] in cycles_count_inc:
            cycles_count_inc[event["line_id"]] += 1
        else:
            cycles_count_inc[event["line_id"]] = 1

        print(f"{event["line_id"]}: {total_good_units[event["line_id"]]} total units")
        append_to_opensearch(event, "production_events")
        summary_doc = {
            "line_id": event["line_id"],
            "total_good_units": total_good_units[event["line_id"]],
            "total_cycles": cycles_count_inc[event["line_id"]],
            "last_updated": time.time()
        }
        save_to_opensearch(summary_doc, "production_summary")
        
def handle_reject(event, event_type):
    if event["line_id"] in total_rejects:
        total_rejects[event["line_id"]] += 1
    else:
        total_rejects[event["line_id"]] = 1
    
    reject_rate = round((total_rejects[event["line_id"]] / (total_good_units.get(event["line_id"], 0) + total_rejects[event["line_id"]]) * 100), 1)
    if total_good_units.get(event["line_id"], 0) + total_rejects[event["line_id"]] == 0:
        reject_rate = 0
    
    print(f"{total_rejects[event["line_id"]]} : reject rate {reject_rate} %")
    append_to_opensearch(event, "reject_events")

def handle_qc(event, event_type):
        append_to_opensearch(event, "qc_events")

def send_to_dlq(event, reason):
    event['rejection_reason'] = reason
    event['rejected_at'] = time.time()
    append_to_opensearch(event, "dead_letter_queue")
    
seen_ids = set()

while True:
    for message in consumer:
        event = message.value
        event_type = event.get("event_type", "unknown")
        if event["event_id"] in seen_ids: continue
        if event_type == "line_state":
            handle_line_state(event, event_type)
        elif event_type == "production":
            handle_production(event, event_type)
        elif event_type == "reject":
            handle_reject(event, event_type)
        elif event_type == "qc_inspection":
            handle_qc(event, event_type)
        else:
            print(f"Unknown event_type: {event_type}")
        seen_ids.add(event["event_id"])
        # Memory protection
        if len(seen_ids) > 1000:
            seen_ids.clear()
        
    if last_seen:
        for key in last_seen:
            if time.time() - last_seen[key] > 30:
                stale = {
                    "line_id" : key,
                    "state" : "STALE",
                    "event_time": time.time(), 
                    "state_start_time": last_seen[key],
                    "time_in_state": time.time() - last_seen[key]
                }
                save_to_opensearch(stale, "line_status")