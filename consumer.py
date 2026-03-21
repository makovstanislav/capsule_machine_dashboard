from kafka import KafkaConsumer
import requests, json, time

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

# Validates line_state event only
def validate(event):
    event_start_time = event['state_start_time']
    event_time = event['event_time']
    curr_time = time.time()
    if event_start_time > curr_time:
        print(f"WARNING: event_start_time > curr_time")
        return False
    elif event_time < event_start_time:
        print(f"WARNING: event_time is BEFORE event_start_time")
        return False
    elif event["line_id"] in last_event_time:
        if event_time < last_event_time[event["line_id"]]:
            print(f"WARNING: event_time is OLDER than last_event_time")
            return False
        else:
            return True
    else:
        return True

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
    if validate(event):
        
        line_id = event["line_id"]
        
        event["time_in_state"] = event["event_time"] - event["state_start_time"]
        save_to_opensearch(event, "line_status")
        
        last_event_time[line_id] = event["event_time"]
        last_seen[line_id] = time.time()
        print(f"[{event_type}] {event}")

def handle_production(event, event_type):
        append_to_opensearch(event, "production_events")

def handle_reject(event, event_type):
        append_to_opensearch(event, "reject_events")
    
def handle_qc(event, event_type):
        append_to_opensearch(event, "qc_events")

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