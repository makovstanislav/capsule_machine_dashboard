from kafka import KafkaConsumer
import requests, json, time

consumer = KafkaConsumer(
    "line_events",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="latest"
)

last_event_time = {} # data will be lost after restart

def validate(event):
    event_start_time = event['state_start_time']
    event_time = event['event_time']
    curr_time = time.time()
    if event_start_time > curr_time:
        print(f"WARNING: event_start_time {event_start_time} > curr_time {curr_time}")
        return False
    elif event_time < event_start_time:
        print(f"WARNING: event_time {event_time} is BEFORE event_start_time {event_start_time}")
        return False
    elif event["line_id"] in last_event_time:
        if event_time < last_event_time[event["line_id"]]:
            print(f"WARNING: event_time {event_time} is OLDER than last_event_time {last_event_time[event["line_id"]]}")
            return False
        else:
            return True
    else:
        return True

def save_to_opensearch(event):
    # Configuration
    index_name = "line_status"
    doc_id = event["line_id"]
    url = f"http://localhost:9200/{index_name}/_update/{doc_id}"
    headers = {"Content-Type": "application/json"}

    # Upsert
    payload = {
        "doc": event,
        "doc_as_upsert": True
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    print(response.json())
    
for message in consumer:
    if validate(message.value):
        save_to_opensearch(message.value)
        last_event_time[message.value["line_id"]] = message.value["event_time"]
