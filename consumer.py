from kafka import KafkaConsumer
import requests, json, time

consumer = KafkaConsumer(
    "line_events",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="latest"
)

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

def validate(event):
    event_start_time = event['state_start_time']
    curr_time = time.time()
    if event_start_time > curr_time:
        print(f"WARNING: event start time is {event_start_time} > {curr_time}")
        return False
    else:
        return True
        
for message in consumer:
    if validate(message.value):
        save_to_opensearch(message.value)