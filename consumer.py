from kafka import KafkaConsumer
import requests

import json

consumer = KafkaConsumer(
    "line_events",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="latest"
)

for message in consumer:
    # Configuration
    index_name = "line_status"
    doc_id = message.value["line_id"]
    url = f"http://localhost:9200/{index_name}/_update/{doc_id}"
    headers = {"Content-Type": "application/json"}

    # Upsert
    payload = {
        "doc": message.value,
        "doc_as_upsert": True
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    print(response.json())