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
    # bug: it's inserting, not upserting: wrong method
    r = requests.put('http://localhost:9200/line_status/_doc', json=message.value)
    print(f"Status Code: {r.status_code}, Response: {r.json()}")