import random, time
import json
from kafka import KafkaProducer

curr_state = "DOWN"
data = {
        "line_id": "line_1", 
        "state": curr_state, 
        "event_time": time.time(), 
        "state_start_time": 1700000000.0
    }

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    states = ["RUN", "IDLE", "DOWN"]
    new_state = random.choice(states)
    new_state_start_time = time.time()
    
    if curr_state != new_state:
        
        curr_state = new_state
        
        data["state"] = new_state
        data["state_start_time"] = new_state_start_time
        data["event_time"] = time.time()

        producer.send('line_events', value=data)
        producer.flush()
        
        print(f"STATE CHANGED to {new_state}" )
    else:
        print(f"still {curr_state}" )
        data["event_time"] = time.time()
        producer.send('line_events', value=data)
        producer.flush()
        
    time.sleep(5)