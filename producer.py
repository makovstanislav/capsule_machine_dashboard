import random, time, uuid, json
from kafka import KafkaProducer

# Import configs
with open("config/reason_codes.json") as f: 
    reason_codes = json.load(f)

with open("config/line_spec.json") as f: 
    line_spec = json.load(f)
    
with open("config/stations.json") as f: 
    stations = json.load(f)

curr_state = "DOWN"

unit_counter = 0
batch_counter = 0

batch_id = "batch_001"
batch_number = 1

material_lot_id = "lot_001"
material_lot_number = 1

data = {
        "event_id": str(uuid.uuid4()),
        "event_type": "line_state",
        "line_id": line_spec["line_id"], 
        "state": curr_state,
        "event_time": time.time(), 
        "state_start_time": 1700000000.0,
        "reason_code": None
    }

def generate_production_event(new_units):
    production_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "production",
        "line_id": line_spec["line_id"],
        "station_id": random.choice(stations)["station_id"],
        "good_count_inc": new_units,
        "cycles_count_inc": 1,
        "batch_id": batch_id,
        "event_time": time.time()
    }
    producer.send('line_events', value=production_event)
    producer.flush()
    print(f"Production event sent: {production_event}")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    states = ["RUN", "IDLE", "DOWN"]
    new_state = random.choice(states)
    new_state_start_time = time.time()
    
    # Check whether the state has been changed
    if curr_state != new_state:
        
        curr_state = new_state
        
        data["event_id"] = str(uuid.uuid4())
        data["event_type"] = "line_state"
        data["state"] = new_state
        data["state_start_time"] = new_state_start_time
        data["event_time"] = time.time()
        
        if curr_state == "DOWN":
            data["reason_code"] = random.choice(reason_codes)["code"]
        else:
            data["reason_code"] = None
            
        producer.send('line_events', value=data)
        producer.flush()
        
        print(f"STATE CHANGED to {new_state}" )
    else:
        print(f"still {curr_state}" )
        data["event_time"] = time.time()
        producer.send('line_events', value=data)
        producer.flush()
    
    # Update counters
    if new_state == 'RUN':
        new_units = random.randint(3, 8)
        unit_counter += new_units
        batch_counter += new_units
        
        if batch_counter >= 50:
            batch_number += 1
            batch_id = f"batch_{batch_number:03d}"
            batch_counter = 0
            if batch_number % 2 == 1:
                material_lot_number += 1
                material_lot_id = f"lot_{material_lot_number:03d}"
        
        generate_production_event(new_units)

    time.sleep(5)