import random, time, uuid, json
from kafka import KafkaProducer

# Config
with open("config/reason_codes.json") as f:
    reason_codes = json.load(f)
with open("config/line_spec.json") as f:
    line_spec = json.load(f)
with open("config/stations.json") as f:
    stations = json.load(f)
with open("config/reject_reasons.json") as f:
    reject_reasons = json.load(f)

LINE_ID = line_spec["line_id"]
STATION_IDS = [s["station_id"] for s in stations]
REASON_CODES = [r["code"] for r in reason_codes]

# State
curr_state = "DOWN"
state_start_time = time.time()

unit_counter = 0
batch_counter = 0
batch_number = 1
batch_id = "batch_001"
material_lot_number = 1
material_lot_id = "lot_001"

# Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
TOPIC = "line_events"

# Helpers
def send(event):
    # Sending event to Kafka
    producer.send(TOPIC, value=event)
    producer.flush()
    print(f"[{event['event_type']}] {event.get('state', event.get('unit_id', ''))}")

def base_event(event_type):
    # Shared fields by every event
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "line_id": LINE_ID,
        "event_time": time.time(),
    }

# Event producers
def generate_line_state_event():
    event = base_event("line_state")
    event.update({
        "state": curr_state,
        "state_start_time": state_start_time,
        "reason_code": random.choice(REASON_CODES) if curr_state == "DOWN" else None,
    })
    send(event)

def generate_production_event(new_units):
    event = base_event("production")
    event.update({
        "station_id": random.choice(STATION_IDS),
        "good_count_inc": new_units,
        "cycles_count_inc": 1,
        "batch_id": batch_id,
    })
    send(event)

def generate_reject_event():
    event = base_event("reject")
    event.update({
        "station_id": random.choice(STATION_IDS),
        "unit_id": f"unit_{unit_counter:06d}",
        "reject_reason": random.choice(reject_reasons),
        "material_lot_id": material_lot_id,
        "batch_id": batch_id,
    })
    send(event)

def generate_qc_events(new_units):
    for i in range(new_units):
        uid = f"unit_{unit_counter - new_units + i:06d}"
        for inspection_type, station in [("CHECKWEIGHER", "WEIGHER"), ("VISION", "SEALER")]:
            event = base_event("qc_inspection")
            event.update({
                "station_id": station,
                "unit_id": uid,
                "inspection_type": inspection_type,
                "result": "PASS" if random.random() < 0.95 else "FAIL",
                "batch_id": batch_id,
            })
            send(event)

def generate_station_events(new_units):
    station_order = [s["station_id"] for s in stations]
    # Track only 1 unit per cycle to avoid event explosion
    uid = f"unit_{unit_counter - new_units:06d}"
    for station in station_order:
        for action in ["ENTER", "EXIT"]:
            event = base_event("station_state")
            event.update({
                "station_id": station,
                "unit_id": uid,
                "batch_id": batch_id,
                "action": action,
            })
            send(event)
            
# Batch tracking
def update_counters(new_units):
    global unit_counter, batch_counter, batch_number, batch_id
    global material_lot_number, material_lot_id

    unit_counter += new_units
    batch_counter += new_units

    if batch_counter >= 50:
        batch_number += 1
        batch_id = f"batch_{batch_number:03d}"
        batch_counter = 0
        if batch_number % 2 == 1:
            material_lot_number += 1
            material_lot_id = f"lot_{material_lot_number:03d}"

# Main loop
while True:
    new_state = random.choice(["RUN", "IDLE", "DOWN"])

    # Update state tracker
    if new_state != curr_state:
        curr_state = new_state
        state_start_time = time.time()

    # Always send line state
    generate_line_state_event()

    # Production events only when state is RUN
    if curr_state == "RUN":
        new_units = random.randint(3, 8)
        update_counters(new_units)
        generate_production_event(new_units)
        if random.random() < 0.05:
            generate_reject_event()
        generate_qc_events(new_units)
        generate_station_events(new_units)

    time.sleep(5)