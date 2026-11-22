import random, time, uuid, json
from datetime import datetime, timezone
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

def get_now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

# State
curr_state = "DOWN"
state_start_time = get_now_iso()
down_reason = random.choice(REASON_CODES)

run_id = uuid.uuid4().hex
unit_counter = 0
batch_counter = 0
batch_number = 1
batch_id = "batch_001"
material_lot_number = 1
material_lot_id = "lot_001"

producer = None
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
        "event_time": get_now_iso(),
    }

# Event producers
def generate_line_state_event():
    event = base_event("line_state")
    event.update({
        "state": curr_state,
        "state_start_time": state_start_time,
        "reason_code": down_reason if curr_state == "DOWN" else None,
    })
    send(event)

def generate_cycle(new_units):
    cycle_id = f"{run_id}:{LINE_ID}:cycle:{unit_counter}"
    for number in range(unit_counter - new_units, unit_counter):
        unit_id = f"{run_id}:{LINE_ID}:{number}"
        reason = random.choice(reject_reasons) if random.random() < 0.05 else None
        failed_type = random.choice(["VISION", "CHECKWEIGHER"]) if reason else None
        failed_station = {"VISION": "SEALER", "CHECKWEIGHER": "WEIGHER"}.get(failed_type)
        fields = {"unit_id": unit_id, "batch_id": batch_id,
                  "material_lot_id": material_lot_id}
        event = base_event("production")
        event.update(fields, station_id="PACKER", cycle_id=cycle_id,
                     good_count_inc=0 if reason else 1,
                     cycles_count_inc=1 if number == unit_counter - new_units else 0,
                     result="FAIL" if reason else "PASS", reject_reason=reason)
        send(event)
        for inspection_type, station in [("CHECKWEIGHER", "WEIGHER"), ("VISION", "SEALER")]:
            event = base_event("qc_inspection")
            event.update(fields, station_id=station, inspection_type=inspection_type,
                         result="FAIL" if inspection_type == failed_type else "PASS")
            send(event)
        if reason:
            event = base_event("reject")
            event.update(fields, station_id=failed_station, reject_reason=reason)
            send(event)
        for station in STATION_IDS:
            for action in ["ENTER", "EXIT"]:
                event = base_event("station_state")
                event.update(fields, station_id=station, action=action)
                send(event)
            if station == failed_station:
                break

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

def main():
    global curr_state, state_start_time, producer, down_reason
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                             value_serializer=lambda value: json.dumps(value).encode("utf-8"))
    while True:
        new_state = random.choice(["RUN", "IDLE", "DOWN"])
        if new_state != curr_state:
            curr_state = new_state
            state_start_time = get_now_iso()
            down_reason = random.choice(REASON_CODES) if curr_state == "DOWN" else None
        generate_line_state_event()
        if curr_state == "RUN":
            new_units = random.randint(3, 8)
            update_counters(new_units)
            generate_cycle(new_units)
        time.sleep(5)


if __name__ == "__main__":
    main()
