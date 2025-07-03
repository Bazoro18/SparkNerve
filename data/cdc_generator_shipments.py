import json
import random
import time
from datetime import datetime, timedelta
from uuid import uuid4
from kafka import KafkaProducer

# Load order_id list
with open("./master_keys/orders.json") as f:
    ORDER_IDS = json.load(f)

CARRIERS = ["BlueDart", "Delhivery", "FedEx", "EcomExpress"]
STATUSES = ["SHIPPED", "IN_TRANSIT", "DELIVERED"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

topic = "shipments_cdc"
NUM_EVENTS = 10000
BATCH_SIZE = 100
OPS = ["I", "U"]

print("Starting CDC generator for shipments...")

for i in range(NUM_EVENTS):
    op = random.choice(OPS)
    shipment_id = str(uuid4())[:10].upper()
    order_id = random.choice(ORDER_IDS)

    ship_days = random.randint(1, 7)
    expected_delivery = (datetime.utcnow() + timedelta(days=ship_days)).date().isoformat()

    record = {
        "shipment_id": shipment_id,
        "order_id": order_id,
        "carrier": random.choice(CARRIERS),
        "status": random.choice(STATUSES),
        "expected_delivery": expected_delivery,
        "change_ts": datetime.utcnow().isoformat(),
        "op": op
    }

    producer.send(
        topic=topic,
        key=f"{op}_{shipment_id}",
        value=record,
        partition=random.randint(0, 2)
    )

    if i % BATCH_SIZE == 0 and i != 0:
        print(f"[Batch] Sent {i} shipment events so far...")
        time.sleep(1)

print(f"✅ Finished sending {NUM_EVENTS} shipment CDC events.")
