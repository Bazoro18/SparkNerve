import json
import random
import time
from datetime import datetime
from uuid import uuid4
from kafka import KafkaProducer

# Load customer master list
with open("./master_keys/customers.json") as f:
    CUSTOMERS = json.load(f)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

topic = "orders_cdc"
NUM_EVENTS = 12000
BATCH_SIZE = 100
OPS = ["I", "U"]
PAYMENT_TYPES = ["CARD", "UPI", "NETBANKING", "COD"]
STATUS_TYPES = ["PLACED", "PROCESSING", "SHIPPED", "DELIVERED"]

print("Starting CDC generator for orders...")

ORDER_IDS = []  # track for downstream use

for i in range(NUM_EVENTS):
    op = random.choice(OPS)
    order_id = str(uuid4())[:8].upper()
    ORDER_IDS.append(order_id)

    record = {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMERS)["customer_id"],
        "order_date": datetime.utcnow().date().isoformat(),
        "order_amount": round(random.uniform(100, 100000), 2),
        "currency": "INR",
        "payment_type": random.choice(PAYMENT_TYPES),
        "status": random.choice(STATUS_TYPES),
        "change_ts": datetime.utcnow().isoformat(),
        "op": op
    }

    producer.send(
        topic=topic,
        key=f"{op}_{order_id}",
        value=record,
        partition=random.randint(0, 2)
    )

    if i % BATCH_SIZE == 0 and i != 0:
        print(f"[Batch] Sent {i} order events so far...")
        time.sleep(1)

# Save generated order_ids for downstream use
with open("./master_keys/orders.json", "w") as f:
    json.dump(ORDER_IDS, f, indent=2)

print(f"✅ Finished sending {NUM_EVENTS} order CDC events.")
