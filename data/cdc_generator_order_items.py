import json
import random
import time
from datetime import datetime
from uuid import uuid4
from kafka import KafkaProducer

# Load master keys
with open("./master_keys/orders.json") as f:
    ORDER_IDS = json.load(f)

with open("./master_keys/products.json") as f:
    PRODUCTS = json.load(f)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

topic = "order_items_cdc"
NUM_EVENTS = 15000
BATCH_SIZE = 100
OPS = ["I", "U"]

print("Starting CDC generator for order items...")

for i in range(NUM_EVENTS):
    op = random.choice(OPS)
    item_id = str(uuid4())[:10].upper()
    order_id = random.choice(ORDER_IDS)
    product = random.choice(PRODUCTS)

    quantity = random.randint(1, 5)
    unit_price = product["price"]

    record = {
        "item_id": item_id,
        "order_id": order_id,
        "product_id": product["product_id"],
        "quantity": quantity,
        "unit_price": unit_price,
        "line_total": round(quantity * unit_price, 2),
        "change_ts": datetime.utcnow().isoformat(),
        "op": op
    }

    producer.send(
        topic=topic,
        key=f"{op}_{item_id}",
        value=record,
        partition=random.randint(0, 2)
    )

    if i % BATCH_SIZE == 0 and i != 0:
        print(f"[Batch] Sent {i} order item events so far...")
        time.sleep(1)

print(f"✅ Finished sending {NUM_EVENTS} order item CDC events.")
