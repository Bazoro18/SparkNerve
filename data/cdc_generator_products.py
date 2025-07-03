import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Load supplier master list
with open("./master_keys/suppliers.json") as f:
    SUPPLIERS = json.load(f)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

topic = "products_cdc"
NUM_EVENTS = 8000
BATCH_SIZE = 100
OPS = ["I", "U"]

CATEGORIES = ["Electronics", "Clothing", "Home", "Grocery", "Books"]

print("Starting CDC generator for products...")

for i in range(NUM_EVENTS):
    op = random.choice(OPS)
    product_id = f"PROD{random.randint(1, 5000):05d}"

    record = {
        "product_id": product_id,
        "name": fake.word().title(),
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(10, 5000), 2),
        "supplier_id": random.choice(SUPPLIERS)["supplier_id"],
        "change_ts": datetime.utcnow().isoformat(),
        "op": op
    }

    producer.send(
        topic=topic,
        key=f"{op}_{product_id}",
        value=record,
        partition=random.randint(0, 2)
    )

    if i % BATCH_SIZE == 0 and i != 0:
        print(f"[Batch] Sent {i} product events so far...")
        time.sleep(1)

print(f"✅ Finished sending {NUM_EVENTS} product CDC events.")
