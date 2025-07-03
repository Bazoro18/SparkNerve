import json
import random
import time
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime

fake = Faker()

topic = "customers_cdc"
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

CUSTOMER_OPS = ["I", "U"]  # insert, update

NUM_EVENTS = 10000
BATCH_SIZE = 100

print("Starting CDC generator for customers...")

for i in range(NUM_EVENTS):
    op = random.choice(CUSTOMER_OPS)
    customer_id = f"CUST{random.randint(1, 10000):05d}"

    record = {
        "customer_id": customer_id,
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "country": fake.country_code(),
        "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
        "change_ts": datetime.utcnow().isoformat(),
        "op": op
    }

    producer.send(
        topic=topic,
        key=f"{op}_{customer_id}",
        value=record,
        partition=random.randint(0, 2)
    )

    if i % BATCH_SIZE == 0 and i != 0:
        print(f"[Batch] Sent {i} customer events so far...")
        time.sleep(1)

print(f"✅ Finished sending {NUM_EVENTS} customer CDC events.")
