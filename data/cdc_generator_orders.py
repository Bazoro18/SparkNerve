import json
import time
import random
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

#Setup Kafka
producer = KafkaProducer(
	bootstrap_servers='127.0.1.1:9092',
	value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Simulated Order States
statuses = ["Placed","Cancelled", "Delivered"]
payment_types = ["UPI","Wallet","Credit","COD"]
customers = [f"CUST{str(i).zfill(4)}" for i in range(1,101)]

#Live Order State for Update and Delete
live_orders = {}
def generate_order_event(op=None):
	now = datetime.now(timezone.utc).isoformat()
	#Generate Data
	if op == "I" or op is None:
		order_id = str(uuid.uuid4())
		record = {
			"op":"I",
			"change_ts":now,
			"order_id":order_id,
			"customer_id":random.choice(customers),
			"order_date":now,
			"order_status":"Placed",
			"payment_type":random.choice(payment_types),
			"total_amount":round(random.uniform(100,10000),2)
			}
		live_orders[order_id] = record
		return record
	#Update Records
	elif op == "U" and live_orders:
		order_id = random.choice(list(live_orders.keys()))
		updated_status = random.choice(statuses)
		record = {
			"op":"U",
			"change_ts":now,
			"order_id":order_id,
			"order_status":updated_status,
			"total_amount":round(random.uniform(100,10000),2)
			}
		live_orders[order_id].update(record)
		return record
	#Delete Records
	elif op == "D" and live_orders:
		order_id = random.choice(list(live_orders.keys()))
		record = {
			"op":"D",
			"change_ts":now,
			"order_id":order_id
			}
		live_orders.pop(order_id, None)
		return record

	return None

#Stream Generator
def stream_events():
	while True:
		op = random.choices(["I","U","D"], weights=[0.6,0.3,0.1])[0]
		event = generate_order_event(op)
		if event:
			producer.send("orders-cdc",value=event)
			print(f">>> {event}")
		time.sleep(1)

if __name__ == "__main__":
	print("CDC Order Generator Started...")
	stream_events()
