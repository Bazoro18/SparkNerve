from kafka import KafkaProducer
import json
import logging
logging.basicConfig(level=logging.DEBUG)
producer  = KafkaProducer(
	bootstrap_servers = "127.0.1.1:9092",
	value_serializer=lambda v:json.dumps(v).encode("utf-8"),
	retries = 5,
	request_timeout_ms = 10000,
	max_block_ms = 15000,
	acks="all"
)
try:
	future = producer.send("orders-cdc",value={"test":"hello"})
	result = future.get(timeout=15)
	print(("Sent Successfully: ", result))
except Exception as e:
	print("Error: ", e)
