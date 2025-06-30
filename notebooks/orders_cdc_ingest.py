from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Create Session
spark = SparkSession.builder \
	.appName("Kafka CDC Order Stream") \
	.master("local[*]") \
	.getOrCreate()

#Set Checkpoint and Output Path
Checkpoint_dir = "/SparkNerve/checkpoints/orders_raw"
Output_dir = "/SparkNerve/raw/orders"

#Define Schema for CDC Event
order_schema = StructType([
	StructField("op", StringType(), True),
	StructField("order_id", StringType(), True),
	StructField("customer_id", StringType(), True),
	StructField("order_date", StringType(), True),
	StructField("order_status", StringType(), True),
	StructField("payment_type", StringType(), True),
	StructField("total_amount", DoubleType(), True),
	StructField("change_ts", StringType(), True)
])

#Read Kafka CDC Stream
df_raw = (
	spark.readStream
		.format("kafka")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("subscribe","orders_cdc")
		.option("startingOffsets", "earliest")
		.option("failOnDataLoss", "false")
		.load()
)

#Parse JSON Payload
df_parsed = (
	df_raw.selectExpr("CAST(value AS STRING) as json_str")
		.select(from_json("json_str", order_schema).alias("data"))
		.select("data.*")
		.withColumn("order_date", to_date("order_date", "yyyy-MM-dd"))
		.withColumn("ingestion_ts",current_timestamp())
)

#Write to Raw Zone partitioned by Date
query = df_parsed.writeStream \
	.format("parquet") \
	.option("path","/SparkNerve/raw/orders") \
	.option("checkpointLocation","/SparkNerve/checkpoints/orders_raw") \
	.partitionBy("order_date","op") \
	.outputMode("append") \
	.start()

query.awaitTermination()
