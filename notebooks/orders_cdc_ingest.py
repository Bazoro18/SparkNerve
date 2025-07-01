from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("Kafka CDC Order Stream") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Set Checkpoint and Output Paths
Checkpoint_dir = "/SparkNerve/checkpoints/orders_raw_v2"
Output_dir = "SparkNerve/raw/orders"

# Define Schema for CDC Events (fields nullable for partial updates)
order_schema = StructType([
    StructField("op", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),  # raw ISO-8601 string
    StructField("order_status", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("change_ts", StringType(), True)
])

# Read Kafka CDC Stream
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders_cdc_v2")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
)

# Parse JSON and extract fields
df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json("json_str", order_schema).alias("data"))
        .select("data.*")
        .withColumn("order_date_parsed", to_date("order_date"))  # Extract yyyy-MM-dd
        .withColumn("partition_date", when(col("order_date_parsed").isNotNull(), col("order_date_parsed"))
                    .otherwise(lit("1900-01-01").cast("date")))
        .withColumn("ingestion_ts", current_timestamp())
)

# Batch Write Function
def process_batch(df, epoch_id):
    print(f"[Batch {epoch_id}] Batch count: {df.count()}")
    try:
        count = df.count()
        print(f"\n[Batch {epoch_id}] ✅ Received {count} rows")

        if count > 0:
            # Log fallback usage
            fallback_count = df.filter(col("partition_date") == "1900-01-01").count()
            if fallback_count > 0:
                print(f"[Batch {epoch_id}] ⚠️ Fallback used for {fallback_count} rows (missing order_date)")
            print(f"[Batch {epoch_id}] ✅ Current Kafka Offsets Committed")
            df.write.mode("append") \
                .partitionBy("partition_date", "op") \
                .parquet(Output_dir)
			
            print(f"[Batch {epoch_id}] 📦 Data written to {Output_dir}")
        else:
            print(f"[Batch {epoch_id}] ⚠️ No rows to write")

    except Exception as e:
        print(f"[Batch {epoch_id}] ❌ Error during batch processing: {str(e)}")

# Write Stream using foreachBatch
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", Checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start()


query.awaitTermination()
