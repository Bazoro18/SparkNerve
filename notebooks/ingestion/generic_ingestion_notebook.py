# Generic CDC Ingestion Notebook for SparkNerve

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, to_date, lit, expr
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkNerve CDC Ingestion") \
    .getOrCreate()

# --------------------------
# Utility: Load Schema from JSON
# --------------------------
def load_schema_from_json(schema_path):
    with open(schema_path, 'r') as f:
        schema_json = json.load(f)
    
    type_mapping = {
        "string": StringType(),
        "double": DoubleType(),
        "integer": IntegerType()
    }

    fields = [StructField(col['name'], type_mapping[col['type']], col.get('nullable', True))
              for col in schema_json]
    return StructType(fields)

# --------------------------
# Parameters (Change here)
# --------------------------
table = "suppliers"  # <---- CHANGE ME to desired table name
topic = f"{table}_cdc"
schema_path = f"../../configs/schemas/{table}.json"
checkpoint_dir = f"/SparkNerve/checkpoints/{table}/"
output_dir = f"/SparkNerve/raw/{table}/"

schema = load_schema_from_json(schema_path)

# --------------------------
# Read from Kafka
# --------------------------
df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("kafka.group.id", f"sparknerve_{table}_cdc")
    .option("subscribe", topic) 
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

# Parse Kafka value as JSON
parsed_df = (df_raw
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json("json", schema).alias("data"))
    .select("data.*")
    .withColumn("partition_date", to_date(col("change_ts").cast("string")))
    .fillna({"partition_date": "1900-01-01"}))

# Add partition_date from change_ts (fallback if null)
df_with_partition = parsed_df

# --------------------------
# Batch Write Function
# --------------------------
def process_batch(df, epoch_id):
    try:
        count = df.count()
        print(f"\n[Batch {epoch_id}] ✅ Received {count} rows")

        if count > 0:
            fallback_count = df.filter(col("partition_date") == "1900-01-01").count()
            if fallback_count > 0:
                print(f"[Batch {epoch_id}] ⚠️ Fallback used for {fallback_count} rows (missing change_ts)")

            df.write.mode("append") \
                .partitionBy("partition_date", "op") \
                .parquet(output_dir)

            print(f"[Batch {epoch_id}] 📦 Data written to {output_dir}")
        else:
            print(f"[Batch {epoch_id}] ⚠️ No rows to write")

    except Exception as e:
        print(f"[Batch {epoch_id}] ❌ Error during batch processing: {str(e)}")

# --------------------------
# Start Query
# --------------------------
query = (df_with_partition.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_dir)
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()
