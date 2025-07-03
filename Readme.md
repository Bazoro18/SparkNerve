# SparkNerve - Open Source Big Data Platform (Streaming + Batch)

SparkNerve is a fully open-source, Spark-native big data platform designed to support both streaming and batch data pipelines. It includes rich capabilities such as schema evolution, data quality enforcement, observability, and CDC handling — built entirely on open technologies like Apache Spark, Kafka, Hive, HDFS, and Airflow.

---

## 🔧 Components Involved

- **Apache Spark** (Structured Streaming)
- **Apache Kafka** (CDC ingestion)
- **Apache HDFS** (raw + curated zone storage)
- **Apache Hive** (querying curated tables)
- **Airflow** (orchestration, optional)
- **Prometheus + Grafana** (monitoring)

---

## ✅ Getting Started (Runtime Setup)

### 1. Start Hadoop HDFS
```bash
start-dfs.sh
```
Verify:
```bash
jps
# Should show: NameNode, DataNode, SecondaryNameNode
```

### 2. Start Kafka Services
```bash
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

### 3. Create Kafka Topics
```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders_cdc --partitions 3 --replication-factor 1

# Repeat for:
# customers_cdc, products_cdc, suppliers_cdc, order_items_cdc, shipments_cdc
```

### 4. Generate Master Keys (for realistic joins)
```bash
cd /SparkNerve/data
python3 generate_master_keys.py
```

### 5. Start CDC Generators
```bash
python3 cdc_generator_customers.py
python3 cdc_generator_suppliers.py
python3 cdc_generator_products.py
python3 cdc_generator_orders.py
python3 cdc_generator_order_items.py
python3 cdc_generator_shipments.py
```

These scripts push data into Kafka topics continuously (100–500 events/sec).

---

## 🔁 Ingestion Flow (Kafka → Spark → HDFS)

### 1. Define Schema
Place schemas under:
```
/SparkNerve/configs/schemas/<table>.json
```

### 2. Run Generic Spark Streaming Notebook
Notebook: `/SparkNerve/notebooks/ingestion/generic_ingestion_notebook.py`

Update table name:
```python
table = "orders"  # or "customers", "products", etc.
```

Then run:
```bash
spark-submit generic_ingestion_notebook.py
```

The notebook will:
- Read from Kafka topic (e.g. `orders_cdc`)
- Parse CDC messages via schema
- Partition by `partition_date`, `op`
- Write to HDFS:
  - `/SparkNerve/raw/orders/`
  - Checkpoint: `/SparkNerve/checkpoints/orders/`

---

## ✅ Directory Structure

```
/SparkNerve/
├── configs/
│   └── schemas/            # All table schemas in JSON format
├── data/                   # CDC generator scripts + master ID generator
├── notebooks/
│   └── ingestion/          # Generic Spark ingestion script
├── checkpoints/            # Spark streaming checkpoints
├── raw/                    # Raw zone HDFS data
├── trusted/                # (Future) Post-DQ data
├── logs/                   # Application logs
```

---

## 🚀 Next Steps (After Raw Ingestion)

- Implement transformations (joins, enrichments)
- Apply config-driven DQ rules
- Write trusted data to `/SparkNerve/trusted/`
- Setup Trino/Presto for BI querying
- Expose to Power BI, Superset
- Monitor with Prometheus + Grafana

---

## 📬 Questions?
Feel free to ask for help on:
- Adding a new ingestion source
- Handling schema evolution
- DQ engine configs
- Reprocessing by offset/timestamp

Let's build it right 🚀
