# Databricks Notebook
# Attach to a cluster with the Snowflake Spark connector installed.
# Secrets are stored in a Databricks secret scope called "fraud-detection".
# Run: Clusters → your cluster → Edit → Spark Config → set scope/key references.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

# ── config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = dbutils.secrets.get("fraud-detection", "confluent-bootstrap")
KAFKA_KEY       = dbutils.secrets.get("fraud-detection", "confluent-api-key")
KAFKA_SECRET    = dbutils.secrets.get("fraud-detection", "confluent-api-secret")
TOPIC           = "banking.transactions"
CHECKPOINT      = "dbfs:/checkpoints/fraud/kafka_to_snowflake"

SF_OPTIONS = {
    "sfURL":      dbutils.secrets.get("fraud-detection", "snowflake-url"),
    "sfUser":     dbutils.secrets.get("fraud-detection", "snowflake-user"),
    "sfPassword": dbutils.secrets.get("fraud-detection", "snowflake-password"),
    "sfDatabase": "FRAUD_DB",
    "sfSchema":   "RAW",
    "sfWarehouse":"FRAUD_WH",
}

# ── schema ────────────────────────────────────────────────────────────────────
# mirrors exactly what the Kafka producer sends

TXN_SCHEMA = StructType([
    StructField("transaction_id",        StringType(),  False),
    StructField("customer_id",           StringType(),  False),
    StructField("amount",                DoubleType(),  False),
    StructField("currency",              StringType(),  True),
    StructField("merchant_name",         StringType(),  True),
    StructField("merchant_category",     StringType(),  True),
    StructField("transaction_timestamp", StringType(),  False),
    StructField("country",               StringType(),  True),
    StructField("city",                  StringType(),  True),
    StructField("latitude",              DoubleType(),  True),
    StructField("longitude",             DoubleType(),  True),
    StructField("device_id",             StringType(),  True),
    StructField("is_fraud",              BooleanType(), True),
    StructField("fraud_type",            StringType(),  True),
])

# ── stream ────────────────────────────────────────────────────────────────────

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username=\"{KAFKA_KEY}\" password=\"{KAFKA_SECRET}\";")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw
    .select(
        F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("d"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "d.*",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
    )
)

# ── write ─────────────────────────────────────────────────────────────────────
# pure ingestion - no transformations, no fraud logic.
# dbt handles all of that once the data is in Snowflake.

query = (
    parsed.writeStream
    .format("snowflake")
    .options(**SF_OPTIONS)
    .option("dbtable", "TRANSACTIONS")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start()
)

print(f"streaming to Snowflake RAW.TRANSACTIONS — checkpoint: {CHECKPOINT}")
query.awaitTermination()
