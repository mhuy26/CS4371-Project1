import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

load_dotenv()

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("CS4371-Kafka-Test")
    .config("spark.driver.memory", "1g")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

broker = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
topic = os.getenv("KAFKA_INPUT_TOPIC", "topic1")

try:
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    messages = (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(
            get_json_object(col("json"), "$.id").alias("id"),
            get_json_object(col("json"), "$.type").alias("type"),
            get_json_object(col("json"), "$.text").alias("text"),
        )
    )

    print("Total Kafka messages:", messages.count())

    messages.show(
        5,
        truncate=80
    )

finally:
    spark.stop()
