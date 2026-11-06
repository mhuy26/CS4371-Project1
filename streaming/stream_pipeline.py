import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

# This file lives at <repository>/streaming/stream_pipeline.py.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "part1" / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "part2" / "src"))

from dotenv import load_dotenv

# Load this clone's optional local settings, independent of the cwd.
load_dotenv(PROJECT_ROOT / ".env")

from kafka import KafkaProducer
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

import stream_entities as ner
from part2_common import names

LOG = logging.getLogger("gp1-combined")
STOP_REQUESTED = False


def request_shutdown(signum, _frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    LOG.warning("Signal %s received; requesting graceful shutdown", signum)


def project_path(value):
    """Resolve a user-supplied path relative to the cloned repository."""
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Part 1 NER + Part 2 AG News classification in one Spark application"
    )
    parser.add_argument("--broker", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-topic", default=os.getenv("KAFKA_INPUT_TOPIC", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "topic2"))
    parser.add_argument("--model", required=True, help="Saved MLlib PipelineModel path")
    parser.add_argument("--database", default="data/entity_counts_combined.db")
    parser.add_argument("--ner-checkpoint", default="checkpoints/combined-ner")
    parser.add_argument("--classifier-checkpoint", default="checkpoints/combined-classifier")
    parser.add_argument("--trigger-seconds", type=int, default=15)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all currently available Kafka messages with both queries, then exit",
    )
    parser.add_argument("--from-earliest", action="store_true",
                        help="Read earliest Kafka records when using NEW checkpoints")
    args = parser.parse_args()

    if args.trigger_seconds < 1:
        parser.error("--trigger-seconds must be >= 1")

    args.model = project_path(args.model)
    args.database = project_path(args.database)
    args.ner_checkpoint = project_path(args.ner_checkpoint)
    args.classifier_checkpoint = project_path(args.classifier_checkpoint)

    if args.ner_checkpoint == args.classifier_checkpoint:
        parser.error("NER and classifier checkpoints must be different directories")
    if not args.model.exists():
        parser.error(f"Saved model not found: {args.model}")
    if not (PROJECT_ROOT / "part1" / "src" / "stream_entities.py").is_file():
        parser.error("Missing part1/src/stream_entities.py")
    return args


def create_spark():
    # Local networking can be overridden for other machines/environments.
    local_ip = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_IP", local_ip)
    builder = (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .appName("CS4371-GP1-NER-and-Classification")
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", local_ip))
        .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", local_ip))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars.packages",
            os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"),
        )
    )
    return builder.getOrCreate()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    # These are runtime output paths; do not overwrite historical experiment data.
    args.database.parent.mkdir(parents=True, exist_ok=True)
    args.ner_checkpoint.mkdir(parents=True, exist_ok=True)
    args.classifier_checkpoint.mkdir(parents=True, exist_ok=True)
    ner.initialize_database(str(args.database))

    kafka_producer = None
    spark = None
    queries = []
    failed = False

    try:
        import spacy

        LOG.info("Loading spaCy NER model")
        nlp = spacy.load(
            "en_core_web_sm",
            disable=["tagger", "parser", "lemmatizer", "attribute_ruler"],
        )

        kafka_producer = KafkaProducer(
            bootstrap_servers=args.broker,
            key_serializer=lambda value: value,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=1000,
            enable_idempotence=True,
        )

        spark = create_spark()
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("Loading AG News model: %s", args.model)
        model = PipelineModel.load(str(args.model))

        schema = T.StructType([
            T.StructField("id", T.LongType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("type", T.StringType(), True),
        ])

        # Both streaming queries consume the same input Kafka topic.
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.broker)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", "earliest" if args.from_earliest else "latest")
            .option("maxOffsetsPerTrigger", 100)
            .load()
        )
        messages = (
            source.select(F.from_json(F.col("value").cast("string"), schema).alias("message"))
            .select("message.*")
            .where(
                F.col("id").isNotNull()
                & F.col("text").isNotNull()
                & (F.length(F.trim(F.col("text"))) > 0)
            )
        )

        def process_ner_batch(batch_df, batch_id):
            ner.process_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                db_path=str(args.database),
                nlp=nlp,
                producer=kafka_producer,
                output_topic=args.output_topic,
            )

        ner_writer = (
            messages.writeStream
            .foreachBatch(process_ner_batch)
            .outputMode("append")
            .option("checkpointLocation", str(args.ner_checkpoint))
            .queryName("gp1_ner")
        )
        if args.once:
            ner_writer = ner_writer.trigger(availableNow=True)
        else:
            ner_writer = ner_writer.trigger(
                processingTime=f"{args.trigger_seconds} seconds"
            )
        ner_query = ner_writer.start()
        queries.append(ner_query)
        LOG.info("NER query started: %s -> %s", args.input_topic, args.output_topic)

        classified = names(model.transform(messages.select("text")))
        counts = classified.groupBy("predicted_class").count()
        classifier_writer = (
            counts.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option("checkpointLocation", str(args.classifier_checkpoint))
            .queryName("gp1_agnews_classifier")
        )
        if args.once:
            classifier_writer = classifier_writer.trigger(availableNow=True)
        else:
            classifier_writer = classifier_writer.trigger(
                processingTime=f"{args.trigger_seconds} seconds"
            )
        classifier_query = classifier_writer.start()
        queries.append(classifier_query)
        LOG.info("Classification query started on %s", args.input_topic)
        if args.once:
            LOG.info("Both queries started in --once mode; processing available Kafka data")
        else:
            LOG.info("Both queries running. Ctrl+C stops both gracefully.")

        while not STOP_REQUESTED:
            for query in queries:
                if query.isActive:
                    continue
                error = query.exception()
                if error is not None:
                    LOG.error("Query %s failed: %s", query.name, error)
                    failed = True
                    break
                if not args.once:
                    LOG.error("Query %s stopped unexpectedly", query.name)
                    failed = True
                    break
            if failed:
                break
            if args.once and all(not query.isActive for query in queries):
                LOG.info("Both queries finished processing available Kafka data")
                break
            time.sleep(2)

    except KeyboardInterrupt:
        # PySpark can raise KeyboardInterrupt from its own signal handler,
        # even after our SIGINT handler has requested a clean stop.
        # Do not treat an intentional Ctrl+C as a streaming failure.
        LOG.info("Keyboard interrupt received; stopping both queries gracefully")

    except Exception:
        failed = True
        LOG.exception("Combined streaming application failed")

    finally:
        for query in reversed(queries):
            if query.isActive:
                try:
                    LOG.info("Stopping query: %s", query.name)
                    query.stop()
                except Exception:
                    failed = True
                    LOG.exception("Unable to stop query: %s", query.name)

        if spark is not None:
            try:
                spark.stop()
            except Exception:
                failed = True
                LOG.exception("Unable to stop Spark")

        if kafka_producer is not None:
            try:
                kafka_producer.flush(timeout=20)
                kafka_producer.close(timeout=20)
            except Exception:
                failed = True
                LOG.exception("Unable to close Kafka producer")
        LOG.info("Shutdown complete")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
