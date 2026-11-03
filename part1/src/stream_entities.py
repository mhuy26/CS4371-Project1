
#!/usr/bin/env python3
"""
CS4371 Group Project 1
Spark Structured Streaming + spaCy NER

Pipeline:
    Kafka topic1
        -> Spark Structured Streaming
        -> spaCy Named Entity Recognition
        -> SQLite cumulative counts
        -> Kafka topic2

Supports:
    - Continuous streaming
    - Bounded --once processing
    - SQLite deduplication
    - Checkpoint recovery
    - Local Spark driver networking
    - Graceful SIGINT / SIGTERM shutdown

Examples:
    uv run python src/stream_entities.py --once

    uv run python src/stream_entities.py

    uv run python src/stream_entities.py \
        --input-topic topic1-run01 \
        --output-topic topic2-run01 \
        --database experiments/run01/data/entity_counts.db \
        --checkpoint experiments/run01/checkpoints/entity_stream
"""

import argparse
import json
import logging
import os
import signal
import sqlite3
import threading

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import spacy
from dotenv import load_dotenv
from kafka import KafkaProducer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)


load_dotenv()

LOG = logging.getLogger("entity-stream")

STOP_REQUESTED = threading.Event()

NER_LABELS = {
    "PERSON",
    "ORG",
    "GPE",
    "LOC",
    "FAC",
    "PRODUCT",
    "EVENT",
    "WORK_OF_ART",
    "LAW",
    "NORP",
}


# ==================================================
# Shutdown handling
# ==================================================

def request_shutdown(signum, frame):
    LOG.warning(
        "Received signal %s; requesting graceful shutdown",
        signum,
    )
    STOP_REQUESTED.set()


def setup_signals():
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)


# ==================================================
# SQLite
# ==================================================

def initialize_database(db_path):
    Path(db_path).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with sqlite3.connect(db_path, timeout=30) as db:

        db.execute("""
            CREATE TABLE IF NOT EXISTS processed_messages (
                id INTEGER PRIMARY KEY
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS entity_counts (
                entity TEXT NOT NULL,
                label TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (entity, label)
            )
        """)

        db.commit()


def get_statistics(db):
    processed = db.execute(
        "SELECT COUNT(*) FROM processed_messages"
    ).fetchone()[0]

    unique_entities = db.execute(
        "SELECT COUNT(*) FROM entity_counts"
    ).fetchone()[0]

    mentions = db.execute(
        "SELECT COALESCE(SUM(count), 0) FROM entity_counts"
    ).fetchone()[0]

    return processed, unique_entities, mentions


# ==================================================
# Named Entity Recognition
# ==================================================

def extract_entities(nlp, text):
    if not text or not text.strip():
        return Counter()

    document = nlp(text[:100000])

    result = Counter()

    for ent in document.ents:
        if ent.label_ not in NER_LABELS:
            continue

        name = " ".join(ent.text.split())

        if not name:
            continue

        result[(name, ent.label_)] += 1

    return result


# ==================================================
# Kafka output
# ==================================================

def publish_entity_counts(db, producer, topic):
    updated_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows = db.execute("""
        SELECT entity, label, count
        FROM entity_counts
        ORDER BY count DESC, entity, label
    """).fetchall()

    for entity, label, count in rows:
        payload = {
            "entity": entity,
            "label": label,
            "count": count,
            "updated_at": updated_at,
        }

        kafka_key = json.dumps(
            [entity, label],
            ensure_ascii=False,
        ).encode("utf-8")

        producer.send(
            topic,
            key=kafka_key,
            value=payload,
        ).get(timeout=40)

    LOG.info(
        "Published %d cumulative entity counts to %s",
        len(rows),
        topic,
    )

    return rows


# ==================================================
# Spark micro-batch processing
# ==================================================

def process_batch_with_db(
    batch_df,
    batch_id,
    db,
    nlp,
    producer,
    output_topic,
):
    new_messages = 0
    new_mentions = 0

    for row in batch_df.toLocalIterator():
        message_id = row.id
        text = row.text

        if message_id is None or not text:
            continue

        # Ignore previously processed message IDs.
        exists = db.execute(
            """
            SELECT 1
            FROM processed_messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()

        if exists:
            continue

        entities = extract_entities(nlp, text)

        # Commit one message and its entity counts
        # in the same SQLite transaction.
        with db:
            inserted = db.execute(
                """
                INSERT OR IGNORE
                INTO processed_messages(id)
                VALUES (?)
                """,
                (message_id,),
            ).rowcount

            if inserted == 0:
                continue

            for (entity, label), count in entities.items():
                db.execute(
                    """
                    INSERT INTO entity_counts(
                        entity, label, count
                    )
                    VALUES (?, ?, ?)
                    ON CONFLICT(entity, label)
                    DO UPDATE SET
                        count = count + excluded.count
                    """,
                    (entity, label, count),
                )

        new_messages += 1
        new_mentions += sum(entities.values())

    # Publish the current cumulative counts.
    # Stable Kafka keys support downstream upserts.
    rows = publish_entity_counts(
        db,
        producer,
        output_topic,
    )

    processed, unique_entities, total_mentions = (
        get_statistics(db)
    )

    LOG.info(
        "Batch=%d | New messages=%d | "
        "New mentions=%d | "
        "Processed total=%d | "
        "Unique entities=%d | "
        "Total mentions=%d",
        batch_id,
        new_messages,
        new_mentions,
        processed,
        unique_entities,
        total_mentions,
    )

    LOG.info("Top 10 named entities:")

    for entity, label, count in rows[:10]:
        LOG.info(
            "  %-30s %-12s %d",
            entity[:30],
            label,
            count,
        )


def process_batch(
    batch_df,
    batch_id,
    db_path,
    nlp,
    producer,
    output_topic,
):
    """
    Spark invokes foreachBatch on its callback thread.

    Create SQLite connections inside that thread.
    """

    with sqlite3.connect(db_path, timeout=30) as db:
        process_batch_with_db(
            batch_df,
            batch_id,
            db,
            nlp,
            producer,
            output_topic,
        )


# ==================================================
# Configuration
# ==================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="CS4371 Spark NER streaming"
    )

    parser.add_argument(
        "--broker",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
    )

    parser.add_argument(
        "--input-topic",
        default=os.getenv(
            "KAFKA_INPUT_TOPIC",
            "topic1",
        ),
    )

    parser.add_argument(
        "--output-topic",
        default=os.getenv(
            "KAFKA_OUTPUT_TOPIC",
            "topic2",
        ),
    )

    parser.add_argument(
        "--database",
        default=os.getenv(
            "ENTITY_DATABASE",
            "data/entity_counts.db",
        ),
    )

    parser.add_argument(
        "--checkpoint",
        default=os.getenv(
            "ENTITY_CHECKPOINT_DIR",
            "checkpoints/entity_stream",
        ),
    )

    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=int(
            os.getenv("SPARK_TRIGGER_SECONDS", "30")
        ),
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Process available Kafka messages and exit",
    )

    args = parser.parse_args()

    if args.trigger_seconds < 1:
        parser.error("--trigger-seconds must be >= 1")

    return args


# ==================================================
# Spark initialization
# ==================================================

def create_spark():
    # Local mode: Spark driver and executors
    # communicate on this Ubuntu server.
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("CS4371-Entity-Streaming")

        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .config(
            "spark.driver.memory",
            "1g",
        )
        .config(
            "spark.ui.enabled",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:"
            "spark-sql-kafka-0-10_2.13:4.1.3",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    LOG.info(
        "Spark initialized: version=%s",
        spark.version,
    )

    return spark


# ==================================================
# Main
# ==================================================

def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(message)s"
        ),
    )

    setup_signals()

    initialize_database(args.database)

    Path(args.checkpoint).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    LOG.info("Loading spaCy model")

    nlp = spacy.load(
        "en_core_web_sm",
        disable=[
            "tagger",
            "parser",
            "lemmatizer",
            "attribute_ruler",
        ],
    )

    LOG.info("spaCy model loaded")

    producer = None
    spark = None
    query = None
    failed = False

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.broker,
            key_serializer=lambda value: value,
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
            ).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=1000,
            enable_idempotence=True,
        )

        spark = create_spark()

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("text", StringType(), True),
            StructField("type", StringType(), True),
        ])

        source = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                args.broker,
            )
            .option(
                "subscribe",
                args.input_topic,
            )
            .option(
                "startingOffsets",
                "earliest",
            )
            .option(
                "maxOffsetsPerTrigger",
                100,
            )
            .load()
        )

        messages = (
            source
            .select(
                from_json(
                    col("value").cast("string"),
                    schema,
                ).alias("message")
            )
            .select("message.*")
            .filter(col("id").isNotNull())
            .filter(col("text").isNotNull())
        )

        def handle_batch(batch_df, batch_id):
            process_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                db_path=args.database,
                nlp=nlp,
                producer=producer,
                output_topic=args.output_topic,
            )

        writer = (
            messages.writeStream
            .foreachBatch(handle_batch)
            .option(
                "checkpointLocation",
                args.checkpoint,
            )
            .outputMode("append")
        )

        if args.once:
            LOG.info(
                "Mode: process available Kafka data once"
            )
            writer = writer.trigger(
                availableNow=True
            )
        else:
            LOG.info(
                "Mode: continuous streaming"
            )
            writer = writer.trigger(
                processingTime=(
                    f"{args.trigger_seconds} seconds"
                )
            )

        LOG.info(
            "Input topic: %s",
            args.input_topic,
        )
        LOG.info(
            "Output topic: %s",
            args.output_topic,
        )

        query = writer.start()

        LOG.info("Spark stream started")

        # Check termination periodically instead of
        # blocking indefinitely in awaitTermination().
        # This lets the main thread handle signals.
        while not STOP_REQUESTED.is_set():
            terminated = query.awaitTermination(2)

            if terminated:
                break

        if STOP_REQUESTED.is_set():
            LOG.info("Graceful shutdown requested")

        elif query.exception() is not None:
            failed = True
            LOG.error(
                "Streaming query terminated with an error"
            )

        else:
            LOG.info("Spark stream completed")

    except KeyboardInterrupt:
        LOG.warning("Keyboard interrupt received")
        STOP_REQUESTED.set()

    except Exception:
        failed = True
        LOG.exception("Spark streaming failed")
        raise

    finally:
        LOG.info("Shutting down stream")

        try:
            if query is not None and query.isActive:
                LOG.info(
                    "Stopping active Spark query gracefully"
                )
                query.stop()

        finally:
            if spark is not None:
                spark.stop()

            if producer is not None:
                producer.flush(timeout=20)
                producer.close(timeout=20)

        LOG.info("Shutdown complete")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
