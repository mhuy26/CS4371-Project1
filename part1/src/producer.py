"""
CS4371 Group Project 1
Hacker News -> SQLite -> Kafka

Features:
- Collect Hacker News stories and comments.
- Clean HTML from collected text.
- Store unique messages in SQLite.
- Publish messages to Kafka.
- Retry temporary API and Kafka failures.
- Resume collecting after interruption.
- Stop automatically after a configured duration.
- Export delivered messages to JSONL.

Examples:
    uv run python src/producer.py
    uv run python src/producer.py --duration 2
    uv run python src/producer.py --export-only
"""

import argparse
import html
import json
import logging
import os
import random
import re
import sqlite3
import time

from pathlib import Path

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

LOG = logging.getLogger("hn-producer")


# ==================================================
# Text processing
# ==================================================

def clean_text(value):
    """Remove HTML tags, decode entities, normalize spaces."""
    if not value:
        return ""

    value = re.sub(r"<[^>]*>", " ", value)
    value = html.unescape(value)

    return " ".join(value.split())


# ==================================================
# Hacker News API
# ==================================================

def fetch_json(session, api_url, endpoint):
    """Fetch JSON from Hacker News with a timeout."""

    response = session.get(
        f"{api_url.rstrip('/')}/{endpoint}",
        timeout=15,
    )

    response.raise_for_status()

    return response.json()


# ==================================================
# SQLite
# ==================================================

def initialize_database(db):
    """Create persistent storage tables."""

    db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            delivered INTEGER NOT NULL DEFAULT 0
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    db.commit()


def get_cursor(db):
    """Return the last successfully examined HN item ID."""

    row = db.execute(
        "SELECT value FROM state WHERE key = 'cursor'"
    ).fetchone()

    return int(row[0]) if row else None


def save_item(db, item_id, message):
    """
    Atomically save the message and collection cursor.

    The message ID acts as the SQLite primary key.
    """

    with db:

        if message is not None:
            db.execute(
                """
                INSERT OR IGNORE INTO messages
                    (id, payload, delivered)
                VALUES (?, ?, 0)
                """,
                (
                    item_id,
                    json.dumps(
                        message,
                        ensure_ascii=False,
                    ),
                ),
            )

        db.execute(
            """
            INSERT INTO state (key, value)
            VALUES ('cursor', ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value
            """,
            (str(item_id),),
        )


def get_statistics(db):
    """Get stored, delivered and pending record counts."""

    total = db.execute(
        "SELECT COUNT(*) FROM messages"
    ).fetchone()[0]

    delivered = db.execute(
        """
        SELECT COUNT(*) FROM messages
        WHERE delivered = 1
        """
    ).fetchone()[0]

    return {
        "stored": total,
        "delivered": delivered,
        "pending": total - delivered,
    }


# ==================================================
# Kafka producer
# ==================================================

def create_kafka_producer(broker):
    """Create a producer with conservative retry settings."""

    return KafkaProducer(
        bootstrap_servers=broker,

        key_serializer=lambda value: value,

        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),

        acks="all",
        retries=5,

        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        max_block_ms=30000,

        # Safe retry behavior for a single producer session.
        enable_idempotence=True,
    )


def deliver_pending(db, producer, topic, limit=100):
    """
    Publish locally saved messages to Kafka.

    Messages are marked delivered only after Kafka
    acknowledges the send.

    If Kafka is temporarily unavailable, SQLite
    retains the undelivered messages for retry.

    Delivery across Kafka and SQLite is at-least-once:
    a crash after Kafka acknowledgment but before the
    SQLite update may cause a replay.
    """

    rows = db.execute(
        """
        SELECT id, payload
        FROM messages
        WHERE delivered = 0
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    sent = 0

    for item_id, payload in rows:

        try:
            future = producer.send(
                topic,
                key=str(item_id).encode("utf-8"),
                value=json.loads(payload),
            )

            # Wait for broker acknowledgment.
            future.get(timeout=40)

        except KafkaError as error:
            LOG.warning(
                "Kafka delivery failed for item %s: %s",
                item_id,
                error,
            )

            # Leave the failed and remaining records
            # pending for the next collection cycle.
            break

        except TimeoutError as error:
            LOG.warning(
                "Kafka timeout for item %s: %s",
                item_id,
                error,
            )
            break

        # Update delivery state only after acknowledgment.
        with db:
            db.execute(
                """
                UPDATE messages
                SET delivered = 1
                WHERE id = ?
                """,
                (item_id,),
            )

        sent += 1

    return sent


# ==================================================
# JSONL export
# ==================================================

def export_jsonl(db, output_path):
    """
    Export Kafka-acknowledged messages.

    Uses atomic replacement to avoid partially
    overwriting an existing export.
    """

    output = Path(output_path)

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = output.with_name(
        output.name + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
    ) as file:

        rows = db.execute(
            """
            SELECT payload
            FROM messages
            WHERE delivered = 1
            ORDER BY id
            """
        )

        for (payload,) in rows:
            file.write(payload + "\n")

    temporary.replace(output)

    LOG.info(
        "Exported messages to %s",
        output,
    )


# ==================================================
# Hacker News collection
# ==================================================

def collect_items(
    session,
    db,
    api_url,
    max_items,
    deadline=None,
):
    """
    Scan Hacker News item IDs sequentially.

    Collect stories and comments.
    Skip deleted, dead and empty items.

    Failed API requests do not advance the cursor.
    """

    latest_id = fetch_json(
        session,
        api_url,
        "maxitem.json",
    )

    cursor = get_cursor(db)

    if cursor is None:
        # Initially examine the 100 newest item IDs.
        cursor = max(0, latest_id - 100)

    collected = 0
    scanned = 0

    while cursor < latest_id and scanned < max_items:

        if (
            deadline is not None
            and time.monotonic() >= deadline
        ):
            break

        item_id = cursor + 1

        try:
            item = fetch_json(
                session,
                api_url,
                f"item/{item_id}.json",
            )

        except requests.RequestException as error:
            LOG.warning(
                "Failed to fetch item %s: %s",
                item_id,
                error,
            )

            # Retry this item in the next cycle.
            break

        message = None

        if (
            item
            and not item.get("deleted")
            and not item.get("dead")
        ):
            item_type = item.get("type")

            if item_type in ("story", "comment"):

                title = clean_text(
                    item.get("title")
                )

                body = clean_text(
                    item.get("text")
                )

                text = f"{title} {body}".strip()

                if text:

                    message = {
                        "id": item_id,
                        "source": "hackernews",
                        "type": item_type,
                        "text": text,
                        "timestamp": item.get("time"),
                    }

                    collected += 1

        save_item(
            db,
            item_id,
            message,
        )

        cursor = item_id
        scanned += 1

    backlog = max(
        0,
        latest_id - cursor,
    )

    return scanned, collected, backlog


# ==================================================
# Command-line arguments
# ==================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description="Hacker News to Kafka producer"
    )

    parser.add_argument(
        "--broker",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv(
            "KAFKA_INPUT_TOPIC",
            "topic1",
        ),
    )

    parser.add_argument(
        "--api",
        default=os.getenv(
            "HN_API_URL",
            "https://hacker-news.firebaseio.com/v0",
        ),
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=int(
            os.getenv("HN_POLL_INTERVAL", "30")
        ),
    )

    parser.add_argument(
        "--max-items",
        type=int,
        default=int(
            os.getenv("HN_MAX_ITEMS", "30")
        ),
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=float(
            os.getenv("HN_DURATION_MINUTES", "60")
        ),
        help="Duration in minutes; 0 means unlimited",
    )

    parser.add_argument(
        "--database",
        default=os.getenv(
            "HN_DATABASE",
            "data/producer.db",
        ),
    )

    parser.add_argument(
        "--output",
        default=os.getenv(
            "DATA_OUTPUT",
            "data/messages.jsonl",
        ),
    )

    parser.add_argument(
        "--export-only",
        action="store_true",
    )

    args = parser.parse_args()

    if args.interval < 5:
        parser.error("--interval must be >= 5")

    if args.max_items < 1:
        parser.error("--max-items must be >= 1")

    if args.duration < 0:
        parser.error("--duration must be >= 0")

    return args


# ==================================================
# Main application
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

    database_path = Path(
        args.database
    )

    database_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    db = sqlite3.connect(
        database_path,
        timeout=30,
    )

    initialize_database(db)

    # Export-only mode does not connect to Kafka
    # or contact Hacker News.
    if args.export_only:
        try:
            export_jsonl(
                db,
                args.output,
            )
        finally:
            db.close()

        return

    session = requests.Session()

    session.headers.update({
        "User-Agent": "CS4371-AcademicProject/1.0"
    })

    producer = None

    started = time.monotonic()

    deadline = (
        started + args.duration * 60
        if args.duration > 0
        else None
    )

    consecutive_failures = 0

    LOG.info(
        "Starting Hacker News producer"
    )

    LOG.info(
        "Kafka topic: %s",
        args.topic,
    )

    LOG.info(
        "Collection duration: %s",
        (
            f"{args.duration:g} minutes"
            if deadline is not None
            else "unlimited"
        ),
    )

    try:

        producer = create_kafka_producer(
            args.broker
        )

        while True:

            if (
                deadline is not None
                and time.monotonic() >= deadline
            ):
                LOG.info(
                    "Collection duration reached"
                )
                break

            try:

                sent = deliver_pending(
                    db,
                    producer,
                    args.topic,
                )

                scanned, collected, backlog = (
                    collect_items(
                        session=session,
                        db=db,
                        api_url=args.api,
                        max_items=args.max_items,
                        deadline=deadline,
                    )
                )

                sent += deliver_pending(
                    db,
                    producer,
                    args.topic,
                )

                stats = get_statistics(db)

                elapsed = (
                    time.monotonic() - started
                ) / 60

                LOG.info(
                    "Elapsed=%.1f min | "
                    "Scanned=%d | New=%d | "
                    "Sent=%d | Stored=%d | "
                    "Delivered=%d | Pending=%d | "
                    "Backlog=%d",
                    elapsed,
                    scanned,
                    collected,
                    sent,
                    stats["stored"],
                    stats["delivered"],
                    stats["pending"],
                    backlog,
                )

                # Unacknowledged Kafka messages should
                # remain in SQLite and be retried.
                if stats["pending"] > 0:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

            except (
                requests.RequestException,
                KafkaError,
                TimeoutError,
            ) as error:

                consecutive_failures += 1

                LOG.warning(
                    "Temporary collection error: %s",
                    error,
                )

            except Exception:
                LOG.exception(
                    "Unexpected producer error"
                )
                raise

            # Exponential backoff, capped at 120 seconds.
            if consecutive_failures > 0:

                delay = min(
                    120,
                    max(
                        args.interval,
                        5 * (2 ** min(consecutive_failures, 5))
                    ),
                )

                LOG.warning(
                    "Retry delay: %.1f seconds",
                    delay,
                )

            else:

                delay = (
                    args.interval
                    + random.uniform(0, 5)
                )

            # Do not sleep past the automatic deadline.
            if deadline is not None:

                remaining = (
                    deadline - time.monotonic()
                )

                if remaining <= 0:
                    break

                delay = min(
                    delay,
                    remaining,
                )

            time.sleep(delay)

    except KeyboardInterrupt:

        LOG.info(
            "Producer stopped by user"
        )

    finally:

        LOG.info(
            "Closing Kafka producer"
        )

        try:

            if producer is not None:
                producer.flush(timeout=20)
                producer.close(timeout=20)

        finally:

            session.close()

            export_jsonl(
                db,
                args.output,
            )

            stats = get_statistics(db)

            LOG.info(
                "Final: Stored=%d | "
                "Delivered=%d | Pending=%d",
                stats["stored"],
                stats["delivered"],
                stats["pending"],
            )

            db.close()

            LOG.info(
                "Producer finished"
            )


if __name__ == "__main__":
    main()
