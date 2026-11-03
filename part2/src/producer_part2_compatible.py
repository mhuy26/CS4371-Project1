#!/usr/bin/env python3
"""CS4371 GP1: resumable Hacker News -> SQLite -> (optional Kafka) -> JSONL.

Default behavior matches Part 1 (Kafka delivery, export acknowledged messages).
For Part 2, use --no-kafka --target 800 with a NEW database and output path.
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


def clean_text(value):
    if not value:
        return ""
    return " ".join(html.unescape(re.sub(r"<[^>]*>", " ", value)).split())


def fetch_json(session, api_url, endpoint):
    response = session.get(f"{api_url.rstrip('/')}/{endpoint}", timeout=15)
    response.raise_for_status()
    return response.json()


def initialize_database(db):
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
    row = db.execute("SELECT value FROM state WHERE key = 'cursor'").fetchone()
    return int(row[0]) if row else None


def save_item(db, item_id, message):
    # Commit the item and cursor together, to allow safe resume after interruption.
    with db:
        if message is not None:
            db.execute(
                "INSERT OR IGNORE INTO messages (id, payload, delivered) VALUES (?, ?, 0)",
                (item_id, json.dumps(message, ensure_ascii=False)),
            )
        db.execute("""
            INSERT INTO state (key, value) VALUES ('cursor', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (str(item_id),))


def get_statistics(db):
    total = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    delivered = db.execute(
        "SELECT COUNT(*) FROM messages WHERE delivered = 1"
    ).fetchone()[0]
    return {"stored": total, "delivered": delivered, "pending": total - delivered}


def create_kafka_producer(broker):
    return KafkaProducer(
        bootstrap_servers=broker,
        key_serializer=lambda value: value,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        max_block_ms=30000,
        enable_idempotence=True,
    )


def deliver_pending(db, producer, topic, limit=100):
    rows = db.execute("""
        SELECT id, payload FROM messages
        WHERE delivered = 0 ORDER BY id LIMIT ?
    """, (limit,)).fetchall()
    sent = 0
    for item_id, payload in rows:
        try:
            producer.send(
                topic, key=str(item_id).encode("utf-8"), value=json.loads(payload)
            ).get(timeout=40)
        except (KafkaError, TimeoutError) as error:
            LOG.warning("Kafka delivery failed for item %s: %s", item_id, error)
            break
        with db:
            db.execute("UPDATE messages SET delivered = 1 WHERE id = ?", (item_id,))
        sent += 1
    return sent


def export_jsonl(db, output_path, include_undelivered=False):
    """Atomic export; Part 1 exports Kafka-acknowledged messages only."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    where = "" if include_undelivered else "WHERE delivered = 1"
    written = 0
    with temporary.open("w", encoding="utf-8") as file:
        rows = db.execute(f"SELECT payload FROM messages {where} ORDER BY id")
        for (payload,) in rows:
            file.write(payload + "\n")
            written += 1
    temporary.replace(output)
    LOG.info("Exported %d messages to %s", written, output)


def collect_items(session, db, api_url, max_items, deadline=None, target=None):
    """Scan HN item IDs, storing stories/comments with meaningful text."""
    latest_id = fetch_json(session, api_url, "maxitem.json")
    cursor = get_cursor(db)
    if cursor is None:
        cursor = max(0, latest_id - 100)

    collected = 0
    scanned = 0
    while cursor < latest_id and scanned < max_items:
        if deadline is not None and time.monotonic() >= deadline:
            break
        if target is not None and get_statistics(db)["stored"] >= target:
            break
        item_id = cursor + 1
        try:
            item = fetch_json(session, api_url, f"item/{item_id}.json")
        except requests.RequestException as error:
            LOG.warning("Failed to fetch item %s: %s", item_id, error)
            break  # retry same ID in the next cycle

        message = None
        if item and not item.get("deleted") and not item.get("dead"):
            item_type = item.get("type")
            if item_type in ("story", "comment"):
                title = clean_text(item.get("title"))
                body = clean_text(item.get("text"))
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
        save_item(db, item_id, message)
        cursor = item_id
        scanned += 1
    return scanned, collected, max(0, latest_id - cursor)


def parse_args():
    parser = argparse.ArgumentParser(description="Resumable Hacker News producer")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_INPUT_TOPIC", "topic1"))
    parser.add_argument("--api", default=os.getenv("HN_API_URL", "https://hacker-news.firebaseio.com/v0"))
    parser.add_argument("--interval", type=int, default=int(os.getenv("HN_POLL_INTERVAL", "30")))
    parser.add_argument("--max-items", type=int, default=int(os.getenv("HN_MAX_ITEMS", "30")))
    parser.add_argument("--duration", type=float, default=float(os.getenv("HN_DURATION_MINUTES", "60")),
                        help="Maximum duration in minutes; 0 = unlimited")
    parser.add_argument("--database", default=os.getenv("HN_DATABASE", "data/producer.db"))
    parser.add_argument("--output", default=os.getenv("DATA_OUTPUT", "data/messages.jsonl"))
    parser.add_argument("--export-only", action="store_true")
    parser.add_argument("--no-kafka", action="store_true",
                        help="Collect to SQLite + JSONL only; no Kafka connections")
    parser.add_argument("--target", type=int, default=0,
                        help="Stop at N total unique stored messages (0 = no count target)")
    args = parser.parse_args()
    if args.interval < 5:
        parser.error("--interval must be >= 5")
    if args.max_items < 1:
        parser.error("--max-items must be >= 1")
    if args.duration < 0:
        parser.error("--duration must be >= 0")
    if args.target < 0:
        parser.error("--target must be >= 0")
    if args.target and not args.no_kafka:
        parser.error("--target is supported only with --no-kafka")
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    database_path = Path(args.database)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(database_path, timeout=30)
    initialize_database(db)
    if args.export_only:
        try:
            export_jsonl(db, args.output, include_undelivered=args.no_kafka)
        finally:
            db.close()
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "CS4371-AcademicProject/1.0"})
    producer = None
    started = time.monotonic()
    deadline = started + args.duration * 60 if args.duration > 0 else None
    consecutive_failures = 0
    LOG.info("Starting Hacker News collection (%s)", "file-only" if args.no_kafka else "Kafka")
    LOG.info("Duration: %s", f"{args.duration:g} min" if deadline else "unlimited")
    if args.target:
        LOG.info("Target stored messages: %d", args.target)

    try:
        if not args.no_kafka:
            producer = create_kafka_producer(args.broker)
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                LOG.info("Collection duration reached")
                break
            if args.target and get_statistics(db)["stored"] >= args.target:
                LOG.info("Target message count reached")
                break
            try:
                sent = 0
                if producer is not None:
                    sent += deliver_pending(db, producer, args.topic)
                scanned, collected, backlog = collect_items(
                    session, db, args.api, args.max_items,
                    deadline=deadline, target=args.target or None,
                )
                if producer is not None:
                    sent += deliver_pending(db, producer, args.topic)
                stats = get_statistics(db)
                LOG.info(
                    "Elapsed=%.1f min | Scanned=%d | New=%d | Sent=%d | "
                    "Stored=%d | Delivered=%d | Pending=%d | Backlog=%d",
                    (time.monotonic() - started) / 60, scanned, collected,
                    sent, stats["stored"], stats["delivered"], stats["pending"], backlog,
                )
                if producer is not None and stats["pending"] > 0:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0
            except (requests.RequestException, KafkaError, TimeoutError) as error:
                consecutive_failures += 1
                LOG.warning("Temporary collection error: %s", error)
            except Exception:
                LOG.exception("Unexpected producer error")
                raise

            if consecutive_failures:
                delay = min(120, max(args.interval, 5 * (2 ** min(consecutive_failures, 5))))
                LOG.warning("Retry delay: %.1f seconds", delay)
            else:
                delay = args.interval + random.uniform(0, 5)
            if deadline is not None:
                delay = min(delay, deadline - time.monotonic())
            if args.target and get_statistics(db)["stored"] >= args.target:
                break
            if delay > 0:
                time.sleep(delay)
    except KeyboardInterrupt:
        LOG.info("Producer stopped by user")
    finally:
        LOG.info("Closing producer")
        try:
            if producer is not None:
                producer.flush(timeout=20)
                producer.close(timeout=20)
        finally:
            session.close()
            export_jsonl(db, args.output, include_undelivered=args.no_kafka)
            stats = get_statistics(db)
            LOG.info("Final: Stored=%d | Delivered=%d | Pending=%d",
                     stats["stored"], stats["delivered"], stats["pending"])
            db.close()
            LOG.info("Producer finished")


if __name__ == "__main__":
    main()
