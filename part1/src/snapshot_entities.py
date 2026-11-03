"""Save timed snapshots of cumulative NER counts."""

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("snapshots")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def capture_snapshot(database, output, elapsed_minutes, started_at):
    # A read transaction provides a consistent view
    # while Spark continues updating SQLite.
    with sqlite3.connect(database, timeout=30) as db:
        db.execute("BEGIN")

        try:
            processed = db.execute(
                "SELECT COUNT(*) FROM processed_messages"
            ).fetchone()[0]

            entities = [
                {
                    "entity": entity,
                    "label": label,
                    "count": count,
                }
                for entity, label, count in db.execute("""
                    SELECT entity, label, count
                    FROM entity_counts
                    ORDER BY count DESC, entity, label
                """)
            ]

            db.commit()
        except Exception:
            db.rollback()
            raise

    snapshot = {
        "experiment_started_at": started_at,
        "captured_at": utc_now(),
        "elapsed_minutes": elapsed_minutes,
        "processed_messages": processed,
        "total_mentions": sum(x["count"] for x in entities),
        "top_10": entities[:10],
        "all_entities": entities,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(snapshot, file, indent=2, ensure_ascii=False)

    temporary.replace(output)

    LOG.info(
        "Saved %s | messages=%d | unique entities=%d",
        output,
        processed,
        len(entities),
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--database",
        default=os.getenv(
            "ENTITY_DATABASE", "data/entity_counts.db"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv(
            "SNAPSHOT_DIR", "outputs/snapshots"
        ),
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=float(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "15")),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("SNAPSHOT_COUNT", "4")),
    )

    args = parser.parse_args()

    if args.interval_minutes <= 0 or args.count < 1:
        parser.error("Interval and count must be positive")

    if not Path(args.database).is_file():
        parser.error(f"Database not found: {args.database}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    started_at = utc_now()
    started_monotonic = time.monotonic()

    LOG.info("Experiment clock started: %s", started_at)

    try:
        for index in range(1, args.count + 1):
            target_minutes = index * args.interval_minutes
            deadline = started_monotonic + target_minutes * 60

            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(remaining, 1))

            label = f"{target_minutes:g}"
            filename = f"entities_{label}min.json"

            capture_snapshot(
                args.database,
                Path(args.output_dir) / filename,
                target_minutes,
                started_at,
            )

    except KeyboardInterrupt:
        LOG.info("Snapshot scheduler stopped manually")
        return

    LOG.info("All %d snapshots completed", args.count)


if __name__ == "__main__":
    main()
