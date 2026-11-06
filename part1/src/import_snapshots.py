"""Import Part 1 entity snapshots into Elasticsearch."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path

import requests
from dotenv import load_dotenv


# Locate the repository independently of the working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import entity snapshots into Elasticsearch"
    )

    parser.add_argument(
        "--snapshot-dir",
        default="experiments/run01/snapshots",
        help="Snapshot folder, relative to project root or absolute",
    )

    parser.add_argument(
        "--es-url",
        default=os.getenv(
            "ELASTICSEARCH_HOST", "http://localhost:9200"
        ),
        help="Elasticsearch URL",
    )

    parser.add_argument(
        "--index",
        default=None,
        help="Destination index; inferred from snapshot run if omitted",
    )

    return parser.parse_args()


def resolve_path(value):
    path = Path(value).expanduser()

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path.resolve()


def discover_snapshots(directory):
    if not directory.is_dir():
        raise FileNotFoundError(
            f"Snapshot directory not found: {directory}"
        )

    snapshots = []

    for path in directory.glob("entities_*min.json"):
        match = re.fullmatch(
            r"entities_(\d+)min\.json", path.name
        )

        if match:
            snapshots.append((int(match.group(1)), path))

    snapshots.sort(key=lambda item: item[0])

    if not snapshots:
        raise FileNotFoundError(
            f"No valid snapshot JSON files found in {directory}"
        )

    return snapshots


def infer_index(directory):
    # experiments/run01/snapshots -> run01
    run_id = directory.parent.name

    if run_id in ("", ".", "/"):
        raise ValueError(
            "Cannot infer run ID; provide --index explicitly"
        )

    # Elasticsearch index names must be lowercase.
    run_id = re.sub(
        r"[^a-z0-9_-]+", "-", run_id.lower()
    ).strip("-_")

    if not run_id:
        raise ValueError(
            "Invalid run ID; provide --index explicitly"
        )

    return f"cs4371-entities-history-{run_id}"


def create_index(session, es_url, index):
    mapping = {
        "mappings": {
            "properties": {
                "entity": {"type": "keyword"},
                "label": {"type": "keyword"},
                "count": {"type": "integer"},
                "elapsed_minutes": {"type": "integer"},
                "captured_at": {"type": "date"},
                "processed_messages": {"type": "integer"},
            }
        }
    }

    response = session.put(
        f"{es_url}/{index}",
        json=mapping,
        timeout=30,
    )

    if response.status_code == 400:
        error = response.json().get("error", {})

        if error.get("type") != "resource_already_exists_exception":
            response.raise_for_status()
    else:
        response.raise_for_status()


def import_snapshot(session, es_url, index, minute, path):
    with path.open("r", encoding="utf-8") as file:
        snapshot = json.load(file)

    lines = []

    for item in snapshot["all_entities"]:
        document = {
            "entity": item["entity"],
            "label": item["label"],
            "count": item["count"],
            "elapsed_minutes": minute,
            "captured_at": snapshot["captured_at"],
            "processed_messages": snapshot["processed_messages"],
        }

        unique_key = json.dumps(
            [minute, item["entity"], item["label"]],
            ensure_ascii=False,
        )

        document_id = hashlib.sha256(
            unique_key.encode("utf-8")
        ).hexdigest()

        lines.append(json.dumps({
            "index": {
                "_index": index,
                "_id": document_id,
            }
        }))

        lines.append(
            json.dumps(document, ensure_ascii=False)
        )

    if not lines:
        print(f"{minute} minutes: no entity records")
        return 0

    response = session.post(
        f"{es_url}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )

    response.raise_for_status()
    result = response.json()

    if result.get("errors"):
        failures = [
            item
            for item in result["items"]
            if item.get("index", {}).get("error")
        ]
        raise RuntimeError(
            f"Bulk indexing failed: {failures[:3]}"
        )

    count = len(snapshot["all_entities"])

    print(
        f"{minute} minutes: indexed {count} entities "
        f"({snapshot['processed_messages']} messages)"
    )

    return count


def main():
    args = parse_args()

    snapshot_dir = resolve_path(args.snapshot_dir)
    snapshots = discover_snapshots(snapshot_dir)

    es_url = args.es_url.rstrip("/")
    index = args.index or infer_index(snapshot_dir)

    print(f"Snapshot directory: {snapshot_dir}")
    print(f"Elasticsearch: {es_url}")
    print(f"Index: {index}")
    print(f"Snapshots found: {len(snapshots)}")

    total = 0

    with requests.Session() as session:
        session.headers.update({
            "Content-Type": "application/json"
        })

        create_index(session, es_url, index)

        for minute, path in snapshots:
            total += import_snapshot(
                session,
                es_url,
                index,
                minute,
                path,
            )

        session.post(
            f"{es_url}/{index}/_refresh",
            timeout=30,
        ).raise_for_status()

    print(f"\nImported {total} historical entity records.")
    print(f"Index: {index}")


if __name__ == "__main__":
    main()
