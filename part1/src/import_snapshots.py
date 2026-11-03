
#!/usr/bin/env python3
"""Import run01 entity snapshots into Elasticsearch."""

import hashlib
import json
from pathlib import Path

import requests

ES_URL = "http://localhost:9200"
INDEX = "cs4371-entities-history-run01"
SNAPSHOT_DIR = Path("experiments/run01/snapshots")

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

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
    f"{ES_URL}/{INDEX}",
    json=mapping,
    timeout=30,
)

if response.status_code not in (200, 400):
    response.raise_for_status()

if response.status_code == 400:
    result = response.json()
    if result.get("error", {}).get("type") != "resource_already_exists_exception":
        response.raise_for_status()

total = 0

for minute in (15, 30, 45, 60):
    path = SNAPSHOT_DIR / f"entities_{minute}min.json"
    snapshot = json.loads(path.read_text(encoding="utf-8"))

    lines = []

    for item in snapshot["all_entities"]:
        doc = {
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
                "_index": INDEX,
                "_id": document_id,
            }
        }))
        lines.append(json.dumps(doc, ensure_ascii=False))

    if not lines:
        print(f"{minute} minutes: no entity records")
        continue

    response = session.post(
        f"{ES_URL}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )
    response.raise_for_status()

    result = response.json()

    if result.get("errors"):
        failures = [
            item for item in result["items"]
            if item.get("index", {}).get("error")
        ]
        raise RuntimeError(f"Bulk indexing failed: {failures[:3]}")

    count = len(snapshot["all_entities"])
    total += count

    print(
        f"{minute} minutes: indexed {count} entities "
        f"({snapshot['processed_messages']} messages)"
    )

session.post(f"{ES_URL}/{INDEX}/_refresh", timeout=30).raise_for_status()

print(f"\nImported {total} historical entity records.")
print(f"Index: {INDEX}")
session.close()
