
#!/usr/bin/env bash
set -Eeuo pipefail

# CS4371 Group Project 1
# Experiment launcher
#
# Usage:
#   ./src/run_experiment.sh test04 3
#   ./src/run_experiment.sh run01 60
#
# Existing development data is preserved.
# Each experiment uses separate Kafka topics,
# SQLite databases, checkpoints and snapshots.

cd "$(dirname "$0")/.."

RUN_ID="${1:-run01}"
DURATION="${2:-60}"

if [[ ! "$RUN_ID" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    echo "ERROR: Invalid experiment ID."
    exit 1
fi

if [[ ! "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: Duration must be a positive integer."
    exit 1
fi

RUN_DIR="experiments/$RUN_ID"
TOPIC_IN="topic1-$RUN_ID"
TOPIC_OUT="topic2-$RUN_ID"

PRODUCER_DB="$RUN_DIR/data/producer.db"
ENTITY_DB="$RUN_DIR/data/entity_counts.db"
JSONL="$RUN_DIR/data/messages.jsonl"

CHECKPOINT="$RUN_DIR/checkpoints/entity_stream"
SNAPSHOTS="$RUN_DIR/snapshots"
LOGS="$RUN_DIR/logs"
MARKER="$RUN_DIR/started_at.txt"

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONUNBUFFERED=1

SPARK_PID=""
PRODUCER_PID=""
SNAPSHOT_PID=""

# ---------------------------------------------
# Utility functions
# ---------------------------------------------

log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*"
}

is_running() {
    local pid="${1:-}"

    [[ -n "$pid" ]] || return 1

    kill -0 "$pid" 2>/dev/null || return 1

    # A zombie has exited but may still be awaiting
    # collection by the parent shell.
    local state
    state=$(ps -o stat= -p "$pid" 2>/dev/null) || return 1

    [[ "$state" != Z* ]]
}

cleanup() {
    trap - EXIT INT TERM

    log "Cleaning up launcher processes..."

    for pid in "$PRODUCER_PID" "$SNAPSHOT_PID" "$SPARK_PID"; do
        if is_running "$pid"; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_exit() {
    local pid="$1"
    local timeout="${2:-120}"
    local elapsed=0

    while is_running "$pid"; do
        if (( elapsed >= timeout )); then
            log "ERROR: Process $pid did not exit within ${timeout}s."
            return 1
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    return 0
}

# ---------------------------------------------
# Preflight checks
# ---------------------------------------------

if [[ -e "$MARKER" ]]; then
    log "ERROR: Experiment $RUN_ID already started."
    log "Use a fresh experiment ID."
    exit 1
fi

for command in docker uv java; do
    if ! command -v "$command" >/dev/null 2>&1; then
        log "ERROR: Missing command: $command"
        exit 1
    fi
done

for source in \
    src/producer.py \
    src/stream_entities.py \
    src/snapshot_entities.py
do
    if [[ ! -f "$source" ]]; then
        log "ERROR: Missing $source"
        exit 1
    fi
done

mkdir -p \
    "$RUN_DIR/data" \
    "$RUN_DIR/checkpoints" \
    "$SNAPSHOTS" \
    "$LOGS"

# Don't accidentally reuse partial local data.
if [[ -e "$PRODUCER_DB" || -e "$ENTITY_DB" ]]; then
    log "ERROR: Existing experiment databases found."
    log "Use a new run ID."
    exit 1
fi

log "======================================"
log "CS4371 Experiment: $RUN_ID"
log "Duration: $DURATION minutes"
log "Input: $TOPIC_IN"
log "Output: $TOPIC_OUT"
log "Directory: $RUN_DIR"
log "======================================"

# ---------------------------------------------
# Step 1: Prepare Kafka
# ---------------------------------------------

log "[1/6] Preparing Kafka..."

for topic in "$TOPIC_IN" "$TOPIC_OUT"; do
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor 1
done

# Verify leaders are available.

for topic in "$TOPIC_IN" "$TOPIC_OUT"; do
    ready=0

    for attempt in $(seq 1 30); do
        description=$(
            docker compose exec -T kafka \
                /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server localhost:9092 \
                --describe \
                --topic "$topic" 2>/dev/null
        ) || description=""

        if echo "$description" | grep -Eq 'Leader: [0-9]+' &&
           echo "$description" | grep -Eq 'Isr: [0-9]+'; then
            ready=1
            break
        fi

        sleep 2
    done

    if [[ "$ready" -ne 1 ]]; then
        log "ERROR: Kafka topic not ready: $topic"
        exit 1
    fi

    log "Kafka topic ready: $topic"
done

# Prevent reuse of topics with previous records.

for topic in "$TOPIC_IN" "$TOPIC_OUT"; do
    offsets=$(
        docker compose exec -T kafka \
            /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server localhost:9092 \
            --topic "$topic"
    )

    if echo "$offsets" | awk -F: '
        NF >= 3 && $3 != 0 { found=1 }
        END { exit !found }
    '; then
        log "ERROR: Existing Kafka records in $topic"
        exit 1
    fi
done

# ---------------------------------------------
# Step 2: Start Spark
# ---------------------------------------------

log "[2/6] Starting Spark..."

uv run python src/stream_entities.py \
    --input-topic "$TOPIC_IN" \
    --output-topic "$TOPIC_OUT" \
    --database "$ENTITY_DB" \
    --checkpoint "$CHECKPOINT" \
    --trigger-seconds 10 \
    > "$LOGS/spark.log" 2>&1 &

SPARK_PID=$!

SPARK_READY=0

for attempt in $(seq 1 90); do
    if ! is_running "$SPARK_PID"; then
        log "ERROR: Spark exited during startup."
        tail -n 40 "$LOGS/spark.log"
        exit 1
    fi

    if grep -q "Spark stream started" "$LOGS/spark.log"; then
        SPARK_READY=1
        break
    fi

    sleep 2
done

if [[ "$SPARK_READY" -ne 1 ]]; then
    log "ERROR: Spark startup timed out."
    exit 1
fi

log "Spark stream ready."

# ---------------------------------------------
# Step 3: Start snapshot scheduler
# ---------------------------------------------

log "[3/6] Starting snapshots..."

INTERVAL=$(
    uv run python -c "print($DURATION / 4)"
)

STARTED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "$STARTED_AT" > "$MARKER"

uv run python src/snapshot_entities.py \
    --database "$ENTITY_DB" \
    --output-dir "$SNAPSHOTS" \
    --interval-minutes "$INTERVAL" \
    --count 3 \
    > "$LOGS/snapshots.log" 2>&1 &

SNAPSHOT_PID=$!

# ---------------------------------------------
# Step 4: Start bounded producer
# ---------------------------------------------

log "[4/6] Starting Hacker News producer..."

uv run python src/producer.py \
    --topic "$TOPIC_IN" \
    --database "$PRODUCER_DB" \
    --output "$JSONL" \
    --duration "$DURATION" \
    > "$LOGS/producer.log" 2>&1 &

PRODUCER_PID=$!

log "Collection started."
log "Producer PID: $PRODUCER_PID"
log "Spark PID: $SPARK_PID"

# Monitor Spark and producer.
#
# IMPORTANT:
# The snapshot scheduler is intentionally
# allowed to finish early after three snapshots.

while is_running "$PRODUCER_PID"; do
    if ! is_running "$SPARK_PID"; then
        log "ERROR: Spark exited during collection."
        tail -n 40 "$LOGS/spark.log"
        exit 1
    fi

    sleep 5
done

if wait "$PRODUCER_PID"; then
    log "Producer completed successfully."
else
    log "ERROR: Producer failed."
    tail -n 40 "$LOGS/producer.log"
    exit 1
fi

PRODUCER_PID=""

# The scheduler may already have finished.
# Validate its actual exit status now.

if wait "$SNAPSHOT_PID"; then
    log "Snapshot scheduler completed successfully."
else
    log "ERROR: Snapshot scheduler failed."
    tail -n 40 "$LOGS/snapshots.log"
    exit 1
fi

SNAPSHOT_PID=""

# Verify the intermediate snapshots exist.

EXPECTED_INTERVALS=$(
    uv run python -c "
duration = int('$DURATION')
for i in range(1, 4):
    print(f'{duration * i / 4:g}')
"
)

while read -r minutes; do
    file="$SNAPSHOTS/entities_${minutes}min.json"

    if [[ ! -f "$file" ]]; then
        log "ERROR: Missing snapshot $file"
        exit 1
    fi
done <<< "$EXPECTED_INTERVALS"

log "All three intermediate snapshots verified."

# ---------------------------------------------
# Step 5: Gracefully stop Spark
# ---------------------------------------------

log "[5/6] Stopping Spark gracefully..."

if is_running "$SPARK_PID"; then
    kill -TERM "$SPARK_PID" 2>/dev/null || true

    if ! wait_for_exit "$SPARK_PID" 120; then
        log "ERROR: Spark shutdown timed out."
        log "No forced SIGKILL was sent."
        exit 1
    fi
fi

if wait "$SPARK_PID"; then
    log "Continuous Spark exited normally."
else
    log "ERROR: Continuous Spark exited unsuccessfully."
    log "Check $LOGS/spark.log"
    exit 1
fi

SPARK_PID=""

log "Processing final Kafka backlog..."

uv run python src/stream_entities.py \
    --input-topic "$TOPIC_IN" \
    --output-topic "$TOPIC_OUT" \
    --database "$ENTITY_DB" \
    --checkpoint "$CHECKPOINT" \
    --once \
    >> "$LOGS/spark.log" 2>&1

log "Final catch-up completed."

# ---------------------------------------------
# Step 6: Capture final snapshot
# ---------------------------------------------

log "[6/6] Capturing final snapshot..."

uv run python - \
    "$ENTITY_DB" \
    "$SNAPSHOTS" \
    "$DURATION" \
    "$STARTED_AT" <<'PY'
import sys
from pathlib import Path

sys.path.insert(0, "src")
from snapshot_entities import capture_snapshot

database, directory, duration, started = sys.argv[1:]

capture_snapshot(
    database,
    Path(directory) / f"entities_{duration}min.json",
    int(duration),
    started,
)
PY

# ---------------------------------------------
# Final validation
# ---------------------------------------------

uv run python - \
    "$PRODUCER_DB" \
    "$ENTITY_DB" \
    "$SNAPSHOTS" \
    "$DURATION" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

producer_path, entity_path, snapshot_dir, duration = sys.argv[1:]

with sqlite3.connect(producer_path) as db:
    delivered = db.execute(
        "SELECT COUNT(*) FROM messages WHERE delivered=1"
    ).fetchone()[0]

    pending = db.execute(
        "SELECT COUNT(*) FROM messages WHERE delivered=0"
    ).fetchone()[0]

with sqlite3.connect(entity_path) as db:
    processed = db.execute(
        "SELECT COUNT(*) FROM processed_messages"
    ).fetchone()[0]

folder = Path(snapshot_dir)
files = list(folder.glob("entities_*.json"))
final_path = folder / f"entities_{duration}min.json"

print()
print("========== EXPERIMENT VALIDATION ==========")
print("Delivered messages:", delivered)
print("Processed messages:", processed)
print("Pending messages:", pending)
print("Snapshot files:", len(files))
print("Final snapshot exists:", final_path.exists())

if delivered == 0:
    sys.exit("FAIL: No messages collected")

if pending != 0:
    sys.exit("FAIL: Undelivered messages remain")

if processed != delivered:
    sys.exit("FAIL: Spark processing count mismatch")

if len(files) != 4 or not final_path.exists():
    sys.exit("FAIL: Four snapshots required")

previous = -1

for path in sorted(
    files,
    key=lambda p: float(
        p.stem.removeprefix("entities_").removesuffix("min")
    ),
):
    data = json.loads(path.read_text())
    current = data["processed_messages"]

    print(
        f"{path.name}: {current} processed messages"
    )

    if current < previous:
        sys.exit("FAIL: Snapshot counts decreased")

    previous = current

with final_path.open() as file:
    final_data = json.load(file)

if final_data["processed_messages"] != processed:
    sys.exit("FAIL: Final snapshot count mismatch")

print("PASS: Final counts and snapshots verified")
PY

log "======================================"
log "Experiment completed: $RUN_ID"
log "Results: $RUN_DIR"
log "======================================"

trap - EXIT
