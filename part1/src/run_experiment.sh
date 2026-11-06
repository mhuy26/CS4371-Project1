#!/usr/bin/env bash
set -Eeuo pipefail

# CS4371 GP1: one bounded experiment with two concurrent Spark streaming queries.
# Usage: ./part1/src/run_experiment.sh RUN_ID DURATION_MINUTES [MODEL_PATH]
# Example: ./part1/src/run_experiment.sh demo05 3 models/agnews_tfidf_lr
# Never reuse an existing RUN_ID. Existing run01 data is not modified.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Load the local environment, if present. Paths are repository-relative unless
# explicitly supplied otherwise. Do not commit .env or credentials to Git.
if [[ -f .env ]]; then
    set -a
    # This is a trusted, local shell configuration file.
    source .env
    set +a
fi

RUN_ID="${1:-}"
DURATION="${2:-}"
MODEL_PATH="${3:-${GP1_MODEL_PATH:-models/agnews_tfidf_lr}}"
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-cs4371-gp1}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SPARK_TRIGGER="${SPARK_TRIGGER_SECONDS:-10}"

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9_-]+$ ]]; then
    echo 'Usage: run_experiment.sh RUN_ID DURATION_MINUTES [MODEL_PATH]' >&2
    echo 'RUN_ID must be nonempty and contain only letters, digits, _ or -.' >&2
    exit 2
fi
if [[ ! "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
    echo 'DURATION_MINUTES must be a positive integer.' >&2
    exit 2
fi
if [[ ! "$SPARK_TRIGGER" =~ ^[1-9][0-9]*$ ]]; then
    echo 'SPARK_TRIGGER_SECONDS must be a positive integer.' >&2
    exit 2
fi

# Resolve supplied model paths against the repository; no server-specific path.
if [[ "$MODEL_PATH" != /* ]]; then
    MODEL_PATH="$PROJECT_ROOT/$MODEL_PATH"
fi
[[ -d "$MODEL_PATH/metadata" && -d "$MODEL_PATH/stages" ]] || {
    echo "ERROR: Saved MLlib model missing or incomplete: $MODEL_PATH" >&2
    echo 'Train Part 2 first, or pass the model directory as argument 3.' >&2
    exit 1
}

RUN_DIR="experiments/$RUN_ID"
TOPIC_IN="topic1-$RUN_ID"
TOPIC_OUT="topic2-$RUN_ID"
PRODUCER_DB="$RUN_DIR/data/producer.db"
ENTITY_DB="$RUN_DIR/data/entity_counts.db"
JSONL="$RUN_DIR/data/messages.jsonl"
NER_CHECKPOINT="$RUN_DIR/checkpoints/entity_stream"
CLASSIFIER_CHECKPOINT="$RUN_DIR/checkpoints/agnews_classifier"
SNAPSHOTS="$RUN_DIR/snapshots"
LOGS="$RUN_DIR/logs"
MARKER="$RUN_DIR/started_at.txt"

log() { printf '[%s] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*"; }

# Project name, compose files and container command are explicit. /opt/kafka is
# an INTERNAL Kafka image path, not a local machine filesystem path.
compose() {
    docker compose -p "$COMPOSE_PROJECT" \
        -f docker-compose.yml -f docker-compose.elastic.yml "$@"
}

is_running() {
    local pid="${1:-}" state
    [[ -n "$pid" ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    state="$(ps -o stat= -p "$pid" 2>/dev/null)" || return 1
    [[ "$state" != Z* ]]
}

SPARK_PID=''
PRODUCER_PID=''
SNAPSHOT_PID=''
cleanup() {
    trap - EXIT INT TERM
    log 'Cleaning up launched processes...'
    local pid
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
    local pid="$1" timeout="${2:-180}" elapsed=0
    while is_running "$pid"; do
        if (( elapsed >= timeout )); then
            log "ERROR: Process $pid did not stop within ${timeout}s"
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

for executable in docker uv java python3; do
    command -v "$executable" >/dev/null 2>&1 || {
        echo "ERROR: Missing executable: $executable" >&2; exit 1;
    }
done
for source in streaming/stream_pipeline.py part1/src/producer.py part1/src/stream_entities.py part1/src/snapshot_entities.py part2/src/part2_common.py; do
    [[ -f "$source" ]] || { echo "ERROR: Missing $source" >&2; exit 1; }
done

# Never overwrite a previous experiment, even if no marker exists.
if [[ -e "$RUN_DIR" ]]; then
    echo "ERROR: Experiment directory already exists: $RUN_DIR. Use a NEW run ID." >&2
    exit 1
fi

# Prepare all temporary directories in the selected project clone (not /tmp on
# the potentially failing OS drive). Can be overridden in the calling shell.
mkdir -p spark-tmp
export TMPDIR="${TMPDIR:-$PROJECT_ROOT/spark-tmp}"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-$PROJECT_ROOT/spark-tmp}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$PROJECT_ROOT/.uv-cache}"
export PYTHONUNBUFFERED=1
if [[ -z "${JAVA_HOME:-}" ]]; then
    JAVA_BIN="$(readlink -f "$(command -v java)")"
    export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
fi

# 'uv run --no-sync' preserves the already installed spaCy NER language model.
# Dependencies should first be installed using 'uv sync' on a fresh clone.
mkdir -p "$RUN_DIR/data" "$RUN_DIR/checkpoints" "$SNAPSHOTS" "$LOGS"
log "Experiment $RUN_ID | ${DURATION} min | model=$MODEL_PATH"
log "Kafka: $TOPIC_IN -> NER -> $TOPIC_OUT | classifier reads $TOPIC_IN"

log '[1/6] Preparing NEW Kafka topics'
for topic in "$TOPIC_IN" "$TOPIC_OUT"; do
    compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic "$topic" --partitions 1 --replication-factor 1
    ready=0
    for attempt in $(seq 1 30); do
        description="$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 --describe --topic "$topic" 2>/dev/null)" || description=''
        if grep -Eq 'Leader: [0-9]+' <<<"$description" && grep -Eq 'Isr: [0-9]+' <<<"$description"; then
            ready=1; break
        fi
        sleep 2
    done
    (( ready == 1 )) || { log "ERROR: Kafka topic not ready: $topic"; exit 1; }
    offsets="$(compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic "$topic")"
    if awk -F: 'NF >= 3 && $3 != 0 { found=1 } END { exit !found }' <<<"$offsets"; then
        log "ERROR: Existing data in topic $topic. Choose another run ID."
        exit 1
    fi
done

log '[2/6] Starting both Spark queries in one application'
uv run --no-sync python streaming/stream_pipeline.py \
    --broker "$KAFKA_BOOTSTRAP" \
    --input-topic "$TOPIC_IN" --output-topic "$TOPIC_OUT" \
    --model "$MODEL_PATH" --database "$ENTITY_DB" \
    --ner-checkpoint "$NER_CHECKPOINT" \
    --classifier-checkpoint "$CLASSIFIER_CHECKPOINT" \
    --trigger-seconds "$SPARK_TRIGGER" --from-earliest \
    >"$LOGS/spark.log" 2>&1 &
SPARK_PID=$!
SPARK_READY=0
for attempt in $(seq 1 90); do
    if ! is_running "$SPARK_PID"; then
        log 'ERROR: Spark exited during startup.'
        tail -n 50 "$LOGS/spark.log"; exit 1
    fi
    if grep -q 'Both queries running' "$LOGS/spark.log"; then
        SPARK_READY=1; break
    fi
    sleep 2
done
(( SPARK_READY == 1 )) || { log 'ERROR: Combined Spark startup timed out'; tail -n 40 "$LOGS/spark.log"; exit 1; }
log 'NER and classification queries ready.'

log '[3/6] Starting snapshot scheduler'
INTERVAL="$(python3 -c 'import sys; print(f"{int(sys.argv[1]) / 4:g}")' "$DURATION")"
STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf '%s\n' "$STARTED_AT" > "$MARKER"
uv run --no-sync python part1/src/snapshot_entities.py \
    --database "$ENTITY_DB" --output-dir "$SNAPSHOTS" \
    --interval-minutes "$INTERVAL" --count 3 \
    >"$LOGS/snapshots.log" 2>&1 &
SNAPSHOT_PID=$!

log '[4/6] Starting bounded Hacker News producer'
uv run --no-sync python part1/src/producer.py \
    --topic "$TOPIC_IN" --broker "$KAFKA_BOOTSTRAP" \
    --database "$PRODUCER_DB" --output "$JSONL" --duration "$DURATION" \
    >"$LOGS/producer.log" 2>&1 &
PRODUCER_PID=$!
while is_running "$PRODUCER_PID"; do
    if ! is_running "$SPARK_PID"; then
        log 'ERROR: Combined Spark exited during collection.'
        tail -n 50 "$LOGS/spark.log"; exit 1
    fi
    sleep 5
done
if wait "$PRODUCER_PID"; then
    log 'Producer finished successfully.'
else
    log 'ERROR: Producer failed.'; tail -n 50 "$LOGS/producer.log"; exit 1
fi
PRODUCER_PID=''

if wait "$SNAPSHOT_PID"; then
    log 'Three intermediate snapshots captured.'
else
    log 'ERROR: Snapshot scheduler failed.'; tail -n 50 "$LOGS/snapshots.log"; exit 1
fi
SNAPSHOT_PID=''

# Confirm intermediates BEFORE stopping Spark.
python3 - "$SNAPSHOTS" "$DURATION" <<'PY'
import sys
from pathlib import Path
folder, duration = Path(sys.argv[1]), int(sys.argv[2])
for i in range(1, 4):
    minute = f"{duration * i / 4:g}"
    path = folder / f"entities_{minute}min.json"
    if not path.is_file():
        sys.exit(f"FAIL: missing intermediate snapshot {path}")
PY

log '[5/6] Stopping live Spark and processing final backlog'
if is_running "$SPARK_PID"; then
    kill -TERM "$SPARK_PID" 2>/dev/null || true
    wait_for_exit "$SPARK_PID" 180 || {
        log 'ERROR: Spark stop timed out; no SIGKILL sent'; exit 1;
    }
fi
if wait "$SPARK_PID"; then
    log 'Live Spark stopped cleanly.'
else
    log 'ERROR: Live Spark exited unsuccessfully'; tail -n 50 "$LOGS/spark.log"; exit 1
fi
SPARK_PID=''

# Same topic, SQLite database, model and BOTH checkpoints: availableNow
# resumes from the last committed offsets and drains any producer backlog.
uv run --no-sync python streaming/stream_pipeline.py \
    --broker "$KAFKA_BOOTSTRAP" \
    --input-topic "$TOPIC_IN" --output-topic "$TOPIC_OUT" \
    --model "$MODEL_PATH" --database "$ENTITY_DB" \
    --ner-checkpoint "$NER_CHECKPOINT" \
    --classifier-checkpoint "$CLASSIFIER_CHECKPOINT" \
    --once >>"$LOGS/spark.log" 2>&1
log 'Final NER and classifier catch-up complete.'

log '[6/6] Capturing final snapshot and validating'
uv run --no-sync python - "$ENTITY_DB" "$SNAPSHOTS" "$DURATION" "$STARTED_AT" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'part1' / 'src'))
from snapshot_entities import capture_snapshot
path, folder, duration, started_at = sys.argv[1:]
capture_snapshot(path, Path(folder) / f'entities_{duration}min.json', int(duration), started_at)
PY

uv run --no-sync python - "$PRODUCER_DB" "$ENTITY_DB" "$SNAPSHOTS" "$DURATION" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path
producer_file, entity_file, snap_dir, duration = sys.argv[1:]
with sqlite3.connect(producer_file) as con:
    delivered = con.execute('SELECT COUNT(*) FROM messages WHERE delivered=1').fetchone()[0]
    pending = con.execute('SELECT COUNT(*) FROM messages WHERE delivered=0').fetchone()[0]
with sqlite3.connect(entity_file) as con:
    processed = con.execute('SELECT COUNT(*) FROM processed_messages').fetchone()[0]
folder = Path(snap_dir)
files = list(folder.glob('entities_*min.json'))
final = folder / f'entities_{duration}min.json'
print('\n========== EXPERIMENT VALIDATION ==========')
print(f'Delivered: {delivered} | NER processed: {processed} | Pending: {pending}')
print(f'Snapshot files: {len(files)} | Final snapshot: {final.exists()}')
if delivered <= 0 or pending != 0 or processed != delivered:
    sys.exit('FAIL: Delivery/processing counts do not match')
if len(files) != 4 or not final.is_file():
    sys.exit('FAIL: Expected four snapshots')
previous = -1
for path in sorted(files, key=lambda p: float(p.stem.removeprefix('entities_').removesuffix('min'))):
    count = json.loads(path.read_text(encoding='utf-8'))['processed_messages']
    print(f'{path.name}: {count} messages')
    if count < previous:
        sys.exit('FAIL: Snapshot counts decreased')
    previous = count
if json.loads(final.read_text(encoding='utf-8'))['processed_messages'] != processed:
    sys.exit('FAIL: Final snapshot count does not match NER processed total')
print('PASS: Producer, NER and snapshots verified')
PY

log "Experiment completed: $RUN_ID; results in $RUN_DIR"
trap - EXIT
