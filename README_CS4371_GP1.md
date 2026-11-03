# CS4371 Group Project 1 — Spark Streaming with Kafka and Text Classification

**Course:** CS4371, Fall 2026  
**Project:** Group Project 1  
**Data source:** Hacker News Firebase API  
**Part 1:** Real-time named-entity recognition (NER) and Kibana visualization  
**Part 2:** AG News text classification with Spark MLlib, batch evaluation, and live classification  
**Submission deadline:** November 15, 2026, 11:59 PM (per assignment)

> **Demo environment:** Ubuntu Server 26.04, Python 3.14 / `uv`, Java 21, PySpark 4.1.3, Apache Kafka 4.1.2, Elasticsearch/Kibana/Logstash 9.5.5. The running example uses a working copy on a WD HDD because the original Ubuntu OS HDD has read errors. Do not delete the separate backup directory.
>
> **Status note:** This README documents the code and commands used or prepared for this project. The Part 1 60-minute experiment, Part 2 batch classification, 50-label evaluation, and separate Part 2 live stream have been run. A *simultaneous* NER + classifier demo and final Kibana screenshot still need to be verified/captured. Do not claim a demonstration was completed until it is actually shown.

## 0. Host, paths, and terminal conventions

Use a **Mac Terminal** for SSH, SCP and the browser tunnel. Unless explicitly marked **Mac**, all commands below run on **Ubuntu** via SSH.

```bash
# Mac: SSH into Ubuntu (replace with your reachable host/IP)
ssh user@YOUR_SERVER_IP
```

**Ubuntu working repository**:

```bash
cd /mnt/storage2/cs4371-work/GP1
pwd
findmnt -T .
```

Expected `pwd`: `/mnt/storage2/cs4371-work/GP1`; expected backing filesystem: the WD HDD mounted at `/mnt/storage2`. In commands below, `REPO` is a convenience variable:

```bash
cd /mnt/storage2/cs4371-work/GP1
export REPO="$PWD"
mkdir -p "$REPO/spark-tmp" "$REPO/outputs" "$REPO/checkpoints" "$REPO/models"
export TMPDIR="$REPO/spark-tmp"
export SPARK_LOCAL_DIRS="$REPO/spark-tmp"
export SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export UV_CACHE_DIR="$REPO/.uv-cache"
export UV_PROJECT_ENVIRONMENT="$REPO/.venv"
```

**Re-run the exports inside every newly created tmux pane/session**: tmux shells do not automatically inherit exports made later in a different shell.

> **Storage caution:** Existing Kafka and Elasticsearch *data* are bind-mounted on `/mnt/storage2/cs4371-work/docker-data/`. The Ubuntu system, Docker images and some other container files remain on the failing Toshiba HDD. Keep runs short and monitor for I/O errors. Do not use `docker compose down -v` or `docker system prune --volumes`.

### tmux quick reference (Ubuntu)

```bash
tmux new -s session-name          # Create and enter a session
# Detach: press Ctrl+B, release, then press D
tmux ls                           # List sessions
tmux attach -t session-name       # Reattach
tmux kill-session -t session-name # Only after the program has ended
```

`Ctrl+C` **inside** an attached tmux session asks the foreground program to stop; `Ctrl+B`, then `D` **detaches without stopping**. Do not confuse the two.

## 1. Project file tree — Part 1

The following is the relevant **Part 1** layout (generated folders and filenames may vary):

```text
GP1/
├── docker-compose.yml                # Kafka
├── docker-compose.elastic.yml        # Logstash, Elasticsearch, Kibana
├── config/
│   └── logstash.conf                 # Kafka topic2 -> Elasticsearch
├── src/
│   ├── producer.py                   # Hacker News -> SQLite -> Kafka + JSONL
│   ├── stream_entities.py            # Spark Kafka -> spaCy NER -> counts -> Kafka
│   ├── snapshot_entities.py          # Save entity frequency snapshots
│   ├── import_snapshots.py           # Import historical snapshots to Elasticsearch
│   └── run_experiment.sh             # Bounded Part 1 experiment orchestrator
├── experiments/
│   └── run01/
│       ├── data/
│       │   ├── producer.db
│       │   ├── entity_counts.db
│       │   └── messages.jsonl
│       ├── snapshots/
│       │   ├── entities_15min.json
│       │   ├── entities_30min.json
│       │   ├── entities_45min.json
│       │   └── entities_60min.json
│       ├── logs/
│       │   ├── producer.log
│       │   ├── spark.log
│       │   └── snapshots.log
│       └── checkpoints/
├── data/                              # Working data and SQLite files
├── checkpoints/                       # Spark checkpoints (may be large)
├── outputs/
├── pyproject.toml
└── uv.lock
```

### 1.1 Part 1 architecture

```text
Hacker News Firebase API
         |
         v
  src/producer.py
         |-------> SQLite + JSONL archive
         v
  Kafka topic1 (or topic1-run01)
         |
         v
  src/stream_entities.py (Spark + spaCy NER)
         |-------> cumulative counts in SQLite
         v
  Kafka topic2 (or topic2-run01)
         |
         v
    Logstash -> Elasticsearch -> Kibana top-10 entity plots
```

### 1.2 Start Part 1 services (Ubuntu, from repository)

```bash
cd /mnt/storage2/cs4371-work/GP1
# Project name is explicit to reuse the original cs4371-gp1 stack.
docker compose -p cs4371-gp1 \
  -f docker-compose.yml -f docker-compose.elastic.yml \
  up -d kafka elasticsearch kibana logstash

docker compose -p cs4371-gp1 \
  -f docker-compose.yml -f docker-compose.elastic.yml ps
```

Kafka is on Ubuntu `127.0.0.1:9092`, Elasticsearch on `127.0.0.1:9200`, and Kibana on `127.0.0.1:5601` in the current Compose configuration. If Kibana fails with `EIO`, **do not delete Elasticsearch data or retry destructive repairs**; check its logs and underlying disk health.

```bash
# Kafka topics
docker exec cs4371-gp1-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --list

# Elasticsearch health and indexed Part 1 data
curl -sS --max-time 10 'http://localhost:9200/_cluster/health?pretty'
curl -sS --max-time 10 'http://localhost:9200/_cat/indices/cs4371-*?v'

# Kibana readiness
curl -I --max-time 10 http://localhost:5601
```

**Prior observed data counts**: `cs4371-entities` = 295; `cs4371-entities-run01` = 272; restored `cs4371-entities-history-run01` = 845 (was previously observed as 846, so verify rather than asserting identical restoration). A **yellow** Elasticsearch cluster is expected on a single node with replicas configured; ensure `unassigned_primary_shards` is zero.

### 1.3 Show Kibana on the Mac

Run **on Mac Terminal** (leave this terminal open):

```bash
ssh -N -L 5601:127.0.0.1:5601 user@YOUR_SERVER_IP
```

Open `http://localhost:5601` in the **Mac browser**. Find the saved historical dashboard showing the top 10 entities at 15, 30, 45 and 60 minutes. Capture screenshots or record the browser as part of the demo. If the dashboard is missing, the JSON snapshots remain available and `src/import_snapshots.py` can be used to rebuild the history; inspect that script's arguments with `--help` before running. The Kibana saved dashboard itself was not guaranteed to restore until tested.

### 1.4 Show the completed 60-minute Part 1 experiment WITHOUT rerunning it

```bash
cd /mnt/storage2/cs4371-work/GP1
ls -lh experiments/run01/snapshots/
ls -lh experiments/run01/data/
tail -n 40 experiments/run01/logs/producer.log
tail -n 40 experiments/run01/logs/snapshots.log
```

Verified historical experiment summary:

| Elapsed time | Processed messages | Distinct entity-label pairs |
|---|---:|---:|
| 15 min | 176 | 149 |
| 30 min | 266 | 191 |
| 45 min | 334 | 233 |
| 60 min | 405 | 272 |

The experiment completed with **405 delivered, 405 processed, 0 pending**, and four JSON snapshots. The snapshot counts represent cumulative observations, not four independent datasets.

### 1.5 OPTIONAL: run a new bounded Part 1 experiment for demonstration

> This starts the original full pipeline and writes NEW experiment data; do not overwrite `run01`. Because Logstash/Elasticsearch/Kibana use additional resources, only do this if the running server is stable. First check the runner's usage and implementation:

```bash
cd /mnt/storage2/cs4371-work/GP1
head -n 80 src/run_experiment.sh
```

If the runner still uses the previously verified positional interface (`RUN_ID`, `DURATION_MINUTES`), a short fresh demo is:

```bash
tmux new -s gp1-part1-demo
# Inside tmux:
cd /mnt/storage2/cs4371-work/GP1
export TMPDIR="$PWD/spark-tmp" SPARK_LOCAL_DIRS="$PWD/spark-tmp" SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export UV_CACHE_DIR="$PWD/.uv-cache"
./src/run_experiment.sh demo-part1 3
# Ctrl+B, D to detach; tmux attach -t gp1-part1-demo to return
```

A 3-minute smoke test is **not** a replacement for the already completed 60-minute `run01` experiment. The orchestrator creates its own topics/checkpoints, snapshots, logs and SQLite files; inspect them under `experiments/demo-part1/` after completion.

## 2. Project file tree — Part 2

```text
GP1/
├── src/
│   ├── part2_common.py              # Spark session + JSON text + class name helpers
│   ├── part2_get_agnews.py          # Download AG News training Parquet
│   ├── part2_train.py               # MLlib pipeline training + saved model
│   ├── part2_batch.py               # Batch predict + CSV output
│   ├── part2_evaluate.py            # Evaluate 50 human labels; excludes None
│   ├── part2_stream.py              # Second Spark Structured Streaming query
│   ├── producer.py                  # Original Kafka-enabled HN producer
│   └── producer_part2_compatible.py # Optional no-Kafka collector
├── data/
│   ├── ag_news/
│   │   └── train_000.parquet        # 120,000 AG News training articles
│   ├── part2_extra_messages.jsonl   # 800 newly collected HN messages
│   ├── part2_producer.db
│   └── part2_combined.jsonl         # 1,165 unique nonempty messages
├── experiments/run01/data/
│   └── messages.jsonl               # Original 405 HN messages
├── models/
│   └── agnews_tfidf_lr/             # Saved fitted Spark PipelineModel
├── outputs/part2/
│   ├── all_predictions/             # Spark CSV: all 1,165 predictions
│   ├── five_per_class/              # Spark CSV: 5 examples x 4 classes
│   ├── hand_label_50/               # Spark CSV: selected 50 messages
│   ├── part2_manual_50.csv          # Human-filled manual labels
│   └── evaluation.txt               # 50-label accuracy and mismatches
├── checkpoints/
│   └── part2-classifier/            # Live Spark checkpoint
└── spark-tmp/
```

### 2.1 Part 2 architecture

```text
AG News 120,000 labeled articles (World / Sports / Business / Sci/Tech)
    -> RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF
    -> LogisticRegression -> models/agnews_tfidf_lr/

Hacker News original JSONL (405) + additional JSONL (800)
    -> 1,205 raw -> 1,165 unique nonempty -> saved model
    -> batch CSV predictions -> 50 manual labels -> accuracy

Live Hacker News producer -> Kafka topic1-part2
    -> second Spark streaming query -> same saved model
    -> running per-class counts on console
```

The model has **only four prediction classes**. `None` is allowed for *human* labels and is excluded from the eligible accuracy denominator; it is **not** a fifth trained model class.

### 2.2 Environment and AG News training demo (Ubuntu)

```bash
cd /mnt/storage2/cs4371-work/GP1
mkdir -p spark-tmp data/ag_news models outputs/part2
export TMPDIR="$PWD/spark-tmp" SPARK_LOCAL_DIRS="$PWD/spark-tmp" SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 UV_CACHE_DIR="$PWD/.uv-cache"
uv run --no-sync python -c 'import pyspark; print(pyspark.__version__)'
ls -lh data/ag_news/train_000.parquet
```

If AG News is not downloaded yet:

```bash
uv run --no-sync python src/part2_get_agnews.py --output data/ag_news
```

**Training** (only if the saved model does not already exist, or you intentionally want to retrain):

```bash
uv run --no-sync python src/part2_train.py \
  --ag-news data/ag_news \
  --model models/agnews_tfidf_lr
```

AG News contained **120,000 articles**, evenly distributed as **30,000 in each class**. Training the model is more resource-intensive than loading it; for a narrated demonstration, show the existing model and saved log/results rather than unnecessarily retraining.

### 2.3 Batch classification demo (Ubuntu)

```bash
cd /mnt/storage2/cs4371-work/GP1
wc -l experiments/run01/data/messages.jsonl data/part2_extra_messages.jsonl
wc -l data/part2_combined.jsonl
```

Observed counts: **405 + 800 = 1,205 raw messages**; after removing 40 empty/duplicate messages, **1,165 unique nonempty**.

To regenerate outputs from the **saved model** (this overwrites generated Spark output folders but does not retrain or modify the JSONL input):

```bash
export TMPDIR="$PWD/spark-tmp" SPARK_LOCAL_DIRS="$PWD/spark-tmp" SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 UV_CACHE_DIR="$PWD/.uv-cache"
uv run --no-sync python src/part2_batch.py \
  --input experiments/run01/data/messages.jsonl data/part2_extra_messages.jsonl \
  --model models/agnews_tfidf_lr \
  --output outputs/part2
```

**WARNING:** Re-running `part2_batch.py` overwrites `outputs/part2/hand_label_50/` and may change the sample order. It does **not** overwrite the separate manually completed `outputs/part2/part2_manual_50.csv`, but do not relabel or mix sample versions without checking their text matches. For a demo, simply show the saved CSV and counts when possible.

Verify the CSV export uses `.option('escape', '"')` (embedded quotes doubled) and that all prediction probabilities are in `[0, 1]`. The initial CSV export was malformed; the corrected export was validated as:

```text
all_predictions: 1165 rows, 0 malformed
five_per_class: 20 rows, 0 malformed
hand_label_50: 50 rows, 0 malformed
```

Observed batch predictions:

| Predicted class | Count |
|---|---:|
| World | 45 |
| Sports | 122 |
| Business | 193 |
| Sci/Tech | 805 |
| **Total** | **1,165** |

### 2.4 Manual 50-message evaluation demo (Ubuntu)

The human annotator assigns exactly one of `World`, `Sports`, `Business`, `Sci/Tech`, or `None`. The saved worksheet must have columns `text`, `predicted_class`, `predicted_probability`, and `manual_label`.

```bash
cd /mnt/storage2/cs4371-work/GP1
uv run --no-sync python src/part2_evaluate.py \
  --labeled-csv outputs/part2/part2_manual_50.csv \
  | tee outputs/part2/evaluation.txt
```

Observed evaluation:

```text
Total hand-labeled: 50
None of these: 6
Four-class eligible: 44
Correct within four classes: 34
Hand-check accuracy: 77.27%
```

The score is **34/44**, *not* 34/50. Examples of errors include AI discussion predicted as Sports and politics predicted as Sci/Tech. This is a domain-shift limitation: AG News training articles differ from short informal Hacker News comments. The human labels should be independently assigned, not copied from model predictions.

### 2.5 Real-time Part 2 classifier demo — tmux, Kafka and producer

**Before starting (Ubuntu):**

```bash
cd /mnt/storage2/cs4371-work/GP1
docker compose -p cs4371-gp1 \
  -f docker-compose.yml -f docker-compose.elastic.yml up -d kafka

docker exec cs4371-gp1-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic topic1-part2 \
  --partitions 1 --replication-factor 1
```

**Terminal / tmux session A — classifier**:

```bash
tmux new -s part2-stream
# INSIDE session A:
cd /mnt/storage2/cs4371-work/GP1
mkdir -p spark-tmp checkpoints/part2-classifier
export TMPDIR="$PWD/spark-tmp" SPARK_LOCAL_DIRS="$PWD/spark-tmp" SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 UV_CACHE_DIR="$PWD/.uv-cache"
uv run --no-sync python src/part2_stream.py \
  --model models/agnews_tfidf_lr \
  --topic topic1-part2 \
  --bootstrap localhost:9092 \
  --checkpoint checkpoints/part2-classifier \
  --trigger-seconds 15 \
  --from-earliest
# After it says 'Streaming classification ...': Ctrl+B, then D to detach.
```

An initially empty `Batch: 0` is normal if the new Kafka topic has no messages. `--from-earliest` is relevant for a new checkpoint; **if the checkpoint already exists, Spark resumes from its recorded offsets**, not necessarily the beginning. Do not delete the working checkpoint casually.

**Terminal / tmux session B — bounded Hacker News producer**:

```bash
tmux new -s part2-producer
# INSIDE session B:
cd /mnt/storage2/cs4371-work/GP1
export UV_CACHE_DIR="$PWD/.uv-cache"
uv run --no-sync python src/producer.py \
  --topic topic1-part2 \
  --broker localhost:9092 \
  --duration 10 \
  --interval 10 \
  --max-items 30 \
  --database data/part2_live_producer.db \
  --output data/part2_live_messages.jsonl
# Ctrl+B, then D to detach while collection continues.
```

If repeating the demo and you need *new* messages, use a distinct database and topic (e.g., `topic1-part2-demo2`) and a new checkpoint; otherwise the same SQLite cursor resumes and old topic data may be replayed based on checkpoint state. Always match the producer's topic to the classifier's topic.

**Observe sessions**:

```bash
tmux ls
tmux attach -t part2-stream
# See Batch N and running totals; Ctrl+B, D to detach.
# To inspect producer instead:
tmux attach -t part2-producer
```

The class-count table is cumulative in `complete` output mode. In the prior live demonstration, Batch 21 showed **World 7, Sci/Tech 118, Business 31, Sports 13**, total **169**. This is an observed intermediate batch, **not necessarily the final Kafka total**. Save screenshots/video of two or more changing batch tables.

When the producer prints `Producer finished`, confirm the stream has caught up, then **attach `part2-stream` and press `Ctrl+C` once**. Verify:

```bash
ps -ef | grep '[p]art2_stream.py' || true
```

Kafka end-offset inspection (per partition, not necessarily processed-message count):

```bash
docker exec cs4371-gp1-kafka-1 \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic topic1-part2 --time -1
```

### 2.6 Demonstrate Part 1 NER and Part 2 classification **simultaneously**

The assignment calls for a second Structured Streaming query **alongside** Part 1 NER. The completed live demo so far showed the Part 2 classifier separately. For a simultaneous demo, both queries must subscribe to the **same new topic**, using **different Spark checkpoints**. **This concurrency has not yet been verified** on the current server.

A cautious procedure:

1. Leave Kafka running. Use a fresh topic, for example `topic1-both-demo`, and distinct `topic2-both-demo` for NER output.
2. Create two tmux sessions: `gp1-ner` and `gp1-classifier`.
3. Start `src/stream_entities.py` in `gp1-ner` (input `topic1-both-demo`, output `topic2-both-demo`, new SQLite database and checkpoint); start `src/part2_stream.py` in `gp1-classifier` (input `topic1-both-demo`, new checkpoint).
4. Start a third tmux session `gp1-producer` to run `src/producer.py` for a short fixed duration and publish to `topic1-both-demo` with a new SQLite database.
5. Switch between tmux sessions to show NER processing and live class counts.

**NER CLI reminder:** Verify its actual options before starting:

```bash
cd /mnt/storage2/cs4371-work/GP1
uv run --no-sync python src/stream_entities.py --help
```

Based on the previously used Part 1 command interface, an example is:

```bash
# Ubuntu, inside: tmux new -s gp1-ner
cd /mnt/storage2/cs4371-work/GP1
export TMPDIR="$PWD/spark-tmp" SPARK_LOCAL_DIRS="$PWD/spark-tmp" SPARK_LOCAL_IP=127.0.0.1
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 UV_CACHE_DIR="$PWD/.uv-cache"
uv run --no-sync python src/stream_entities.py \
  --input-topic topic1-both-demo \
  --output-topic topic2-both-demo \
  --database data/both_demo_entity_counts.db \
  --checkpoint checkpoints/both-demo-ner \
  --trigger-seconds 15
```

Make the Kafka topics first:

```bash
for t in topic1-both-demo topic2-both-demo; do
  docker exec cs4371-gp1-kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 \
    --create --if-not-exists --topic "$t" \
    --partitions 1 --replication-factor 1
done
```

Then start classifier with `--topic topic1-both-demo --checkpoint checkpoints/both-demo-classifier`, and producer with `--topic topic1-both-demo --database data/both_demo_producer.db --output data/both_demo_messages.jsonl`. Use the Part 2 session commands from §2.5 with those substitutions. Do not run two copies of the NER processor against the *same* SQLite database. If memory pressure or Spark session startup conflicts arise, use the working separate demonstrations and report the limitation accurately.

## 3. Transferring files, backups and screenshots

**Mac: copy the human-label CSV down from Ubuntu**:

```bash
scp 'user@YOUR_SERVER_IP:/mnt/storage2/cs4371-work/GP1/outputs/part2/hand_label_50/part-*.csv' .
```

**Mac: send edited human labels back to Ubuntu**:

```bash
scp part2_manual_50.csv \
  user@YOUR_SERVER_IP:/mnt/storage2/cs4371-work/GP1/outputs/part2/
```

For screenshots: capture actual terminal tables and the Kibana browser dashboard on the Mac. Do not generate or substitute made-up dashboard screenshots.

**Backup directory (DO NOT edit or delete)**:

```text
/mnt/storage2/server-backup/
├── immich/
├── nextcloud/
├── CS4371-GP1/
├── homeserver/
├── docker-volumes/
└── system/
```

The backup of Project 1 predates some Part 2 work. Make an **additional** copy of the current working repository to a new backup destination or Git repository before the SSD migration, without overwriting the old backup. Exclude `.venv`, caches and Spark temporary files; preserve `src/`, training model, experiment JSONL, outputs and necessary metadata. Keep private `.env` files and credentials out of public Git repositories.

## 4. Final demonstration script (suggested 8-minute maximum)

1. **Introduction (0:00–0:40):** team members, motivation, Hacker News API, architecture.
2. **Part 1 code/data (0:40–2:00):** producer, Kafka, Spark NER, cumulative counts; show `run01` snapshots and 405 processed messages.
3. **Part 1 visualization (2:00–3:00):** Kibana top-10 entities at 15/30/45/60 minutes, if recovered; otherwise clearly explain unavailable dashboard and show actual saved snapshots.
4. **Part 2 training (3:00–4:00):** 120,000 AG News records, four equal classes, preprocessing/TF-IDF/logistic regression, saved model.
5. **Part 2 batch predictions (4:00–5:10):** 1,205 raw, 1,165 cleaned, four-class distribution, five examples/class.
6. **Human evaluation (5:10–6:00):** 50 labels, six `None`, 34/44 correct = 77.27%; describe domain-shift errors.
7. **Part 2 streaming (6:00–7:20):** Kafka producer and cumulative Spark micro-batch class counts; show changes over two batches (and both NER and classifier simultaneously if demonstrated).
8. **Wrap-up (7:20–8:00):** limitations, reproducibility, team contributions. Each member narrates the portion they built.

## 5. Submission checklist

Per the course assignment:

- Code for **both parts**, plus a README with run commands and parameterized paths (avoid submitting environment-specific absolute paths in code or Compose settings).
- Part 1 top-10 NER bar plots at 15, 30, 45 and 60 minutes, and a brief interpretation.
- Part 2 predictions and probabilities for every cleaned message, five examples per class, a CSV with all 50 **human** labels and predictions, and the 77.27% manual-evaluation summary plus error examples.
- One report PDF with the **team number**, members' **names and NetIDs**, and a **contribution table**.
- A publicly accessible/unrestricted narrated screen-recording **link**, max **8 minutes**, on the report's first page; **do not put the video file into the ZIP**.
- An appendix disclosing any AI assistants used, user and purpose, and the full chat transcripts as PDFs in `ai_transcripts/`.
- One ZIP named **`<TeamNumber>-GP1.zip`** submitted by one team member.

## 6. Troubleshooting

| Symptom | Explanation / action |
|---|---|
| Kafka topic exists but Spark shows `Batch 0` empty | Start producer on the **same** topic; check Kafka topic offset and checkpoint |
| Kafka has topic history but stream doesn't replay | Existing checkpoint overrides fresh `startingOffsets`; use a new topic + checkpoint for a fresh demo |
| Producer `Pending > 0` with `--no-kafka` | Expected: messages were saved/exported but not acknowledged by Kafka |
| Spark logs `NativeCodeLoader` or `DAGScheduler Broadcasting large task binary` warnings | Usually nonfatal; look for real exceptions and whether batches advance |
| Elasticsearch status `yellow` | On this one-node server, replica shards are unassigned; verify zero unassigned primary shards |
| Kibana `EIO: i/o error, read` | Existing OS HDD has physical read errors; do not erase volumes. Check container logs/disk health. Dashboard restoration unverified |
| CSV opens with shifted columns | Spark CSV export must escape embedded quotes: `.option('quoteAll', True).option('escape', '"')`; validate with Python `csv.DictReader` |
| Google Sheets probabilities show huge numbers | Locale decimal separator issue; set United States locale and import corrected CSV as data. Don't overwrite human labels |
| Accuracy shows 68% instead of 77.27% | Exclude human label `None` (not `None of these`) from eligible accuracy: 34/44 = 77.27% |
| `Permission denied` writing under `/mnt/storage2` | Create only the needed project subdirectory with `sudo`, then `chown` that subdirectory; don't change ownership of the whole drive |
| `uv sync` appears stalled | PySpark is large; use `uv sync -v` to see download/preparation progress |

## 7. Reproducibility and integrity notes

- Part 1 and Part 2 experiments were performed on different dates. Keep collected-message timestamps and state distinct; don't present new Part 2 messages as part of the original 60-minute Part 1 run.
- Batch class counts are **model predictions**, not audited ground truth.
- `None` is a human-label category only, not a fifth AG News model class.
- Kafka+Spark live counts are **cumulative** and depend on topic/checkpoint state.
- The saved Elasticsearch volume is a cold filesystem copy for matching versions, not a substitute for a tested Elasticsearch snapshot and restore.
- Saved artifacts and screenshots should reflect actual observed output. The final simultaneous NER+classifier demo still requires verification.
