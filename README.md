# CS4371 Group Project 1 
This project analyzes Hacker News stories and comments using two pipelines: Part 1 performs named entity recognition (NER) and visualizes entity frequencies; Part 2 uses a trained AG News classifier for offline evaluation and live streaming inference.

## Start here: what this guide runs

Follow Sections 1–8 in order. Choose your operating system in each platform-specific block; unlabeled commands apply to Linux, macOS and Windows (Ubuntu WSL 2). The default workflow uses the **committed 60-minute snapshots, exported Kibana dashboard, fitted AG News model, original batch prediction CSVs and manual labels**. No model retraining, new offline batch job, or 60-minute data collection is required. The only new collection is a short, two-minute Part 2 live demonstration.

The appendices at the very bottom are independent, optional procedures for readers who want to repeat expensive experiments or training. Do not execute them as part of the default workflow.

## 1. Architecture

```mermaid
flowchart LR
    H[Hacker News API] --> P[Python producer] --> K[Kafka topic1]
    K --> N[Part 1: Spark + spaCy NER]
    N --> T[Kafka topic2] --> L[Logstash] --> E[Elasticsearch] --> V[Kibana]
    N --> S[SQLite counts and JSON snapshots]
    S --> I[Snapshot importer] --> E
    K --> C[Part 2: Spark streaming classifier] --> R[Running category counts]
    A[AG News training dataset] --> M[MLlib training] --> F[Saved model]
    F --> C
    F --> B[Offline batch classifier]
    D[Recorded HN JSONL] --> B --> O[CSV predictions and manual evaluation]
```

The offline batch predictions are not inputs to the live classifier. Both classifiers load the same fitted model. Kafka, Logstash, Elasticsearch and Kibana run in Docker; Python and Spark run on the host.

## 2. Check and install prerequisites

### 2.1 Choose your operating system

The original project and the default reproduction workflow were tested on Ubuntu with Python 3.14, Java 21, PySpark 4.1.3, Kafka 4.1.2 and Elastic Stack 9.5.5. The following are installation paths for different operating systems; native macOS and Windows/WSL execution have not been independently tested.

- **Linux:** Use Bash and Docker Engine with Compose v2.
- **macOS:** Use Terminal (Bash or zsh) and Docker Desktop for your CPU architecture. If dependencies or images are unavailable for Apple Silicon, use a compatible Linux VM or remote Linux machine.
- **Windows:** Use WSL 2 with Ubuntu and Docker Desktop WSL integration. The project shell scripts are written for a Unix-like shell, not native PowerShell. Keep the clone in the WSL filesystem, not under `/mnt/c`.

Windows users without WSL: run the following one-time command in administrator PowerShell, reboot if requested, and finish Ubuntu setup before proceeding in the Ubuntu terminal:

```powershell
wsl --install -d Ubuntu
```

Install Docker Desktop on Windows or macOS if missing: https://docs.docker.com/get-docker/. Start it, and on Windows enable integration for the chosen WSL distribution. On native Linux, install Docker Engine and the Compose plugin using the distribution-specific instructions at the same link. Do not install a competing Docker daemon inside WSL when using Docker Desktop.

### 2.2 Check installed tools

**Where:** Your chosen project terminal. On Windows this means WSL, not PowerShell.

```bash
git --version
python3 --version
java -version
uv --version
docker --version
docker compose version
tmux -V
```

A missing-command error identifies a prerequisite to install. The project requires Java 21 and Python 3.14; uv can install Python without replacing the system Python. A different system `python3` version is acceptable if the project uses Python 3.14 through uv.

### 2.3 Install missing tools (only your platform)

Use the applicable subsection below, then proceed to Step 2.4. Commands are separate so you can skip tools that are already installed.

**Ubuntu/Debian or Ubuntu WSL**

For missing Git, curl or tmux, use apt (omit packages already installed):

```bash
sudo apt update
sudo apt install -y git curl tmux
```

For missing Java 21:

```bash
sudo apt install -y openjdk-21-jdk-headless
```

For missing uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Make uv available if installed in the default user directory:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Other Linux distributions should use their own package manager for Git, curl, tmux and Java 21.

**macOS**

Install Homebrew first if needed, following https://brew.sh/. Then install only the missing packages, using the matching command(s):

```bash
brew install git
```

```bash
brew install tmux
```

```bash
brew install openjdk@21
```

```bash
brew install uv
```

### 2.4 Configure Java 21

Run **only the block for your Java installation**, not all three blocks. These exports apply to the current shell; repeat in a new shell when needed.

**Linux / WSL (system Java 21 selected):**

```bash
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"
```

**macOS with Homebrew Java 21:**

```bash
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"
```

**macOS with a registered Java 21 JDK, such as Adoptium (instead of Homebrew):**

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"
```

Verify Java. If the version is not 21, select the Java 21 installation before proceeding.

```bash
"$JAVA_HOME/bin/java" -version
```

### 2.5 Install and verify the project Python version

```bash
uv python install 3.14
uv python find 3.14
```

Expected: a valid Python 3.14 executable path. uv may reuse an existing compatible interpreter.

### 2.6 Final prerequisite check

**All platforms:**

```bash
git --version
uv --version
uv python find 3.14
java -version
docker compose version
tmux -V
```

**Linux (native Docker Engine):**

```bash
sudo docker info
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker info
```

Expected: Java 21, Python 3.14 available through uv, and Docker Client and Server information. Start Docker Desktop before checking on macOS or Windows. On Windows, enable integration for your Ubuntu WSL distribution.

Native Linux commands use `sudo` for Docker daemon access, as in the tested Ubuntu setup. Docker Desktop commands run as the current user. Do not change Docker socket permissions. The optional experiment runner has a separate Docker access requirement in Appendix A.

## 3. Clone and configure the project

### 3.1 Clone the repository

**Where:** Project terminal, in a directory where you want the repository stored. Access to the GitHub repository may require authorization.

```bash
git clone https://github.com/mhuy26/CS4371-Project1.git
cd CS4371-Project1
```

Expected: the repository downloads successfully. All subsequent shell commands run from this repository root unless explicitly stated otherwise. Do not copy any personal storage paths from another machine.

### 3.2 Install the locked dependencies

```bash
uv sync --locked --python 3.14
uv run --no-sync python --version
```

Expected: a new `.venv` and Python 3.14. Install the English spaCy model into this environment:

```bash
uv pip install pip
uv run --no-sync python -m spacy download en_core_web_sm
```

Verify:

```bash
uv run --no-sync python -c 'import pyspark, spacy, kafka; print("PySpark:", pyspark.__version__); print("spaCy:", spacy.__version__); print("Kafka:", kafka.__version__); spacy.load("en_core_web_sm"); print("NER model loaded")'
uv lock --check
```

Expected: PySpark 4.1.3, spaCy 3.8.16 and kafka-python 3.0.11 in the original locked environment, plus a successful NER model load. The Spark Kafka connector must match PySpark 4.1.3. Internet access is needed for installation and Spark connector resolution.

### 3.3 Configure environment and local storage

```bash
cp .env.example .env
mkdir -p docker-data/kafka docker-data/elasticsearch spark-tmp
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_DIRS="$PWD/spark-tmp"
```

This assumes a fresh clone without `.env`. If repeating this setup on an existing clone, retain the existing `.env` instead of replacing it. The template uses `GP1_DATA_DIR=./docker-data`, and the model path is relative to the repository. On a different storage device, set `GP1_DATA_DIR` in `.env` to your own writable directory. The saved model is already supplied; ignore any older template comment requesting initial training. Never commit `.env` or credentials.

### 3.4 Check the supplied reproduction artifacts

```bash
ls experiments/run01/snapshots/
ls config/kibana-dashboard.ndjson
ls models/agnews_tfidf_lr/
wc -l data/part2_combined.jsonl
ls outputs/part2/all_predictions/part-*.csv
ls outputs/part2/part2_manual_50.csv outputs/part2/evaluation.txt
```

Expected: four historical snapshots (15, 30, 45, 60 minutes), the exported dashboard, model directories `metadata` and `stages`, 1,165 JSONL lines, and prediction/evaluation files. If these are missing, verify repository access and that you cloned the current main branch before proceeding.

## 4. Start and verify Docker infrastructure

### 4.1 Prepare storage and start the services

**Where:** Project terminal, repository root (Ubuntu WSL terminal on Windows).

On a fresh installation, prepare the bind-mounted directories before starting Docker. The following blocks assume `GP1_DATA_DIR=./docker-data` and the directories created in Section 3.3. If you changed that setting, create and use the corresponding directories instead.

**Linux (native Docker Engine):**

```bash
sudo chown 1000:1000 docker-data/kafka
sudo chown 1000:0 docker-data/elasticsearch
sudo chmod u+rwX,g+rwX docker-data/kafka docker-data/elasticsearch
ls -ldn docker-data/kafka docker-data/elasticsearch
```

Expected: Kafka ownership `1000:1000`, Elasticsearch ownership `1000:0`. This prevents the startup `AccessDeniedException` observed during the clean-clone test. Apply these commands only to the new project's own directories. If you changed `GP1_DATA_DIR`, use the corresponding paths. 

**macOS (Docker Desktop):** Section 3.3 creates the directories as your user. No manual Linux UID/GID changes are normally needed. Keep the repository in a Docker Desktop shared location and leave Docker Desktop running.

**Windows (Ubuntu WSL 2 + Docker Desktop):** Keep the repository inside the Ubuntu filesystem, not `/mnt/c`. For these Linux-filesystem bind mounts, prepare the container ownership as follows:

```bash
sudo chown 1000:1000 docker-data/kafka
sudo chown 1000:0 docker-data/elasticsearch
sudo chmod u+rwX,g+rwX docker-data/kafka docker-data/elasticsearch
ls -ldn docker-data/kafka docker-data/elasticsearch
```

Docker Desktop itself runs without `sudo` in WSL. These storage preparation steps are platform guidance; only native Ubuntu has been tested.

Check for existing containers before starting:

**Linux (native Docker Engine):**

```bash
sudo docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

If another copy of this project exists, stop here and resolve both the Compose project name and ports (9092, 9200, 5601) before proceeding. Do not start a second clone under the original `cs4371-gp1` project name. For a separate test stack, use a unique `COMPOSE_PROJECT_NAME` in `.env`, a separate data directory, and free those host ports by stopping only the original project services from its own directory. If the project name changes, substitute the corresponding container names in the commands below. Do not stop unrelated services or delete volumes.

Start the services:

**Linux (native Docker Engine):**

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.elastic.yml config --quiet
sudo docker compose -f docker-compose.yml -f docker-compose.elastic.yml up -d
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker compose -f docker-compose.yml -f docker-compose.elastic.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.elastic.yml up -d
```

Expected: Kafka, Elasticsearch, Kibana and Logstash start. Allocate sufficient RAM and disk space for Docker and host Spark.

### 4.2 Verify the infrastructure

**Linux (native Docker Engine):**

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.elastic.yml ps
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker compose -f docker-compose.yml -f docker-compose.elastic.yml ps
```

Expected: all four containers show `Up`. Check Kafka directly inside its container:

**Linux (native Docker Engine):**

```bash
sudo docker exec cs4371-gp1-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --list
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker exec cs4371-gp1-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --list
```

Expected: successful topic listing (a new broker may not have project topics yet). The default container name uses the Compose project name `cs4371-gp1` from `.env.example`. Host applications connect to `localhost:9092`; container-to-container connections use `kafka:19092`.

Check Elasticsearch:

```bash
curl -fsS 'http://127.0.0.1:9200/_cluster/health?pretty'
```

Expected: green or yellow, with zero unassigned primary shards. Yellow is normal on a single-node cluster with unassigned replicas. Check Kibana:

```bash
curl -I http://127.0.0.1:5601
```

Expected: HTTP response after Kibana finishes initialization. Inspect Logstash startup:

**Linux (native Docker Engine):**

```bash
sudo docker logs --tail 60 cs4371-gp1-logstash-1
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker logs --tail 60 cs4371-gp1-logstash-1
```

Expected: pipeline started, no persistent fatal connection errors. If any service is still starting, repeat its check before continuing. For a local demonstration the Elastic stack is configured without authentication; do not expose these ports publicly.

## 5. Part 1 — Restore the recorded results (no new experiment)

Part 1 collects Hacker News messages into Kafka topic1, runs spaCy NER in Spark, aggregates entity counts, and publishes results into topic2. Logstash indexes those results in Elasticsearch. Separately, the recorded interval snapshots are imported into a historical index for Kibana. The default workflow below restores those snapshots rather than collecting more data.

### 5.1 Inspect the recorded experiment and import its snapshots

```bash
ls -lh experiments/run01/snapshots/
```

Expected: `entities_15min.json`, `entities_30min.json`, `entities_45min.json`, `entities_60min.json`.

Import the four saved snapshots into the historical index:

```bash
uv run --no-sync python part1/src/import_snapshots.py \
  --snapshot-dir experiments/run01/snapshots \
  --es-url http://localhost:9200 \
  --index cs4371-entities-history-run01
```

Expected: four snapshots imported, totaling 845 historical entity records (149, 191, 233 and 272 records at the respective intervals). No new 60-minute experiment is required.

After the import, verify the index:

```bash
curl -fsS 'http://127.0.0.1:9200/cs4371-entities-history-run01/_count?pretty'
```

Expected: `count: 845`. If the index is missing or empty, resolve the importer step before proceeding to the dashboard.

### 5.2 Import the saved Kibana dashboard

**Where:** Project terminal, repository root.

```bash
curl -fsS -X POST 'http://127.0.0.1:5601/api/saved_objects/_import' \
  -H 'kbn-xsrf: true' \
  --form file=@config/kibana-dashboard.ndjson
```

Expected: `"success": true`, `"successCount": 3`. If the import reports conflicts in a dedicated reproduction instance, repeat the same command with `?overwrite=true` appended to the URL. Do not overwrite saved objects in an unrelated instance. The NDJSON contains saved dashboard and data-view definitions, not the underlying Elasticsearch documents.

### 5.3 Open Kibana and view the charts

If the browser is on the same machine as Docker, open `http://localhost:5601` (on Windows with WSL, use the Windows browser and Docker Desktop's forwarded localhost port).

If the project is on a remote machine, use a **separate terminal on your client computer** to run the following command instead. Replace USER and HOST with your actual SSH login and server address. Keep that terminal open and open `http://localhost:5601` in the client browser:

```bash
ssh -N -L 5601:127.0.0.1:5601 USER@HOST
```

In Kibana, go to **Dashboards → CS4371 GP1 — Historical Entity Frequencies**. Set an absolute date range from October 23, 2026 00:00 through October 24, 2026 23:59 if needed and refresh. The four panels show the top ten entity names by sum of count, filtered by `elapsed_minutes` 15, 30, 45 and 60.

Expected historical result:

| Interval | AI occurrences |
|---|---:|
| 15 minutes | 17 |
| 30 minutes | 23 |
| 45 minutes | 27 |
| 60 minutes | 30 |

The original producer delivered 405 messages. These are recorded results, not a new experiment. The saved dashboard should show all four panels; if empty, check the historical index, date range and data view. This completes Part 1 for the default demonstration.

## 6. Part 2 — Inspect the supplied model and offline results

### 6.1 Verify the fitted model

```bash
ls -lh models/agnews_tfidf_lr/
```

Expected: `metadata` and `stages`. Use this model for the rest of the guide. It was trained on 120,000 AG News examples with RegexTokenizer, StopWordsRemover, HashingTF, IDF and LogisticRegression. The repository includes the fitted model; **do not train it again for the default workflow**.

### 6.2 Inspect the original batch predictions

```bash
uv run --no-sync python part2/src/part2_inspect.py
```

Expected: 1,165 predictions and example texts with category and probability.

| Category | Count |
|---|---:|
| World | 45 |
| Sports | 122 |
| Business | 193 |
| Sci/Tech | 805 |
| Total | 1,165 |

### 6.3 Verify the manual evaluation

```bash
cat outputs/part2/evaluation.txt
uv run --no-sync python part2/src/part2_evaluate.py \
  --labeled-csv outputs/part2/part2_manual_50.csv
```

Expected: 50 manually reviewed, 6 outside the four categories, 44 eligible, 34 correct, 77.27% accuracy. This lightweight evaluation uses the original labeled predictions; it does not run Spark training or classify the dataset again. Prediction counts alone are not accuracy measurements. Both offline and streaming classifiers use the same saved model, not each other's predictions.

## 7. Part 2 — Run a short live streaming demo

### 7.1 Create a fresh demo topic and state directory

**Where:** Project terminal outside tmux. This example uses the ID `part2-repro01` for a first run. If reusing a machine, use a fresh ID consistently in all commands to avoid accumulating previous counts.

**All platforms:**

```bash
mkdir -p experiments/part2-repro01/checkpoint
```

**Linux (native Docker Engine):**

```bash
sudo docker exec cs4371-gp1-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --create \
  --topic topic1-part2-repro01 --partitions 1 --replication-factor 1
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker exec cs4371-gp1-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --create \
  --topic topic1-part2-repro01 --partitions 1 --replication-factor 1
```

Expected: `Created topic topic1-part2-repro01.` If it already exists, choose another fresh ID and use it throughout Section 7. The new database, JSONL and checkpoint paths are separate from committed files.

### 7.2 Start tmux — Window 0 (classifier)

**Where:** Project terminal, repository root, outside tmux. Java and uv must be configured as in Section 2.

```bash
tmux new-session -s part2-repro -n classifier
```

Check the status bar at the bottom:

```text
[part2-repro] 0:classifier*
```

- `0:classifier` = Window 0, the classifier window.
- `*` = the window you are currently viewing.

**Stay in Window 0 (`classifier*`).** Run:

```bash
export PATH="$HOME/.local/bin:$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_DIRS="$PWD/spark-tmp"
uv run --no-sync python part2/src/part2_stream.py \
  --model models/agnews_tfidf_lr \
  --topic topic1-part2-repro01 \
  --bootstrap localhost:9092 \
  --checkpoint experiments/part2-repro01/checkpoint \
  --trigger-seconds 10 --from-earliest
```

Expected: Spark loads the model and starts streaming. Leave it running. If it exits with a traceback, fix the error before continuing.

### 7.3 Create and enter Window 1 (producer)

**Where:** Window 0, while Spark is running.

1. Press `Ctrl+B`.
2. Release both keys.
3. Press `c`.

A new window opens. Run:

```bash
tmux rename-window producer
```

Check the status bar:

```text
[part2-repro] 0:classifier- 1:producer*
```

`producer*` means you are now in Window 1. Window 0 continues running. Switching windows does not stop either process.

### 7.4 Start the producer

**Where:** Window 1 (`producer*`), repository root. If uv is not found, run `export PATH="$HOME/.local/bin:$PATH"`.

```bash
uv run --no-sync python part1/src/producer.py \
  --broker localhost:9092 \
  --topic topic1-part2-repro01 \
  --interval 5 --duration 2 --max-items 100 \
  --database experiments/part2-repro01/producer.db \
  --output experiments/part2-repro01/messages.jsonl
```

Expected: messages are published to Kafka for two minutes. The polling interval is five seconds; `--max-items` is not a guaranteed total message count. The demo uses separate database and output files.

### 7.5 Return to Window 0 and watch live classification

**Where:** Window 1, while the producer is running.

1. Press `Ctrl+B`.
2. Release both keys.
3. Press `0` (zero).

Check the status bar:

```text
[part2-repro] 0:classifier* 1:producer-
```

You are now in Window 0. Watch the `Batch: N` tables with `predicted_class` and `count`. These are streaming micro-batches, with cumulative category counts updated as new messages arrive.

### 7.6 Wait for the producer to finish

**Where:** Window 0.

1. Press `Ctrl+B`.
2. Release both keys.
3. Press `1`.

Check that `1:producer*` is active. Wait until the producer finishes automatically and returns to the shell prompt. Note its delivered-message total. Do not press Ctrl+C here.

### 7.7 Stop the classifier

**Where:** Window 1, after the producer finishes.

1. Press `Ctrl+B`.
2. Release both keys.
3. Press `0`.

Check that `0:classifier*` is active. Allow remaining micro-batches to finish and record the last counts. Verify Spark progress against Kafka end offsets before claiming every message was consumed; empty text is filtered.

Press `Ctrl+C` **in Window 0** to stop the classifier. Spark otherwise waits indefinitely for future Kafka messages, even after the producer exits.

Expected: the shell prompt returns. A Py4J traceback may occur during interruption; it does not invalidate previously completed batches.

Verify the processes have stopped.

**Linux / Windows (Ubuntu WSL 2):**

```bash
pgrep -af '[p]art2_stream.py|[p]roducer.py'
```

**macOS:**

```bash
pgrep -fl '[p]art2_stream.py|[p]roducer.py'
```

Expected: no demo processes. Do not stop unrelated processes.

Detach from tmux:

1. Press `Ctrl+B`.
2. Release both keys.
3. Press `d`.

You are back in the normal terminal. If you detach accidentally during the demo, run `tmux attach -t part2-repro` from the normal terminal and use the status bar to identify the active window.

For reference, the original demonstration displayed World 51, Sports 67, Business 119 and Sci/Tech 431 (668 total) using a reused checkpoint. A fresh demo will have different counts.

## 8. Completion and shutdown

The default workflow is now complete. Original Part 1 charts, saved Part 2 offline predictions, the original manual evaluation, and a new short streaming demonstration are the intended evidence. To stop only this project's Docker services after the demonstration, run from the repository root:

**Linux (native Docker Engine):**

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.elastic.yml stop
```

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):**

```bash
docker compose -f docker-compose.yml -f docker-compose.elastic.yml stop
```

This preserves volumes and data. Do not run `down -v` or delete historical snapshots/checkpoints simply to repeat the demo.

### Reproducibility and verification status

- The original project and live demos were executed successfully on Ubuntu.
- GitHub synchronization, a fresh repository clone, installed dependencies, the lock file, and committed datasets/model/predictions/dashboard were verified.
- On November 16, 2026, the default workflow was reproduced from a fresh GitHub clone on Ubuntu: Docker startup after permission fixes, import of 845 historical records and three Kibana saved objects, matching historical charts, and live Part 2 classification with the supplied model.
- The optional 60-minute rerun and model retraining were not repeated in this test. Native macOS and Windows/WSL execution remain unverified.
- Historical results are fixed artifacts; new Hacker News streams vary over time. The preserved model and input dataset are the basis for comparing offline predictions. No accuracy claim is made for the new unlabelled live stream.

### Troubleshooting

| Problem | Action |
|---|---|
| `uv: command not found` | Check Step 2.3 and add `$HOME/.local/bin` to PATH if applicable. |
| `JAVA_GATEWAY_EXITED` | Verify Java 21 and `JAVA_HOME` in the classifier shell. |
| Missing spaCy model | Repeat the model installation in Step 3.2. |
| Checkpoint mkdir failure | Use a writable relative checkpoint directory and create it before starting Spark. |
| Docker socket permission denied | Use the platform-specific Docker block in Step 2.6; on Docker Desktop verify the daemon and WSL integration. |
| Kafka or Elasticsearch data permission denied | Linux/WSL: verify ownership in Step 4.1. macOS: verify Docker Desktop file sharing and directory access. |
| Kafka connection refused | Check container status and host port 9092. |
| Kibana says Configure Elastic | Verify Elasticsearch is running first. |
| Empty historical panels | Verify imported index, data view and date range. |
| No live classifications | Ensure producer and classifier use the same fresh topic and inspect Spark logs. |
| Ctrl+C Py4J traceback | Check the last completed batch and verify the classifier process exited. |

### References

- Hacker News API: https://github.com/HackerNews/API
- AG News: https://huggingface.co/datasets/fancyzhx/ag_news
- Spark: https://spark.apache.org/
- Kafka: https://kafka.apache.org/
- spaCy: https://spacy.io/
- Elastic: https://www.elastic.co/

---

# OPTIONAL APPENDICES — NOT PART OF THE DEFAULT WORKFLOW

The preceding guide is sufficient to use the provided model and historical experiment artifacts and demonstrate streaming. Only follow an appendix if you specifically want to regenerate that component. Start from the configured project root, with Java and uv available; start the Docker services again using Section 4 if the chosen procedure needs them. Keep all regenerated outputs separate from the committed artifacts.

## Appendix A — Repeat the Part 1 experiment (60 minutes)

This optional procedure runs the combined experiment runner, so both NER and classification execute using the supplied fitted model. Skip this entire appendix if you only need the original historical results.

### A.1 Run a short test first

**Where:** Project terminal, repository root. Check that `experiments/reproduce01` does not exist before running. Use a different fresh ID if needed. The runner invokes Docker internally without `sudo`. Prepare access before running it.

**Linux (native Docker Engine):** The optional runner requires non-root Docker access. Only on a trusted machine, configure membership in the Docker group (which grants root-equivalent privileges):

```bash
sudo usermod -aG docker "$USER"
```

Log out and back in, return to the repository root, restore the Java and uv environment from Section 2, and verify:

```bash
docker info
```

Do not run the entire experiment script with `sudo`.

**macOS / Windows (Ubuntu WSL 2 with Docker Desktop):** Start Docker Desktop (enable WSL integration on Windows) and verify:

```bash
docker info
```

**All platforms:**

```bash
tmux new-session -s gp1-experiment -n runner
```

**Where:** tmux Window 0 (`runner`).

```bash
export SPARK_LOCAL_IP=127.0.0.1
bash part1/src/run_experiment.sh reproduce01 3 models/agnews_tfidf_lr
```

Expected: both queries start, the producer collects for three minutes, four short-interval snapshots are saved and the runner reports validation results. Wait for the runner to finish before the next step. Inspect the reported delivery and classifier counts.

### A.2 Execute the full experiment

**Where:** Same runner window after A.1 completes successfully. Use an unused ID `reproduce60`.

```bash
bash part1/src/run_experiment.sh reproduce60 60 models/agnews_tfidf_lr
```

Expected: 60 minutes of collection plus initialization and final backlog processing. The runner starts both queries, collects four snapshots at quarter-duration intervals, shuts down collection and performs its validation. Do not interrupt this run if all four intervals are required.

### A.3 Inspect the new results

```bash
ls -lh experiments/reproduce60/snapshots/
ls experiments/reproduce60/logs/
```

Expected: `entities_15min.json`, `entities_30min.json`, `entities_45min.json`, and `entities_60min.json`, and logs. Inspect runner validation (previously reported `PASS: Producer, NER and snapshots verified`) and classification output. A NER check alone does not establish full classifier consumption.

### A.4 Visualize the new experiment (not the original dashboard)

The current Logstash configuration subscribes to `topic2` and `topic2-run01`; it does not automatically ingest a new `topic2-reproduce60`. For fresh live indexing, adapt the Kafka input subscription and Elasticsearch output routing in `config/logstash.conf` to the new topic and a distinct index, then reload/restart Logstash. Inspect the actual settings first. For historical visualization, import the new snapshots into a separate index:

```bash
uv run --no-sync python part1/src/import_snapshots.py \
  --snapshot-dir experiments/reproduce60/snapshots \
  --es-url http://localhost:9200 \
  --index cs4371-entities-history-reproduce60
```

Create a separate data view and dashboard using the panel filters from Step 5.3 and the new collection dates. Do not overwrite the original `run01` index or snapshot directory.

## Appendix B — Retrain the AG News model

The commands in this optional appendix were not rerun during the November 16 reproduction test. Check the scripts’ `--help` if your checkout differs.

Skip this appendix if you want to use `models/agnews_tfidf_lr`. That is the fitted model used throughout the main workflow.

### B.1 Download the training dataset

**Where:** Project terminal, repository root.

```bash
uv run --no-sync python part2/src/part2_get_agnews.py --output data/ag_news
```

Expected: training Parquet under `data/ag_news`, containing 120,000 labeled training examples for the original dataset. Download behavior depends on upstream dataset availability. To reproduce the exact training data in future, record the dataset revision and checksums.

### B.2 Train a new model

Choose a new, unused output directory. Do not overwrite the supplied fitted model.

```bash
uv run --no-sync python part2/src/part2_train.py \
  --ag-news data/ag_news \
  --model models/agnews_retrained
```

Expected: fitted pipeline saved to `models/agnews_retrained`. Training requires Java, Spark and adequate resources; exact newly fitted parameters and probabilities may vary.

### B.3 Verify the new model

```bash
ls -lh models/agnews_retrained/
```

Expected: `metadata` and `stages`. This model is separate from the original. If you want to use it for an independent live run, follow Section 7 with a fresh topic/checkpoint and substitute `models/agnews_retrained` for the model path. Do not claim the original 77.27% accuracy applies to the retrained model without evaluation.

## Appendix C — Regenerate the offline batch predictions

This appendix is not needed to inspect the original prediction counts or evaluate the saved worksheet. It uses the original supplied model by default, regardless of whether Appendix B was run.

### C.1 Generate new CSV predictions

**Where:** Project terminal, repository root. Choose a new output directory to avoid Spark overwrite of original results.

```bash
uv run --no-sync python part2/src/part2_batch.py \
  --input data/part2_combined.jsonl \
  --model models/agnews_tfidf_lr \
  --output outputs/part2-reproduced
```

Expected: `all_predictions`, `five_per_class` and `hand_label_50` directories under `outputs/part2-reproduced`. The original input contains 1,165 rows; original predicted totals appear in Section 6.2. Inspect generated counts before asserting exact agreement.

### C.2 Inspect the regenerated files

```bash
find outputs/part2-reproduced -name 'part-*.csv'
```

Expected: prediction CSVs and a 50-message labeling worksheet. For a newly generated sample, assign human labels independently (`World`, `Sports`, `Business`, `Sci/Tech`, or `None`) and save the labeled CSV separately. Do not copy old labels by row position; samples can differ. Evaluate using `part2_evaluate.py --labeled-csv` and the new labeled file's path. For exact evaluation of the original labels, use Step 6.3.
