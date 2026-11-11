# PIJD: Prompt Guard

Current path: **browser → Flask → Kafka → intake consumer (P1/P2) → PostgreSQL**, with separate P7/P8 and P9/P10 workers.
Dataset replay feeds the same Kafka/consumer path.
The browser shows **Queued** after Kafka accepts a prompt, then **Pending check**
after the consumer saves its original text and preprocessing snapshot. P4 live scoring remains future work. A separate agent worker
and temporary guard stub are available for the allowed-prompt demo (see [Agent](#agent)).

See the [folder guide](docs/folder-guide.md), [current architecture](docs/architecture-work-plan-v2.md),
and [database sketch](docs/database-schema.dbml).

## Start the complete local stack

With Docker running, from the project root:

```sh
docker compose -f backend/compose.yaml up --build -d --wait
```

Open <http://127.0.0.1:5050>. This starts Flask, the intake consumer, P7 router, P8 aggregation worker, P9/P10
burst worker, Kafka 3.9.1 in KRaft mode, and PostgreSQL 16. A one-shot initialization
service creates the tables and four topics before Flask and the workers start. Re-running startup
preserves existing records and messages.

Ports 5050, 55432, and 9094 must be free. If this project's standalone PostgreSQL
instance occupies 55432, stop it first without deleting its data:

```sh
.venv/bin/python backend/scripts/local_db.py --stop
```

The Compose database uses its own named volume; it does not import
`tmp/postgres-dev/`. Preserve that directory if you have saved local prompts.
Full Compose supplies `db:5432` and `kafka:9092` internally, independently of
host-side settings in `.env`. The image excludes local environment files.

Stop the stack while preserving data:

```sh
docker compose -f backend/compose.yaml stop
```

Do not use `down -v` when you want to keep database records and Kafka messages.

## Submitting and checking prompts

`POST /api/prompts` accepts JSON `{"text": "your prompt"}`, with an optional UUID
`prompt_id`. The browser generates that UUID and reuses it when retrying the
same submission. Input must be nonblank text of at most 8,000 characters, without
null characters. Original whitespace, Unicode and markup are stored literally.

A successful response is **HTTP 202**, after Kafka acknowledges delivery:

```json
{
  "status": "queued",
  "prompt": {
    "id": "UUID",
    "text": "your prompt",
    "created_at": "UTC timestamp",
    "decision": "pending",
    "dataset_prompt_id": null
  }
}
```

Acceptance means the record reached Kafka; PostgreSQL storage happens asynchronously.
An unconfirmed delivery returns 503 within approximately five seconds. The
browser preserves the input and retry ID, including when the acknowledgement is uncertain.

- `GET /api/prompts/<uuid>`: stored prompt with `status: "stored"`; 404 while absent.
- `GET /api/prompts`: latest 50 stored prompts in chronological order.
- Database read outages return 503. The browser continues showing Queued and retries
  its storage check every two seconds until confirmation.

Refresh clears the visible chat without deleting records. Browser submissions do not yet
run a classifier or invoke the agent automatically. The UI stops polling once storage is
confirmed. There are no accounts or separate conversation histories.

## Kafka events and the consumer

| Topic | Current use |
|---|---|
| `prompts_raw` | Raw inputs consumed by the intake worker |
| `prompts_allowed` | Allowed-prompt publisher from P7 (or temporary guard stub) and optional agent worker |
| `prompts_labels` | Reserved for reviewed/evaluation labels |
| `throttle_decisions` | P10 rate-limiter decision per raw prompt, consumed by P7 (see [Decision routing and attack aggregation](#decision-routing-and-attack-aggregation-p7p8)) |

Each topic starts with one partition and replication factor one. Automatic topic
creation is off. Topic initialization leaves existing topics unchanged.

The existing **version-two** event matches the prompt table:

```json
{
  "schema_version": 2,
  "prompt_id": "UUID",
  "text": "original prompt text",
  "source": "127.0.0.1",
  "created_at": "2026-11-02T10:00:00+00:00",
  "dataset_prompt_id": null
}
```

Messages are keyed by `source`. This field is the source/IP identifier called
`source_ip` in the work plan: Flask uses the request IP, and the CLI accepts
simulated sources. Docker may expose a gateway IP for browser requests.
Labels stay in the dataset table and are not included in these events.

The consumer uses group `pijd-intake-v1`. It saves the prompt and initial pending
guard result together with P1/P2 preprocessing in one PostgreSQL transaction,
then commits that record's Kafka offset.
Delivery is at least once, with database deduplication by `prompt_id`:
matching duplicates preserve the original timestamp and any completed guard decision.
Changed text/source/dataset metadata under an existing ID is logged and skipped.

Malformed records and database constraint violations are logged with their
topic/partition/offset, then skipped. They remain in the raw topic until Kafka retention
expires (the broker default is seven days). Transient storage or offset-commit
failures stop the consumer without advancing further; Compose restarts it.
This worker is the entry point for later guard stages.

### P1/P2 preprocessing

P1 creates a separate `cleaned_text` copy using Unicode NFC normalization and
collapsed/trimmed whitespace. Case and punctuation remain intact. Its features
come from the **original** text: `char_count`, whitespace-separated `word_count`,
and `symbol_ratio` (non-alphanumeric, non-whitespace characters / total characters).
The original text remains available for model inference and agent execution.

P2 joins the exact `source` identifier to stored guard decisions checked within the
last ten minutes. `source_context` contains `allow_count`, `block_count`,
`review_count`, and an ISO UTC `as_of` timestamp. Pending decisions and dataset
labels are excluded. Zero counts mean no recent evaluated history, not a benign
verdict. P2 provides context; it does not classify or throttle prompts.

Each consumer process caches source context for 30 seconds; updates can therefore
take up to 30 seconds to appear. Expired entries are removed on lookups, and the
cache starts empty after a restart. Counts come from PostgreSQL, so duplicate
Kafka delivery does not increment them. Before a classifier is integrated, seed
completed decisions in test fixtures to demonstrate nonzero context.

The processing dictionary is stored in the nullable `prompts.preprocessing` JSONB
column. A matching redelivery preserves its first snapshot and any guard decision.
Older rows with no snapshot receive one only if their event is delivered again;
startup does not reset offsets or backfill old prompts. Re-run the normal Compose
startup (or `init-db`) to add the column and source index without replacing data.
HTTP responses and raw Kafka events keep their existing shapes.

Inspect saved preprocessing:

```sh
docker compose -f backend/compose.yaml exec db psql -U pijd -d pijd -c "SELECT id, preprocessing FROM prompts ORDER BY created_at DESC LIMIT 5;"
```

Inspect startup, processing, and consumer lag:

```sh
docker compose -f backend/compose.yaml ps -a
docker compose -f backend/compose.yaml logs --tail 50 init consumer web
docker compose -f backend/compose.yaml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group pijd-intake-v1
```

To demonstrate queue recovery, stop the consumer, submit a prompt, then start it:

```sh
docker compose -f backend/compose.yaml stop consumer
docker compose -f backend/compose.yaml start consumer
```

## Burst detection and rate limiting (P9/P10)

The `burst` service (`python -m backend.stream.burst_worker`, group `pijd-burst-v1`) reads
`prompts_raw` beside the intake consumer:

- **P9** counts every prompt per `source` in a sliding window of `BURST_WINDOW_SECONDS` (default 60),
  using the event's `created_at`, so replays produce the same bursts.
- **P10** blocks a source once its window reaches `BURST_THRESHOLD` prompts (default 10). The block
  lasts `BURST_COOLDOWN_SECONDS` (default 300) and is not extended by traffic during it.

For every prompt the worker stores one row in the `throttle_decisions` table, publishes the same
decision to the `throttle_decisions` topic (keyed by `prompt_id`), then commits its offset.
P7 consumes each event: `action: "block"` overrides classifier routing immediately, while
`allow` proceeds to the stored P4 score policy:

```json
{
  "schema_version": 1,
  "prompt_id": "UUID",
  "source": "source_01",
  "action": "block",
  "reason": "burst_detected",
  "window_count": 10,
  "window_seconds": 60,
  "threshold": 10,
  "blocked_until": "2026-11-10T12:05:09+00:00",
  "burst_started": true,
  "event_time": "2026-11-10T12:00:09+00:00",
  "decided_at": "2026-11-10T12:00:09.120000+00:00"
}
```

`action` is `allow` (`reason` and `blocked_until` null) or `block`, with `reason` set to
`burst_detected` on the prompt that starts a block (`burst_started: true`, a burst alert) or
`source_blocked` during the cooldown. Redelivery republishes the stored decision unchanged.
Active blocks are reloaded from PostgreSQL when the worker restarts.

## Decision routing and attack aggregation (P7/P8)

Compose runs two workers alongside intake and P9/P10:

- **P7 router** consumes `throttle_decisions`. P10 `block` immediately finalizes the prompt as
  `block`. P10 `allow` joins the stored P4 `malicious_score`; prompts with no score remain pending
  while later throttle events continue. P7 allows scores at or below 0.30, blocks scores at or
  above 0.70, and sends the middle band to `review`. A missing score is sent to review after
  30 seconds from the P10 decision. Only allowed prompts enter `prompts_allowed`.
- **P8 aggregation** refreshes a 60-second event-time window every 10 seconds. It counts P4's six
  attack types and records up to ten repeated normalized prompts per type (at least two occurrences).
  The current snapshot is in `attack_aggregate_current`; timestamped snapshots are in
  `attack_aggregate_history` and are retained for seven days.

P4 must save score/type output with `db.save_guard_score(...)` while leaving `decision` as
`pending`; P7 owns the final allow/block/review decision. The P7 allowed-prompt transactional
outbox retries Kafka publishing after a restart. P8 uses P1's NFC/whitespace-normalized
`preprocessing.cleaned_text`, falling back to the same normalization of the original text when
no preprocessing snapshot exists. It uses predicted attack types, not dataset labels.

P7 settings: `P7_ALLOW_MAX_SCORE` (0.30), `P7_BLOCK_MIN_SCORE` (0.70), and
`P7_SCORE_TIMEOUT_SECONDS` (30). P8 settings: `P8_WINDOW_SECONDS` (60),
`P8_SNAPSHOT_INTERVAL_SECONDS` (10), and `P8_HISTORY_RETENTION_SECONDS` (604800).
Scores must satisfy `0 <= allow threshold < block threshold <= 1`; timing values must be positive.

Run the workers directly when the local database and Kafka are available:

```sh
python -m backend.stream.router_worker
python -m backend.stream.aggregation_worker
```

Inspect routing and snapshots:

```sh
docker compose -f backend/compose.yaml logs --tail 50 router aggregation
docker compose -f backend/compose.yaml exec db psql -U pijd -d pijd -c "SELECT decision, count(*) FROM guard_results GROUP BY decision"
docker compose -f backend/compose.yaml exec db psql -U pijd -d pijd -c "SELECT * FROM attack_aggregate_current"
docker compose -f backend/compose.yaml exec db psql -U pijd -d pijd -c "SELECT snapshot_at, attack_type_counts, top_patterns FROM attack_aggregate_history ORDER BY snapshot_at DESC LIMIT 10"
```

Trigger a burst and inspect it (in PowerShell, pipe 12 lines the same way):

```sh
seq 12 | sed 's/^/Burst prompt /' | python -m backend.intake.producer --source source_01
docker compose -f backend/compose.yaml exec db psql -U pijd -d pijd -c "SELECT source, action, reason, window_count, burst_started FROM throttle_decisions ORDER BY event_time"
docker compose -f backend/compose.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic throttle_decisions --from-beginning --timeout-ms 5000
```

## Local Python and command-line publishing

For development outside the app containers:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
docker compose -f backend/compose.yaml up -d --wait db kafka
python -m backend.scripts.create_topics
```

Configure host-side connections in `.env`:

```dotenv
DATABASE_URL=postgresql://pijd:pijd@127.0.0.1:55432/pijd
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9094
```

Run `python -m flask --app backend.app init-db`, then start the consumer and
Flask in separate terminals:

```sh
python -m backend.intake.consumer
python -m flask --app backend.app run --host 127.0.0.1 --port 5050
```

Stop the Compose web/consumer services first if they are already running.
An existing PostgreSQL server or `backend/scripts/local_db.py` can also supply
the database; set `DATABASE_URL` accordingly.

The existing CLI remains available, with one argument or stdin line per prompt:

```sh
python -m backend.intake.producer "Hello" --source source_01
python -m backend.intake.producer "Dataset example" --source source_02 --dataset-prompt-id UUID
```

A dataset ID must already exist in the same database the consumer uses.
Its label can be joined to stored predictions afterward; labels do not enter the stream.

## Dataset replay

Replay reads **already-imported rows** from PostgreSQL; it does not download datasets
or require batch/model packages. With the local Python environment and connections
configured as above:

```sh
python -m backend.intake.replay --dataset neuralchemy --split test --limit 100 --rate 5 --sources 192.0.2.1,192.0.2.2
```

The same command can run inside the existing consumer container, using its internal
database/Kafka addresses: replace `python` with
`docker compose -f backend/compose.yaml exec consumer python`.

| Option | Meaning |
|---|---|
| `--dataset` | Required: `neuralchemy`, `deepset`, or `jackhhao` |
| `--split` | Required stored split, e.g. `train`, `validation`, `test`, or `external_test` |
| `--limit` | Maximum selected rows before validation; default 100 |
| `--rate` | Maximum prompts/second; default 5; 0 sends without intentional delays |
| `--sources` | Comma-separated simulated identifiers, assigned round-robin; default `192.0.2.1` |
| `--label` | Optional row-selection filter: 0 (benign) or 1 (malicious) |
| `--attack-group` | Optional stored attack-group filter, e.g. `obfuscation` |

Rows are selected in stable UUID order. Each submission has a fresh prompt UUID
and current timestamp, linked to its original dataset row. Original text is kept;
invalid or overlength rows are counted and skipped without truncation. Labels and
attack groups filter the selection but are absent from Kafka events.

Replay reuses one producer and confirms every delivery. Broker/database failures
stop the command; uncertain delivery reports the prompt ID. The summary prints
selected, acknowledged, and skipped counts. No valid submissions produces a failing
exit code. Rerunning intentionally creates new submissions, rather than resuming
or retrying the previous command. Actual throughput may be below the requested rate.

For a concentrated burst, use one source and a high rate (or 0):

```sh
python -m backend.intake.replay --dataset neuralchemy --split test --label 1 --limit 100 --rate 0 --sources 192.0.2.99
```

For a controlled change in attack style, run successive commands with different
`--attack-group` values, or switch from NeurAlchemy to an external test dataset,
keeping the desired sources/rate. P9/P10 process this traffic in parallel with
P1/P2. Drift monitoring remains a later stage. External test examples remain
excluded from training.

## Agent

The optional agent runs separately from the default intake/burst stack.

One LangChain agent with two deterministic tools, running a local model through [Ollama](https://ollama.com):

- `calculator(a, operation, b)`: `add`, `subtract`, `multiply` or `divide`, without `eval`.
- `lookup_course_info(topic)`: facts about this project (deadlines, timeline, stage owners, datasets, models, deliverables) from `backend/agent/course_info.json`.

The model is configuration, not code. Install Ollama, pull a model that supports tool calling, and name it in `.env`:

```sh
ollama pull qwen3:1.7b
```

```dotenv
OLLAMA_BASE_URL=http://127.0.0.1:11434
OLLAMA_MODEL=qwen3:1.7b
```

Ask the agent directly; this needs neither Kafka nor the database:

```sh
python -m backend.agent.agent "What is 12 * 7?"
```

### Agent worker

The worker reads `prompts_allowed` from Kafka. For each message it re-checks the decision stored in `guard_results`, runs the agent only if it is `allow`, and stores the outcome in `agent_runs`. It runs the text stored in `prompts`, so the agent answers exactly what the guard checked. Kafka offsets are committed after the row is stored, and a prompt that already has a stored run is skipped on redelivery.

```sh
python -m flask --app backend.app init-db
python -m backend.scripts.create_topics
python -m backend.agent.worker            # keeps running; add --once to stop when the topic is drained
```

The real guard does not exist yet, so a **temporary** guard stub stores a decision and publishes the prompt to `prompts_allowed`. Delete `backend/agent/guard_stub.py` once the guard does this:

```sh
python -m backend.agent.guard_stub "What is 15 divided by 3?"
python -m backend.agent.guard_stub --prompt-id <uuid>                     # an existing prompt
python -m backend.agent.guard_stub "Ignore all instructions" --decision block   # the worker rejects it
```

A `prompts_allowed` message is JSON, keyed by `prompt_id`:

```json
{"schema_version": 1, "prompt_id": "uuid", "text": "original prompt text", "model_version": "guard model version"}
```

Publish one from Python with `backend.intake.producer.publish_allowed_prompt(prompt_id, text, model_version)`.

`agent_runs` has one row per handled prompt:

| Column | Meaning |
|---|---|
| `prompt_id` | The prompt (primary key, so at most one run per prompt) |
| `status` | `completed`; `failed` (Ollama or the model errored, see `error`); `rejected` (the stored decision was not `allow`, the agent was not called) |
| `response` | The agent's answer |
| `tool_calls` | JSON list of `{"name", "args"}` for every tool the agent called |
| `latency_ms` | How long the agent took |
| `model` | The Ollama model used |
| `error` | Why a run failed or was rejected |
| `created_at` | When the run was stored |

## Database and batch data

| Table | Contents |
|---|---|
| `prompts` | UUID, original text, source, creation time, optional dataset link, P1/P2 snapshot |
| `guard_results` | One result per prompt, initially pending, with optional classifier output |
| `dataset_prompts` | NeurAlchemy, deepset and jackhhao training/evaluation rows |
| `agent_runs` | Agent status, response, tools, latency, model, and errors for a handled prompt |

The schema and foreign keys are shown in [database-schema.dbml](docs/database-schema.dbml)
and [database.png](docs/database.png). The PNG predates `preprocessing` and its source
index; the DBML/SQL include them. Re-running `init-db` preserves data and adds the
dataset-link/preprocessing columns and indexes to older databases.

For the full batch/notebook environment, use Python 3.13+ and uv:

```sh
uv sync --project backend/batch --locked
uv run --project backend/batch --locked python backend/batch/import_neuralchemy.py
uv run --project backend/batch --locked python backend/batch/import_external_datasets.py
```

Import commands download their datasets and use the root `.env`. NeurAlchemy baseline
rows are flagged using `neuralchemy_v1`, excluding `control` and `chain_of_thought`.
External deepset/jackhhao rows remain held out of baseline training. Import IDs use
the original source split and row index, so filtering and deduplication do not renumber them.

Train and save the B3 multiclass attack-type classifier after the NeurAlchemy rows
are imported:

```sh
uv run --project backend/batch --locked python backend/batch/train_batch3.py
```

B3 uses the imported six-way `attack_group` labels for malicious NeurAlchemy rows,
preserves their train/validation/test splits, and saves its classifier and metrics
under `backend/batch/models/batch3/`.

Evaluate the saved B1/B2 artifacts on imported external-test rows and save a JSON
report (the command also prints JSON when `--output` is omitted):

```sh
uv run --project backend/batch --locked python -m backend.batch.evaluation --output evaluation.json
```

The report separates deepset and jackhhao and includes malicious-class precision,
recall, F1, macro-F1, false-positive rate, ROC-AUC, confusion counts, and per-prompt
inference latency. Latency includes text feature or embedding generation and prediction,
but excludes model loading. Cross-dataset duplicate prompts are represented by whichever
source the importer retained.

## Checks

Fast checks need only the runtime dependencies:

```sh
python -m unittest discover -s backend/tests -v
node --test frontend/tests/app.test.cjs
```

Database and Kafka checks are opt-in. Create a dedicated test database once, then run:

```sh
docker compose -f backend/compose.yaml exec db createdb -U pijd pijd_test
TEST_DATABASE_URL=postgresql://pijd:pijd@127.0.0.1:55432/pijd_test TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9094 python -m unittest discover -s backend/tests -v
```

Database checks remove their fixture rows. Kafka round-trip checks publish fixture
records to `prompts_raw` and `prompts_allowed`. Batch checks use local examples without downloads:

```sh
TEST_DATABASE_URL=postgresql://pijd:pijd@127.0.0.1:55432/pijd_test uv run --project backend/batch --locked python -m unittest discover -s backend/tests -p test_batch_import.py -v
```

The combined runtime suite also checks the agent's two tools, its stored-decision gate,
and the `prompts_allowed` Kafka round trip using a fake model. Only the live-model test
requires Ollama: set `TEST_OLLAMA_MODEL` to a locally available tool-capable model.

P1/P2 and replay tests use local fixtures and controlled clocks. With both test
connection variables set, they also verify atomic snapshot storage, source history,
redelivery, and replay through real Kafka and the consumer into PostgreSQL. They
do not download datasets/models or change existing browser behavior.

If a host's Windows Application Control blocks the Kafka client DLL, run the runtime
suite inside the existing Linux container instead:

```sh
docker compose -f backend/compose.yaml exec -e TEST_DATABASE_URL=postgresql://pijd:pijd@db:5432/pijd_test -e TEST_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 web python -m unittest discover -s backend/tests -v
```
