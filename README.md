# PIJD: Prompt Guard

Current milestone: **browser → Flask → Kafka → intake consumer → PostgreSQL**.
The browser shows **Queued** after Kafka accepts a prompt, then **Pending check**
after the consumer saves it. Classification is future work. A separate agent worker
and temporary guard stub are available for the allowed-prompt demo (see [Agent](#agent)).

See the [folder guide](docs/folder-guide.md), [current architecture](docs/architecture-work-plan-v2.md),
and [database sketch](docs/database-schema.dbml).

## Start the complete local stack

With Docker running, from the project root:

```sh
docker compose -f backend/compose.yaml up --build -d --wait
```

Open <http://127.0.0.1:5050>. This starts Flask, one intake consumer, Kafka 3.9.1
in KRaft mode, and PostgreSQL 16. A one-shot initialization service creates the
tables and three topics before Flask and the consumer start. Re-running startup
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
| `prompts_allowed` | Allowed-prompt publisher and optional agent worker; supplied by the temporary guard stub |
| `prompts_labels` | Reserved for reviewed/evaluation labels |

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
guard result in one PostgreSQL transaction, then commits that record's Kafka offset.
Delivery is at least once, with database deduplication by `prompt_id`:
matching duplicates preserve the original timestamp and any completed guard decision.
Changed text/source/dataset metadata under an existing ID is logged and skipped.

Malformed records and database constraint violations are logged with their
topic/partition/offset, then skipped. They remain in the raw topic until Kafka retention
expires (the broker default is seven days). Transient storage or offset-commit
failures stop the consumer without advancing further; Compose restarts it.
This worker is the entry point for later guard stages.

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

## Agent

The optional agent runs separately from the default four-service intake stack.

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
| `prompts` | UUID, original text, source, creation time, optional dataset link |
| `guard_results` | One result per prompt, initially pending, with optional classifier output |
| `dataset_prompts` | NeurAlchemy, deepset and jackhhao training/evaluation rows |
| `agent_runs` | Agent status, response, tools, latency, model, and errors for a handled prompt |

The schema and foreign keys are shown in [database-schema.dbml](docs/database-schema.dbml)
and [database.png](docs/database.png). Re-running `init-db` preserves data and
adds the existing dataset-link column to older databases.

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

If a host's Windows Application Control blocks the Kafka client DLL, run the runtime
suite inside the existing Linux container instead:

```sh
docker compose -f backend/compose.yaml exec -e TEST_DATABASE_URL=postgresql://pijd:pijd@db:5432/pijd_test -e TEST_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 web python -m unittest discover -s backend/tests -v
```
