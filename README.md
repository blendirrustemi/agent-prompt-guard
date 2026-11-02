# PIJD: Prompt Guard

Current milestone: **browser → Flask → Kafka → intake consumer → PostgreSQL**.
The browser shows **Queued** after Kafka accepts a prompt, then **Pending check**
after the consumer saves it. Classification and agent execution are future work.

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

Refresh clears the visible chat without deleting records. There is no classification,
decision polling after storage, agent invocation, account system, or separate conversation storage.

## Kafka events and the consumer

| Topic | Current use |
|---|---|
| `prompts_raw` | Raw inputs consumed by the intake worker |
| `prompts_allowed` | Reserved for guard-approved prompts and the future agent |
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

## Database and batch data

| Table | Contents |
|---|---|
| `prompts` | UUID, original text, source, creation time, optional dataset link |
| `guard_results` | One result per prompt, initially pending, with optional classifier output |
| `dataset_prompts` | NeurAlchemy, deepset and jackhhao training/evaluation rows |

The schema and foreign keys are shown in [database-schema.dbml](docs/database-schema.dbml)
and [database.png](docs/database.png). Re-running `init-db` preserves data and
adds the existing dataset-link column to older databases.

Batch dependencies are separate from the intake image. With Python 3.13+ and uv:

```sh
uv sync --project backend/batch --locked
uv run --project backend/batch --locked python backend/batch/import_neuralchemy.py
uv run --project backend/batch --locked python backend/batch/import_external_datasets.py
```

Import commands download their datasets and use the root `.env`. NeurAlchemy baseline
rows are flagged using `neuralchemy_v1`, excluding `control` and `chain_of_thought`.
External deepset/jackhhao rows remain held out of baseline training. Import IDs use
the original source split and row index, so filtering and deduplication do not renumber them.

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

Database checks remove their fixture rows. The Kafka round-trip check publishes one
record to `prompts_raw`. Batch fixture checks use small local examples without downloads:

```sh
TEST_DATABASE_URL=postgresql://pijd:pijd@127.0.0.1:55432/pijd_test uv run --project backend/batch --locked python -m unittest discover -s backend/tests -p test_batch_import.py -v
```
