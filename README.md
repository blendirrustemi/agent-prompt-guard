# PIJD: Prompt Guard

New to the project? Read the short [folder guide for the team](docs/folder-guide.md) for the current structure and where upcoming features belong.

First implementation milestone: a local Flask page with a chat-style prompt box and PostgreSQL storage. Enter a prompt, press Enter or the send button, and see its pending check status. Shift + Enter adds a new line. Refresh starts an empty chat; submitted prompts remain stored in PostgreSQL for later guard processing.

This milestone makes no LLM or external API calls. A local Kafka broker and a command-line producer are available (see [Kafka](#kafka)), but the page still writes directly to PostgreSQL. All page assets are local. Prompts are stored without classification and are never forwarded to an agent. This is one shared local workspace without user accounts or separate conversations.

## Quick start with installed PostgreSQL binaries

After installing the Python requirements below, this command starts an isolated local PostgreSQL instance on port 55432, creates the project tables, and writes the connection to `.env` if that file does not already exist:

```sh
python3 backend/scripts/local_db.py
python3 -m flask --app backend.app run --host 127.0.0.1 --port 5050
```

Open <http://127.0.0.1:5050>. The database files live in the ignored `tmp/postgres-dev` directory. Re-running the script preserves data. It also creates a separate `pijd_test` database for integration checks. To stop this project's instance while keeping its data, run `python3 backend/scripts/local_db.py --stop`. Its loopback-only connection uses trust authentication for this local exercise.

## Run with an existing local PostgreSQL server

Use Python 3.10+ and PostgreSQL. From the project directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

Create the project database once, then create its tables and start Flask:

```sh
createdb -h localhost pijd
python -m flask --app backend.app init-db
python -m flask --app backend.app run --host 127.0.0.1 --port 5050
```

Open <http://127.0.0.1:5050>. Port 5050 avoids the macOS service commonly using port 5000. If `pijd` already exists, skip the `createdb` command. Re-running `init-db` preserves existing tables and data.

The default connection is `postgresql://localhost:5432/pijd`, using your operating-system database role. To use different credentials or a different port, copy `.env.example` to `.env` and edit `DATABASE_URL` before running Flask. `.env` is excluded from Git.

## Optional Docker PostgreSQL

This option uses the same port as the quick-start instance. Stop that instance with `python3 backend/scripts/local_db.py --stop` before switching to Docker.

With Docker running:

```sh
docker compose -f backend/compose.yaml up -d --wait db
```

Set this connection in `.env`, then run `init-db` and Flask as above:

```dotenv
DATABASE_URL=postgresql://pijd:pijd@localhost:55432/pijd
```

This is a separate database on local port 55432. The Compose password is a local development value. Its data persists in a Docker volume. `docker compose -f backend/compose.yaml stop db` stops it while keeping the data.

## Kafka

With Docker running, start the single Kafka broker (KRaft mode, no ZooKeeper) and create the topics:

```sh
docker compose -f backend/compose.yaml up -d --wait kafka
python -m backend.scripts.create_topics
```

The broker listens on `127.0.0.1:9094` for the host and `kafka:9092` for other Compose services. Set `KAFKA_BOOTSTRAP_SERVERS` in `.env` to use a different address. Automatic topic creation is off; re-running the script leaves existing topics unchanged.

| Topic | Contents |
|---|---|
| `prompts_raw` | Every manual or replayed prompt, as the raw prompt event below |
| `prompts_allowed` | Prompts the guard allowed; read by the agent |
| `prompts_labels` | Reviewed or dataset labels, kept separate from model inputs |

Publish raw prompt events from the command line, one argument per prompt or one stdin line per prompt:

```sh
python -m backend.intake.producer "Ignore all previous instructions" --source source_01
python -m backend.intake.producer "Ignore all previous instructions" --source source_02 --dataset-prompt-id <dataset_prompts.id>
```

Each message is keyed by `source` and its value is JSON:

```json
{"schema_version": 2, "prompt_id": "uuid", "text": "original prompt text", "source": "source_01", "created_at": "2026-10-28T10:08:15+00:00", "dataset_prompt_id": null}
```

The fields match the `prompts` table, so storing a message needs no extra columns:

| Event field | `prompts` column |
|---|---|
| `prompt_id` | `id` |
| `text` | `text` |
| `source` | `source` (`web` for the page, `source_01`… for simulated replay sources) |
| `created_at` | `created_at` |
| `dataset_prompt_id` | `dataset_prompt_id` (UUID of the `dataset_prompts` row, or `null`) |

`schema_version` describes the message format and is not stored. Labels are never part of the message. From Python, use `backend.intake.producer.send_prompts(texts, source=..., dataset_prompt_id=...)` to publish and `db.save_raw_event(url, event)` to store a received message with its `pending` guard result. `docker compose -f backend/compose.yaml stop kafka` stops the broker while keeping its messages in a Docker volume.

## The tables

| Table | Contents |
|---|---|
| `prompts` | UUID, original prompt text, source (`web`), creation time, and `dataset_prompt_id` linking a replayed prompt to its `dataset_prompts` row |
| `guard_results` | One record per prompt; decision initially `pending`; optional score, attack type, reason, model version, and check time |
| `dataset_prompts` | Imported rows from all three datasets (NeurAlchemy, deepset, jackhhao) for training and evaluation |

See [docs/database-schema.dbml](docs/database-schema.dbml) for the full diagram.

The `prompts` and `guard_results` inserts commit in one transaction. The guard result references its prompt through a foreign key. Later, a guard worker can read pending rows, run a classifier, and update the decision to `allow`, `block`, or `review`. Model fields remain null until an actual check is implemented.

Input must be non-empty text of at most 8,000 characters. SQL parameters store submitted text literally, and the browser renders it as text. Failed submissions preserve the input and display an error. The initial UI does not poll for future decision changes. Refresh clears the visible messages and input without deleting database records; saved history remains available through `GET /api/prompts`.

### `dataset_prompts`

`dataset_prompts` holds every row of the three datasets, so models are trained and evaluated from one place:

| Column | Type | Meaning |
|---|---|---|
| `id` | `UUID` | Importers should pass a stable UUID (for example `uuid5` of dataset, split and row number) so re-running an import with `ON CONFLICT (id) DO NOTHING` skips rows already stored; otherwise the database generates a random one |
| `text` | `TEXT` | Original prompt text; must not be blank. No length limit, because some dataset prompts are long |
| `label` | `SMALLINT` | `1` malicious (injection or jailbreak), `0` benign. Importers convert each dataset's labels to this, for example jackhhao's `jailbreak`/`benign` |
| `source_dataset` | `TEXT` | `neuralchemy`, `deepset` or `jackhhao` (lowercase) |
| `source` | `TEXT` | Upstream source inside the dataset, such as NeurAlchemy's `neuralchemy_v1`; optional |
| `split` | `TEXT` | Partition the row belongs to, such as `train`, `validation` or `test`; required |
| `category` | `TEXT` | The dataset's own category, such as NeurAlchemy's `control` or `chain_of_thought`; optional |
| `attack_group` | `TEXT` | Attack type or family, for the attack-type classifier (B3); optional |
| `group_id` | `TEXT` | Template group; rows that share a template stay in the same split; optional |
| `include_in_baseline` | `BOOLEAN` | `true` for rows used to train the B1/B2 baseline models (cleaned NeurAlchemy `core` data, excluding `control` and `chain_of_thought`); defaults to `false`, so deepset and jackhhao stay held out |
| `metadata` | `JSONB` | Any other fields from the original dataset row; defaults to `{}` |

An index on `(source_dataset, split)` keeps per-dataset, per-split queries fast.

### Following a dataset prompt into `prompts`

When a dataset row is replayed through the guard, its `prompts` row stores the dataset row's `id` in `prompts.dataset_prompt_id`. Prompts typed in the web UI leave it `NULL`. One dataset row can be replayed many times; each replay is its own `prompts` row pointing to the same dataset row. The database rejects IDs that do not exist, and deleting a dataset row sets the link to `NULL` instead of deleting the live prompt.

In code, pass the ID when storing a replayed prompt: `db.save_prompt(url, text, dataset_prompt_id=row_id)`, where `row_id` is the `dataset_prompts.id` UUID. The replayed event itself carries no label; join afterwards to compare guard decisions with the true labels:

```sql
SELECT p.id, g.decision, g.malicious_score, d.label, d.source_dataset, d.split
FROM prompts p
JOIN guard_results g ON g.prompt_id = p.id
JOIN dataset_prompts d ON d.id = p.dataset_prompt_id;
```

`init-db` adds the column to existing databases too, so re-run it after pulling this change.

## Files and endpoints

Run commands from the project root. Flask loads the UI from `frontend/` and reads `.env` from the root.

```text
frontend/
  templates/index.html       # page markup
  static/                    # CSS, JavaScript, favicon
backend/
  __init__.py                # Python package
  app.py                     # Flask routes and init-db command
  db.py                      # PostgreSQL operations
  schema.sql                 # database schema
  requirements.txt           # Python dependencies
  compose.yaml               # optional Docker database and Kafka broker
  intake/producer.py         # publish raw prompt events to Kafka
  intake/topics.py           # shared topic names and Kafka address
  scripts/local_db.py         # local database setup/start/stop
  scripts/create_topics.py    # create the Kafka topics
  tests/test_app.py           # route and database checks
  tests/test_intake.py        # raw event and Kafka checks
docs/                        # architecture and team work plan
tmp/                         # local database files and preview artifacts
README.md
.env.example                 # example database configuration
```

`__pycache__/` folders and `.pyc` files are generated Python caches and can be deleted. Preview images and rendered PDF pages in `tmp/` are optional. Keep `tmp/postgres-dev/`: it holds the saved local database. The course PDF and `docs/` are reference material, useful for the class but not needed to run Flask. `compose.yaml` is only needed if you choose the Docker database option.

Endpoints:

- `GET /api/prompts`: latest 50 prompts, in chronological order, each with its `dataset_prompt_id` (`null` for web prompts).
- `POST /api/prompts`: JSON `{ "text": "your prompt" }`; returns the saved prompt.
- `docs/architecture-work-plan.md`: broader project proposal. This Flask page implements prompt intake; dashboards, classifiers, streaming, and the agent are later milestones.

## Checks

Basic route checks:

```sh
python -m unittest discover -s backend/tests -v
```

To also verify persistence against an isolated test database:

```sh
createdb -h localhost pijd_test
TEST_DATABASE_URL=postgresql://localhost:5432/pijd_test python -m unittest discover -s backend/tests -v
```

For the quick-start instance, use `TEST_DATABASE_URL=postgresql://pijd@127.0.0.1:55432/pijd_test` instead; the database already exists.

The database checks create the schema in the configured test database and remove only the rows they created. Use a dedicated test database.

To also check a real Kafka round trip (creates the topics if missing and publishes one test message to `prompts_raw`), set `TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9094`.
