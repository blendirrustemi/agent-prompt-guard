# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database setup, Kafka topic creation, and other runnable utilities. |
| `backend/intake/` | Publish manual and replayed prompts (`prompts_raw`) and allowed prompts (`prompts_allowed`) to Kafka. |
| `backend/agent/` | LangChain agent on Ollama, its two tools, and the worker that runs only prompts with a stored `allow` decision. `guard_stub.py` is temporary. |
| `backend/batch/` | Dataset exploration notebooks and import scripts; later model training, evaluation, and retraining. |
| `backend/tests/` | Route, database, Kafka, and agent checks. |
| `docs/` | Architecture, work plan, team guides, and the database diagram (`database-schema.dbml`, open in dbdiagram.io). |

Today: **prompt box → Flask → PostgreSQL**, with each prompt marked `pending`. Separately, **`prompts_allowed` → agent worker → `agent_runs`** answers prompts that have a stored `allow` decision; until the guard exists, the temporary guard stub supplies them. Refresh starts an empty chat while submitted prompts stay in the database. Run commands from the project root; see [README](../README.md). Keep `.env` local and preserve `tmp/postgres-dev/`, which holds the saved database.

## Next steps

Create these folders when their implementation starts:

| Planned folder | Responsibility |
|---|---|
| `backend/stream/` | Guard stages: features, classification, decisions, repetition, metrics, and drift. The guard replaces `backend/agent/guard_stub.py` by storing decisions and publishing allowed prompts to `prompts_allowed`. |
| `backend/dashboard/` | Streamlit graphs and human review pages. |

First agree on event fields and database changes. Then connect intake, guard, and storage; add the agent and dashboards afterward. Keep Flask routes small and put processing logic in the relevant modules. Add new Python packages to `backend/requirements.txt` and update the README when run commands change.

See [architecture and work plan](architecture-work-plan.md) for member assignments.
