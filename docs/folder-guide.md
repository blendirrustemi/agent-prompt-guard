# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database setup and other runnable utilities. |
| `backend/tests/` | Route and database checks. |
| `docs/` | Architecture, work plan, and team guides. |

Today: **prompt box → Flask → PostgreSQL**, with each prompt marked `pending`. Refresh starts an empty chat while submitted prompts stay in the database. Run commands from the project root; see [README](../README.md). Keep `.env` local and preserve `tmp/postgres-dev/`, which holds the saved database.

## Next steps

Create these folders when their implementation starts:

| Planned folder | Responsibility |
|---|---|
| `backend/intake/` | Publish manual and replayed prompts to Kafka. |
| `backend/stream/` | Guard stages: features, classification, decisions, repetition, metrics, and drift. |
| `backend/agent/` | LangChain agent and tools; execute only prompts with a stored `allow` decision. |
| `backend/batch/` | Dataset import, model training, evaluation, and retraining. |
| `backend/dashboard/` | Streamlit graphs and human review pages. |

First agree on event fields and database changes. Then connect intake, guard, and storage; add the agent and dashboards afterward. Keep Flask routes small and put processing logic in the relevant modules. Add new Python packages to `backend/requirements.txt` and update the README when run commands change.

See [architecture and work plan](architecture-work-plan.md) for member assignments.
