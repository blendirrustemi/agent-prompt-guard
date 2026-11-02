# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database/topic setup and Compose initialization. |
| `backend/intake/` | Version-two events, Kafka producer, topic names, and the intake consumer. |
| `backend/batch/` | Dataset imports and exploratory notebooks, with separate dependencies. |
| `backend/tests/` | Route and database checks. |
| `docs/` | Architecture, work plan, team guides, and the database diagram (`database-schema.dbml`, open in dbdiagram.io). |

Today: **prompt box → Flask → Kafka (`prompts_raw`) → consumer → PostgreSQL**.
The browser shows `Queued` after Kafka acknowledges delivery, then `Pending check` once
the consumer saves the prompt. The consumer does not classify prompts yet. Refresh starts
an empty chat while saved prompts remain in PostgreSQL. Run commands from the project root;
see [README](../README.md). Keep `.env` local and preserve `tmp/postgres-dev/` and Docker volumes.

## Next steps

Create these folders when their implementation starts:

| Planned folder | Responsibility |
|---|---|
| `backend/stream/` | Guard stages: features, classification, decisions, repetition, metrics, and drift. |
| `backend/agent/` | LangChain agent and tools; execute only prompts with a stored `allow` decision. |
| `backend/dashboard/` | Streamlit graphs and human review pages. |

Extend the existing consumer with guard stages, then add the agent and dashboards. Keep the
version-two event contract and shared topic names consistent. Runtime packages belong in
`backend/requirements.txt`; batch/notebook packages belong in `backend/batch/pyproject.toml`.
Update the README when run commands change.

See the [current architecture](architecture-work-plan-v2.md) and `work-plan.xlsx` for assignments.
