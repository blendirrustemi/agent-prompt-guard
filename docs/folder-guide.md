# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database/topic setup and Compose initialization. |
| `backend/intake/` | Version-two raw events, publishers, topic names, dataset replay CLI, and the intake consumer. |
| `backend/stream/` | P1/P2 preprocessing; P7 score routing; P8 rolling attack aggregation; P9/P10 burst detection and throttling workers. |
| `backend/agent/` | LangChain/Ollama agent, two tools, allowed-prompt worker, and temporary guard stub. |
| `backend/batch/` | Dataset imports and exploratory notebooks, with separate dependencies. |
| `backend/tests/` | Route, storage, Kafka, agent, and local dataset fixture checks. |
| `docs/` | Architecture, work plan, team guides, and the database diagram (`database-schema.dbml`, open in dbdiagram.io). |

Today: **prompt box → Flask → Kafka (`prompts_raw`) → consumer (P1/P2) → PostgreSQL**.
The browser shows `Queued` after Kafka acknowledges delivery, then `Pending check` once
the consumer saves the prompt and its preprocessing snapshot. P1 derives a cleaned
copy and original-text features; P2 adds ten minutes of source decision counts,
cached for 30 seconds. The consumer does not classify prompts yet. Refresh starts
an empty chat while saved prompts remain in PostgreSQL. Run commands from the project root;
see [README](../README.md). Keep `.env` local and preserve `tmp/postgres-dev/` and Docker volumes.

Separately, **`prompts_allowed` → agent worker → `agent_runs`** answers prompts with a
stored `allow` decision. The temporary guard stub supplies these decisions until the
real guard exists; the default Compose intake stack does not start the agent.

In parallel, **`prompts_raw` → burst worker (P9/P10) → `throttle_decisions` → P7 router** applies
immediate source blocks and score-based routing. P8 periodically reads stored P4 results and
persists rolling attack-type counts and repeated-prompt snapshots.

## Next steps

Add dashboards for the stored P7/P8 results as a later step:

| Folder | Responsibility |
|---|---|
| `backend/stream/` (remaining stages) | P4 scoring integration, other guard stages, metrics, and drift. |
| `backend/dashboard/` | Streamlit graphs and human review pages. |

Replay imported rows with `python -m backend.intake.replay`; select dataset/split,
rate, sources, and optional label/attack-group filters. It uses the browser's same
Kafka event and P1/P2 path. See the README for burst/drift traffic examples.

Extend the existing consumer with later guard stages, connect the allowed-prompt publisher, and add dashboards. Keep the
version-two event contract and shared topic names consistent. Runtime packages belong in
`backend/requirements.txt`; batch/notebook packages belong in `backend/batch/pyproject.toml`.
Update the README when run commands change.

See the [current architecture](architecture-work-plan-v2.md) and `work-plan.xlsx` for assignments.
