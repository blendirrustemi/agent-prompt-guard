# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database/topic setup and Compose initialization. |
| `backend/intake/` | Version-two raw events, publishers, topic names, dataset replay CLI, and the intake consumer. |
| `backend/stream/` | P1/P2 preprocessing; P3 similarity, P4 Batch 2/3 scoring, P5 Half-Space Trees; P7 routing; P8 aggregation; P9/P10 throttling. |
| `backend/agent/` | LangChain/Ollama agent, two tools, allowed-prompt worker, and temporary guard stub. |
| `backend/batch/` | Dataset imports and exploratory notebooks, with separate dependencies. |
| `backend/tests/` | Route, storage, Kafka, agent, and local dataset fixture checks. |
| `docs/` | Architecture, work plan, team guides, and the database diagram (`database-schema.dbml`, open in dbdiagram.io). |

Today: **prompt box → Flask → Kafka (`prompts_raw`) → consumer (P1–P5) → PostgreSQL**.
The browser shows `Queued` after Kafka acknowledges delivery, then `Pending check` once
the consumer saves the prompt and its preprocessing snapshot. P1 derives a cleaned
copy and original-text features; P2 adds ten minutes of source decision counts,
cached for 30 seconds. P3 probes training/recent attack embeddings, and P4 saves Batch 2
probabilities and Batch 3 attack types for P7/P8; P5 stores an advisory anomaly score. Refresh starts
an empty chat while saved prompts remain in PostgreSQL. Run commands from the project root;
see [README](../README.md). Keep `.env` local and preserve `tmp/postgres-dev/` and Docker volumes.

Separately, **`prompts_allowed` → agent worker → `agent_runs`** answers prompts with a
stored `allow` decision. P7 supplies these decisions; the temporary stub is only for
isolated demos. The default Compose stack does not start the agent.

In parallel, **`prompts_raw` → burst worker (P9/P10) → `throttle_decisions` → P7 router** applies
immediate source blocks and score-based routing. P8 periodically reads stored P4 results and
persists rolling attack-type counts and repeated-prompt snapshots.

## Next steps

Add dashboards for the stored P7/P8 results as a later step:

| Folder | Responsibility |
|---|---|
| `backend/stream/` (remaining stages) | P6 drift monitoring; P4 exposes results and a durable completion timestamp for P6. |
| `backend/dashboard/` | Streamlit graphs and human review pages. |

Replay imported rows with `python -m backend.intake.replay`; select dataset/split,
rate, sources, and optional label/attack-group filters. It uses the browser's same
Kafka event and P1–P4 path. Wait for the consumer's scorer-ready log after model/reference warm-up;
the MiniLM download cache is kept in a Compose volume. See the README for configuration and truncation limits.

Extend the existing consumer with later guard stages, connect the allowed-prompt publisher, and add dashboards. Keep the
version-two event contract and shared topic names consistent. Runtime packages belong in
`backend/requirements.txt`; batch/notebook packages belong in `backend/batch/pyproject.toml`.
Update the README when run commands change.

See the [current architecture](architecture-work-plan-v2.md) and `work-plan.xlsx` for assignments.
