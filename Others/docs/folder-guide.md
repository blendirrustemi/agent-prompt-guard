# Folder guide for the team

## Current structure

| Location | What belongs here |
|---|---|
| `frontend/templates/` | HTML page layout (`index.html`). |
| `frontend/static/` | CSS, browser JavaScript, and icons. |
| `backend/` | Flask routes (`app.py`), database helpers (`db.py`), tables (`schema.sql`), dependencies, and Docker configuration. |
| `backend/scripts/` | Database/topic setup and Compose initialization. |
| `backend/intake/` | Version-two raw events, publishers, topic names, dataset replay CLI, and the intake consumer. |
| `backend/stream/` | P1/P2 preprocessing; P3 similarity, P4 Batch 2/3 scoring, P5 Half-Space Trees; P6 score drift; P7 routing; P8 aggregation; P9/P10 throttling. |
| `backend/agent/` | LangChain/Ollama agent, two tools, and allowed-prompt worker. |
| `backend/dashboard/` | Streamlit app (`app.py`) with one sidebar section per member, shared PostgreSQL access (`data.py`), and member pages in `views/`. |
| `backend/batch/models/` | Active Batch 2/3 models required by the guard. |
| `results/evaluation.json` | Saved model evaluation required by Emma’s dashboard and copied into the Docker image. |
| `backend/tests/` | Dashboard render tests and Batch 4 review-workflow checks preserved from main. |
| `backend/batch/` | Current P7 review export and Batch 4 retraining workflow, plus active runtime models. |
| `Others/Models/` | Restored batch data, notebooks, training/import/evaluation tools, dependencies, model snapshots, and historical runs. Training here writes to its own `models/` directory. |
| `Others/results/` | Archived experiment CSVs. |
| `Others/tmp/` | Old logs, screenshots, and verification scripts. Local PostgreSQL data and model caches remain in `tmp/`. |
| `Others/docs/` | Project report, developer guide, Excel work plan, research notes, and architecture/database diagrams (`database-schema.dbml` opens in dbdiagram.io). |

Today: **prompt box → Flask → Kafka (`prompts_raw`) → consumer (P1–P5) → PostgreSQL**.
The browser shows `Queued` after Kafka acknowledges delivery, then `Pending check` once
the consumer saves the prompt and its preprocessing snapshot. P1 derives a cleaned
copy and original-text features; P2 adds ten minutes of source decision counts,
cached for 30 seconds. P3 probes training/recent attack embeddings, and P4 saves Batch 2
probabilities and Batch 3 attack types for P7/P8; P5 stores an advisory anomaly score. Refresh starts
an empty chat while saved prompts remain in PostgreSQL. Run commands from the project root;
see [README](../../README.md). Keep `.env` local and preserve `tmp/postgres-dev/` and Docker volumes.

Separately, **`prompts_allowed` → agent worker → `agent_runs`** answers prompts with a
stored `allow` decision. P7 supplies these decisions.
Compose starts the agent and waits for worker readiness before serving Flask.
The browser polls through the guard/agent lifecycle and displays answers or hold/failure explanations.
`python3 -m backend.ask` provides the same guarded flow from the terminal.

In parallel, **`prompts_raw` → burst worker (P9/P10) → `throttle_decisions` → P7 router** applies
immediate source blocks and score-based routing. P8 periodically reads stored P4 results and
persists rolling attack-type counts and repeated-prompt snapshots.

## Project documentation

- [Project report](project_report.md): datasets, architecture, and testing/experiment sections for the team.
- [Developer guide](developer-guide.md): setup, configuration, API/event contracts, replay, and checks.
- [Team work plan](work-plan.xlsx): planned tasks and assigned team members. 
- [README](../../README.md): short overview and quick start.

| Folder | Responsibility |
|---|---|
| `backend/stream/` | Runtime guard stages; P6 currently logs drift alerts. Durable alert/retraining orchestration remains follow-up work. |
| `backend/dashboard/views/` | Remaining member dashboard pages and the human review page. |

Replay imported rows with `python -m backend.intake.replay`; select dataset/split,
rate, sources, and optional label/attack-group filters. It uses the browser's same
Kafka event and P1–P4 path. Wait for the consumer's scorer-ready log after model/reference warm-up;
the MiniLM download cache is kept in a Compose volume. See the developer guide for configuration and the report for token-limit handling.

The selected batch materials are restored in `Others/Models/`; the current P7 review and Batch 4 workflow remains in `backend/batch/`.
Dashboard and Batch 4 tests updated on main are retained in `backend/tests/`. Commands for the removed older tests and demo stub remain historical references.
Keep the version-two event contract and shared topic names consistent. Runtime packages belong in
`backend/requirements.txt`.
