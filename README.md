# PIJD: Prompt Guard

PIJD protects a LangChain agent from prompt-injection and jailbreak attempts.
It checks incoming prompts, assigns maliciousness scores and attack types,
and routes each prompt to **allow**, **block**, or **review** before agent execution.

## Repository layout

`backend/` and `frontend/` contain the application and its active Batch 2/3 models.
`Others/` contains documentation, experiment results, and old temporary files.
The archived backend tools, tests, notebooks, baseline models, and training history were removed.
Supporting files retain their original folder structure inside `Others/`.
Local environments, configuration, PostgreSQL data, and model caches stay in place.
`Others/` is excluded from Docker builds.

## Documentation

- **[Project report](Others/docs/project_report.md)** - introduction, datasets, system architecture, and testing/experiment sections.
- **[Developer guide](Others/docs/developer-guide.md)** - detailed setup, APIs, event contracts, configuration, dataset replay, and troubleshooting; includes historical instructions for removed backend tools.
- **[Folder guide](Others/docs/folder-guide.md)** - explains what each folder contains.
- **[Research-paper notes](Others/docs/research-papers.md)** - research papers used in this project and referenced in the project report.
- **[Team work plan](Others/docs/work-plan.xlsx)** - planned tasks and assigned team members.

## Architecture - simple overview

```text
Browser or HTTP CLI -> Flask -> Kafka prompts_raw
                                  |
                                  +-> Intake: P1-P5 -> stored scores --+
                                  +-> Burst: P9/P10 -> throttling ----+-> P7
                                                                        |
                                                       allow / block / review
                                                         |
                                         allowed prompts -> agent -> stored answer

P6 monitors stored P4 scores; P8 produces rolling attack summaries.
PostgreSQL stores prompts, scores, decisions, and agent results.
```

P3 similarity and P5 anomaly scores are advisory. P4 uses Batch 2 for binary scoring
and Batch 3 for attack typing; P7 combines the binary score with P10 throttling.
The browser shows processing progress, guard verdicts, and allowed-prompt answers.

## Prerequisites

- Docker Desktop or Docker Engine with Compose.
- [Ollama](https://ollama.com) running on the host with a tool-capable model.
- Ports **5050**, **55432**, and **9094** available.
- Root `.env` copied from [.env.example](.env.example), with the Ollama model selected.
- Python for optional local CLI/development work.

Run commands below from the **project root**.
Containers use internal database/Kafka addresses; local Python uses the root `.env`.

## Start the complete local stack

Pull the default agent model, or select an already installed tool-capable model:

```sh
ollama pull qwen3:1.7b
```

Set the relevant values in `.env`:

```dotenv
OLLAMA_MODEL=qwen3:1.7b
OLLAMA_DOCKER_BASE_URL=http://host.docker.internal:11434
```

With Docker and Ollama running:

```sh
docker compose --env-file .env -f backend/compose.yaml up --build -d --wait
```

Open **<http://127.0.0.1:5050>**.
Compose starts Flask, Kafka, PostgreSQL, the guard workers, and the agent.
Its initialization service creates the schema and topics while preserving existing data.

The first build/start can take longer: the image includes CPU inference libraries,
and the consumer downloads MiniLM to the persistent `model_cache` volume,
embeds available malicious training references, and warms its models.
Health checks wait for scorer/P5 readiness and Kafka group assignment; the agent checks its Ollama model.
Flask starts after its required runtime workers are healthy.
Missing or incompatible model artifacts stop the consumer with an error.

For service logs, port conflicts, or standalone PostgreSQL, see
[startup details](Others/docs/developer-guide.md#start-the-complete-local-stack).

## Use the guard

Submit a prompt in the browser and wait for its verdict or agent answer.
Prompts must be nonblank, contain no null characters, and have at most **8,000 characters**.
Prompts exceeding MiniLM's **256-token** window are held for review instead of being classified from a truncated prefix.
Backlogs and missing scores can also cause review; held prompts do not reach the agent.

For a terminal client, with local Python available:

```sh
python3 -m backend.ask "Which datasets are used in this university project?"
```

This uses the same guarded HTTP path as the browser and prints the answer or hold outcome.
See [CLI and API details](Others/docs/developer-guide.md#command-line-prompt-to-answer)
for JSON output, resume/retry behaviour, and execution states.

## Stop without deleting data

```sh
docker compose --env-file .env -f backend/compose.yaml stop
```

Restart with the same startup command. PostgreSQL records, Kafka data, and the model cache
remain in named Docker volumes. **Do not use `down -v` if you want to keep those volumes.**
The Compose database does not import a standalone database from `tmp/postgres-dev/`.

## Development and experiments

- [Local Python setup](Others/docs/developer-guide.md#local-python-and-command-line-publishing)
- [Dataset replay through the real guard](Others/docs/developer-guide.md#dataset-replay)
