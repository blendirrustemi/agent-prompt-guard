# Project report: PIJD Prompt Guard

[README](../README.md) | [Developer guide](developer-guide.md) | [Research-paper notes](research-papers.md)

## Abstract

## Introduction

The focus of this project is to build a prompt-injection and jailbreak guard for an AI agent: incoming prompts are checked and routed to allow, block, or review before agent execution.

The implemented runtime is **HTTP input → Kafka → guard checks → P7 routing → allowed prompts → agent → stored answer**, exposed through the HTTP API and CLI.
Dataset replay feeds the same Kafka/consumer path.
The browser shows **Queued**, **Checking**, and then the guard verdict. P4 scores with Batch 2,
and P7 combines that score with P10 throttling. Allowed prompts reach the Compose agent worker
automatically; the browser waits for and displays its answer. Block/review outcomes explain why
the prompt was held. 

## Datasets utilised

We used three datasets from Hugging Face. NeurAlchemy was our main dataset for training, validation, and testing. Two smaller datasets, deepset and jackhhao, were kept separate from training and used to check how well the models generalised to prompts from other sources.

### NeurAlchemy

The [NeurAlchemy Prompt Injection Dataset](https://huggingface.co/datasets/neuralchemy/Prompt-injection-dataset) contains benign prompts as well as prompt-injection and jailbreak examples which were used to train the batch 1 and 2 binary classification models. The dataset also includes attack-category labels, which were used for batch 3's multiclass attack classifier. 
We kept the dataset's original training, validation, and test splits. Initially, we planned to use only rows marked as neuralchemy_v1 in the source column. However, we later expanded this to include all sources, giving the models a wider variety of prompts to learn from.
For Batch 3, we grouped the original attack categories into six broader families: instruction manipulation, jailbreak/persona manipulation, obfuscation, information extraction, indirect injection/agent manipulation, and adversarial attacks. This reduced the number of separate labels and brought related attack techniques together. 

### External evaluation datasets

The deepset/prompt-injections and jackhhao/jailbreak-classification datasets are imported into PostgreSQL for external testing.
They remain excluded from model training and selection, and the evaluation report presents their results separately.
Cross-dataset duplicate prompts are represented by whichever source the importer retained.

## System architecture

### Overall flow

Docker Compose runs Flask, Kafka, PostgreSQL, the guard workers, and the agent worker.
Browser and command-line HTTP submissions enter Kafka's `prompts_raw` topic; dataset replay feeds the same raw-event path.
The intake worker performs P1–P5, while P9/P10 process raw traffic in parallel and P6 monitors stored P4 scores.
P7 combines stored classification and throttling outcomes; only allowed prompts enter `prompts_allowed` for the agent.
P8 reads stored classifications to maintain rolling attack summaries.
These stages branch rather than forming a single sequential P1–P10 chain.

The [architecture diagram](architecture-diagram.png) illustrates the wider target, including dashboards and the review/retraining loop.
Dashboards and human-label collection are not yet part of the implemented runtime described here.
The [team work plan](work-plan.xlsx) shows the planned tasks and the assigned team members. 

### Batch models

#### Batch 1: classical binary classification

Batch 1 uses TF-IDF features and Logistic Regression to distinguish benign and malicious prompts.
Saved B1 and B2 artifacts are compared on the same imported external-test rows.

#### Batch 2: embedding-based binary classification

Batch 2 uses the pretrained all-MiniLM-L6-v2 encoder to convert the prompts into numerical features. This is the main difference compared to batch 1, which uses TF-IDF. The MiniLM encoder outputs dense, 384-dimensional embeddings designed to place semantically similar text close together. 
Each prompt is converted into a normalised embedding, which becomes the input to a Logistic Regression classifier. We trained the classifier using the benign/malicious labels, with C=1.0, the lbfgs optimiser, and a maximum of 1000 iterations. Its output is a probability between zero and one representing how likely the prompt is to be malicious. We retained the encoder’s default limit of 256 tokens. The batch 2 model achieved a macro-F1 of 0.94 on the NeurAlchemy test split. It also generalised better than Batch 1 to the external datasets, achieving macro-F1 scores of 0.66 on deepset and 0.62 on jackhhao. We therefore integrated it into P4 to supply the maliciousness score used by P7 for final routing. External performance remained a bit limited: its false-positive rates were approximately 36% and 63%, respectively.

#### Batch 3: attack-type classification

Batch 3 identifies which of the six attack families a malicious prompt belongs to. We used the same frozen encoder and normalised 384-dimensional embeddings as Batch 2, but trained the classifier only on malicious NeurAlchemy prompts mapped to those families. The classifier uses multinomial Logistic Regression, with a softmax function producing probabilities across the six classes. The predicted attack family is the class with the highest probability. We used balanced class weights to give greater weight to less frequent families.
Batch 3 achieved a macro-F1 of 0.76 on the NeurAlchemy test split. During streaming inference, it reuses Batch 2’s embedding and supplies an attack-family label when the binary maliciousness score is at least 0.5. This label describes the suspected attack; it does not determine whether the prompt is allowed, blocked, or reviewed.

#### Batch 4: reviewed-label retraining

Batch 4 is an explicit offline command using reviewed benign/malicious labels supplied as a CSV, with rows assigned to training or validation.
Both splits must contain both labels; validation rows are never used to fit the candidate.
The repository does not currently collect these labels in a review UI.
Promotion replaces the active Batch 2 classifier only if the candidate's validation macro-F1 is strictly higher than the current classifier's score on the same labelled validation rows.
The encoder and B3 classifier remain unchanged; P6 alerts request human consideration rather than automatically running or promoting B4.
Operational instructions are in the [developer guide](developer-guide.md#batch-4-reviewed-label-retraining).

### P1/P2: ingestion, preprocessing and source enrichment

P1 creates a separate `cleaned_text` copy using Unicode NFC normalization and
collapsed/trimmed whitespace. Case and punctuation remain intact. Its features
come from the **original** text: `char_count`, whitespace-separated `word_count`,
and `symbol_ratio` (non-alphanumeric, non-whitespace characters / total characters).
The original text remains available for model inference and agent execution.

P2 joins the exact `source` identifier to stored guard decisions checked within the
last ten minutes. `source_context` contains `allow_count`, `block_count`,
`review_count`, and an ISO UTC `as_of` timestamp. Pending decisions and dataset
labels are excluded. Zero counts mean no recent evaluated history, not a benign
verdict. P2 provides context; it does not classify or throttle prompts.

Each consumer process caches source context for 30 seconds; updates can therefore
take up to 30 seconds to appear. Expired entries are removed on lookups, and the
cache starts empty after a restart. Counts come from PostgreSQL, so duplicate
Kafka delivery does not increment them. Final P7 decisions supply this history.

The processing dictionary is stored in the nullable `prompts.preprocessing` JSONB
column. A matching redelivery preserves its first snapshot and any guard decision.
Older rows with no snapshot receive one only if their event is delivered again;
startup does not reset offsets or backfill old prompts. Re-run the normal Compose
startup (or `init-db`) to add the column and source index without replacing data.
HTTP responses and raw Kafka events keep their existing shapes.

### P3/P4: similarity and classification

The intake worker passes original text through one frozen `all-MiniLM-L6-v2` encoder and reuses
the normalized 384-dimensional embedding for P3, Batch 2 binary inference, and Batch 3 attack typing.
The original text is preserved. P4 counts tokens before encoding; inputs over the 256-token model
window are held for review with `model_input_too_long` and are not classified from a partial prefix.
The original prompt remains stored, but cannot reach the agent. Short-input inference retains batch
parity. Token coverage and per-stage timings are saved in `guard_results.scoring_metadata`.

P3 adapts the semantic intent/cosine comparison in
[PromptGuard](https://www.nature.com/articles/s41598-025-31086-y); this is not the paper's complete
four-layer framework. Its static bank uses only malicious NeurAlchemy `train` rows (all sources,
no `include_in_baseline` filter), deduplicated by text. A bounded recent bank is our extension:
after persistence, predictions >= 0.90 are remembered for 300 seconds, up to 1,000 unique prompt IDs.
Restarting clears recent state. An empty static bank logs a warning and still permits inference.
No validation/test labels or live dataset labels enter the comparison or classifiers.

P4 saves Batch 2's unchanged class-1 probability; P3 similarity never raises that score.
Batch 3 supplies one of six attack types only when probability is at least 0.5; otherwise the type is null.

### P5 streaming anomaly detection

P5 runs in the intake consumer after P4 and uses River Half-Space Trees over seven bounded numeric
features: normalized P1 character and word counts, symbol ratio, normalized P3 cosine similarity,
P3 availability, P4 malicious probability, and P4 availability. Counts are divided by the 8,000
character input limit; cosine similarity is mapped from `[-1, 1]` to `[0, 1]`. Missing P3/P4
signals use zero plus an availability flag. HST receives fixed `[0, 1]` feature limits.

The detector uses 10 trees, height 8, window size 250, and seed 42. It scores before learning from
each prompt. The first 250 observations have `p5_status = 'warming_up'` and no anomaly score; later
rows have `p5_status = 'scored'`. A higher HST score indicates more unusual behavior, not a malicious
verdict. P5 is informational and does not affect P7 routing.

### P6 score drift monitoring

The separate `drift` worker watches successfully stored P4 malicious probabilities in
`guard_results`. It polls scores in completion order and feeds them to River ADWIN
(default `P6_ADWIN_DELTA=0.002`). A detected distribution change emits a
`P6 RETRAINING ALERT` warning in the worker logs. This is an alert for a human to
review labelled feedback and consider Batch 4; P6 does not retrain or replace a model.

P6 monitors score distribution, not classification correctness or prompt content.

### P7/P8: decision routing and attack aggregation

Compose runs two workers alongside intake and P9/P10:

- **P7 router** consumes `throttle_decisions`. P10 `block` immediately finalizes the prompt as
  `block`. P10 `allow` joins the stored P4 `malicious_score`; prompts with no score remain pending
  while later throttle events continue. P7 allows scores at or below 0.30, blocks scores at or
  above 0.70, and sends the middle band to `review`. A missing score is sent to review after
  30 seconds from the P10 decision. Failed inference and over-token inputs enter review as soon as
  P10 permits routing. Missing P10 output enters review after 30 seconds from initial database storage,
  using server-side `stored_at` rather than replay timestamps. Legacy pending scores with unknown
  token coverage also enter review. Only allowed prompts enter `prompts_allowed`.
- **P8 aggregation** refreshes a 60-second event-time window every 10 seconds. It counts P4's six
  attack types and records up to ten repeated normalized prompts per type (at least two occurrences).
  The current snapshot is in `attack_aggregate_current`; timestamped snapshots are in
  `attack_aggregate_history` and are retained for seven days.

P4 must save score/type output with `db.save_guard_score(...)` while leaving `decision` as
`pending`; P7 owns the final allow/block/review decision. The P7 allowed-prompt transactional
outbox retries Kafka publishing after a restart. P8 uses P1's NFC/whitespace-normalized
`preprocessing.cleaned_text`, falling back to the same normalization of the original text when
no preprocessing snapshot exists. It uses predicted attack types, not dataset labels.

### P9/P10: burst detection and rate limiting

The `burst` service (`python -m backend.stream.burst_worker`, group `pijd-burst-v1`) reads
`prompts_raw` beside the intake consumer:

- **P9** counts every prompt per `source` in a sliding window of `BURST_WINDOW_SECONDS` (default 60),
  using the event's `created_at`, so replays produce the same bursts.
- **P10** blocks a source once its window reaches `BURST_THRESHOLD` prompts (default 10). The block
  lasts `BURST_COOLDOWN_SECONDS` (default 300) and is not extended by traffic during it.

### Agent and user interface

Compose starts the agent automatically. It can also be run directly for local development.

One LangChain agent with two deterministic tools, running a local model through [Ollama](https://ollama.com):

- `calculator(a, operation, b)`: `add`, `subtract`, `multiply` or `divide`, without `eval`.
- `lookup_course_info(topic)`: facts about this project (deadlines, timeline, stage owners, datasets, models, deliverables) from `backend/agent/course_info.json`.

The browser polls through checking, waiting, and answering, then displays the stored agent response.
Blocked and review outcomes explain why a prompt was held; each prompt is an independent agent run.

### Storage

| Table | Contents |
|---|---|
| `prompts` | UUID, original text, source, creation time, optional dataset link, P1/P2 snapshot |
| `guard_results` | One result per prompt, including P3 similarity, P4 classification, P5 anomaly score/status, and P7 decision |
| `dataset_prompts` | NeurAlchemy, deepset and jackhhao training/evaluation rows |
| `agent_runs` | Agent status, response, tools, latency, model, and errors for a handled prompt |

The schema and foreign keys are shown in [database-schema.dbml](database-schema.dbml)
and [database.png](database.png). The PNG predates `preprocessing`, P3/P5 fields, and
the source index; the DBML/SQL include them. Re-running `init-db` preserves data and adds the
dataset-link, preprocessing, P3/P5 columns and indexes to older databases.

P10 decisions are stored in `throttle_decisions`; P7 uses an `allowed_prompt_outbox` for reliable publishing.
P8 maintains `attack_aggregate_current` and timestamped `attack_aggregate_history` snapshots.

## Testing and experiments

### Existing automated checks

The complete-flow checks use real Kafka and PostgreSQL with injected deterministic model/agent fixtures.
They exercise routing outcomes, the outbox, duplicate execution prevention, and the HTTP result lifecycle.
P1/P2 and replay checks use local fixtures and controlled clocks; optional integration checks exercise real storage and Kafka.
P3/P4 unit checks inject encoder/classifier fixtures, while separate opt-in checks use MiniLM and Ollama.
These descriptions identify the available checks; they do not record a completed test run or its outcome.
Commands and test-environment setup are in the [developer guide](developer-guide.md#checks).

### Model evaluation methodology

The saved B1/B2 artifacts are evaluated on imported external-test rows, with deepset and jackhhao reported separately.
The evaluation reports malicious-class precision, recall, F1, macro-F1, false-positive rate, ROC-AUC, and confusion counts.
Per-prompt inference latency includes feature or embedding generation and prediction, and excludes model loading.
B3's saved classifier and configuration include its attack-type evaluation metrics.

### Recorded automated test results

### Model comparison results

### Guard latency and throughput

### Drift detection experiment

### Burst detection and throttling experiment

### Retraining before and after

### Guarded versus unguarded agent demonstration

### Dashboard evaluation
