# Project report: PIJD Prompt Guard

[README](../README.md) | [Developer guide](developer-guide.md) | [Research-paper notes](research-papers.md)

## Abstract

## Introduction

AI agents may act on untrusted user input or external content when processing natural-language requests. The model can be redirected from its intended task or produce unsafe outputs by prompt-injection and jailbreak attacks. Programmable guardrails add an extra check before an agent act because training-time alignment alone may not enforce every application-specific rule at runtime ([Rebedea et al., 2023](https://doi.org/10.18653/v1/2023.emnlp-demo.40)). Hence, this project builds a prompt-injection and jailbreak guard.

The system uses ten streaming pipelines: **P1** ingests prompts and cleans text; **P2** adds source context; **P3** checks similarity to known attack prompts; **P4** classifies prompts and predicts attack types; **P5** detects anomalous prompts; **P6** monitors score drift; **P7** routes prompts to allow, block, or review; **P8** aggregates attack types and repeated patterns; **P9** detects source bursts; and **P10** applies rate limits. Allowed prompts can reach the agent, while the system stores decisions and results for analysis.


The project compares classical and embedding-based binary classifiers in Batches 1 and 2, adds attack-type classification in Batch 3, and evaluates reviewed-label retraining in Batch 4. Across its experiments, it measures model performance on primary and external datasets, runtime latency and throughput, drift and burst behavior, and guarded versus unguarded agent runs. The completed dashboards track decision trends, model comparisons, drift alerts, attack-type and Top-10 pattern summaries, and review-queue and throttling activity. Research on sliding-window stream summaries and model adaptation gives context for these design areas ([Gou et al., 2023](https://doi.org/10.1109/TKDE.2022.3151140); [Lu et al., 2019](https://doi.org/10.1109/TKDE.2018.2876857)).


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
A Streamlit dashboard app with one sidebar section per member is part of the runtime; so far it has the P9/P10 burst and agent tool-use pages. The other members' pages and human-label collection are not yet implemented.
The [team work plan](work-plan.xlsx) shows the planned tasks and the assigned team members. 

### Batch models

#### Batch 1: classical binary classification

TF-IDF features with Logistic Regression is used in Batch 1 to classify prompts as benign or malicious, and then we compare the saved B1 and B2 artifacts on the same imported external-test rows. ([Rebedea et al., 2023](https://doi.org/10.18653/v1/2023.emnlp-demo.40)), in NeMo Guardrails, discuss the runtime overhead of programmable guardrails and the potential role of specialized execution-rail models, which motivates a lightweight local classifier in the guard path. Although the paper does not evaluate TF-IDF or Logistic Regression, Batch 1 uses both as the project's classical baseline for comparison.

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

Batch 4 was trained using the repository's reviewed-label CSV, which had 140 rows (70 per label) and validated on 60 rows (30 per label). The existing classifier scored a macro-F1 of 0,6011, while the candidate scored 0.8496 on the same validation split. Additionally, the existing model produced the confusion matrix `[[9, 21], [0, 30]]`, and the candidate produced `[[24, 6], [3, 27]]` with true labels as rows and predicted labels as columns (benign, malicious). The candidate strictly improved over the baseline, so the pipeline promoted it and saved the run report, previous classifier, and previous config under `backend/batch/models/batch4_runs/promoted_20261121T121133Z/`. This validation result applies only to the reviewed sample and does not replace untouched test or external evaluation.
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

This section focuses on the pipelines 3 and 4 from the architecture. For each incoming prompt within the model’s 256-token limit, the intake worker creates one normalised, 384-dimensional embedding using the pretrained `all-MiniLM-L6-v2` encoder. The encoder’s weights remain frozen during processing. This prompt embedding is used for P3 similarity probing and P4 classification.

P3 compares the incoming embedding with two reference collections: malicious prompts from the NeurAlchemy training data and recent prompts that P4 predicted to be malicious with high confidence. It uses cosine similarity, which is equivalent to the dot product when both vectors are normalised to unit length. P3 returns the closest reference and its similarity score, marking a match when the score is at least 0.85. A similar embedding-based cosine comparison is described in [PromptGuard](https://www.nature.com/articles/s41598-025-31086-y). The numerical similarity score is stored in PostgreSQL and used as a feature by P5’s anomaly detector. 

P4 applies the trained Batch 2 classifier to estimate whether the incoming prompt is benign or malicious. Combining pretrained text embeddings with traditional classifiers for jailbreak detection has also been explored by [Galinkin and Sablotny (2024)](https://arxiv.org/abs/2412.01547). When the maliciousness probability is at least 0.5, Batch 3 also predicts one of the six attack families using the same embedding. These are predictions made with the saved classifiers.

P4 stores the probability, predicted attack family, model version, token coverage, and processing timings in PostgreSQL. P7 uses the stored scoring outcome together with throttling information to make the final allow, block, or review decision. Only allowed prompts reach the agent, which generates the response.

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

P7's role as a pre-execution allow/block/review gate is supported in NeMo Guardrails, where ([Rebedea et al., 2023](https://doi.org/10.18653/v1/2023.emnlp-demo.40)) describe reprogrammable, model-independent runtime rails and input moderation ahead of a dialogue system. In this implementation, P8's attack-type counts and repeated-pattern summary are operational monitoring outputs that do not override P7. Additionally, the sliding-window and Top-K literature motivates this rolling summary ([Gou et al., 2023](https://doi.org/10.1109/TKDE.2022.3151140)). Our implementation uses exact aggregation, and it does not implement the paper's approximate sketch framework or claim its complexity or error bounds.

### P9/P10: burst detection and rate limiting

The `burst` service (`python -m backend.stream.burst_worker`, group `pijd-burst-v1`) reads
`prompts_raw` beside the intake consumer:

**P9** follows the elastic-burst model of [Zhu & Shasha (2003)](https://doi.org/10.1145/956750.956789). It counts every prompt per `source` in several sliding windows at once, each with its own threshold,
and reports a burst when any window reaches its threshold. The defaults (`BURST_WINDOWS`) are 6
prompts in 10 seconds, 10 in 60 seconds and 40 in 600 seconds. The short window catches a sharp
flood after 6 prompts. The long window catches a slow, steady source, for example
one prompt every 12 seconds, which never reaches 10 per minute. The allowed rate falls as the
window grows (0.6, 0.17 and 0.067 prompts per second), as in the paper, where the threshold grows
more slowly than the window. Windows use the event's `created_at`, so replays produce the same bursts.

Every decision stores the count in each windo (`windows`), plus the **deciding window**: the shortest window that reached its threshold, or otherwise the one closest to it.

**P10** blocks a source as soon as P9 reports a burst. The block lasts `BURST_COOLDOWN_SECONDS` (default 300) and is not extended by traffic during it.

The challenge remaining is that like any per-source limiter, P9 cannot see an attacker who spreads traffic over many sources.

### Agent and user interface

Compose starts the agent automatically. It can also be run directly for local development.

One LangChain agent with two deterministic tools, running a local model through [Ollama](https://ollama.com):

- `calculator(a, operation, b)`: `add`, `subtract`, `multiply` or `divide`, without `eval`.
- `lookup_course_info(topic)`: facts about this project (deadlines, timeline, stage owners, datasets, models, deliverables) from `backend/agent/course_info.json`.

The agent is built with LangChain's `create_agent`, which runs a ReAct-style loop ([Yao et al., 2023](https://arxiv.org/abs/2210.03629)): the model either answers or calls a tool, reads the tool's result, and repeats until it can answer. Arithmetic and project facts therefore come from the tools rather than from the model's memory. Each run stores every tool call in `agent_runs.tool_calls`, which the agent tool-use dashboard counts per tool.

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

Batch 4 compared both models using the reviewed-label CSV on the same 60 validation prompts (30 benign and 30 malicious). the current model scored a macro-F1 of 0.6011, while candidate scored 0.8496 and was promoted. In the table below, the true labels and predictions, as rows and columns respectively, were mapped into confusion matrices and ordered benign then malicious: 

| Model | Validation macro-F1 | Confusion matrix |
| --- | ---: | --- |
| Current | 0.6011 | `[[9, 21], [0, 30]]` |
| Batch 4 candidate (promoted) | 0.8496 | `[[24, 6], [3, 27]]` |

`backend/batch/models/batch4_runs/` is where the reports, including label counts and candidate training metrics are located. Because the reviewed set is small, the promotion metric reflects model selection and should not be treated as an unbiased final test estimate.

### Guard latency and throughput

The guard latency and throughput were tested using the same 20 short NeurAlchemy test prompts (10 benign and 10 malicious) at target rates of 1 and 5 prompts per second. Latency was measured from just before publishing to Kafka until the recorded final guard decision, excluding agent execution. Throughput was completed decisions divided by the time from the first submission to the last decision, including the sending intervals. The following is a summary of the results:

| Run | Prompts completed | Mean latency (ms) | Median latency (ms) | p95 latency (ms) | Throughput (decisions/s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Slow (target 1 prompt/s) | 20/20 | 277.11 | 195.68 | 505.36 | 1.03 |
| Faster (target 5 prompts/s) | 20/20 | 301.78 | 323.53 | 387.99 | 4.63 |

Both runs produced 9 allow, 10 block and 1 review decisions. The review was caused by a score in the uncertainty band; no measured prompt exceeded the token limit. Mean latency increased slightly in the faster run. These small tests show performance at the chosen traffic rates and do not establish maximum system capacity.

### Drift detection experiment

### Burst detection and throttling experiment

The burst test (`python -m backend.tests.burst_experiment`, see the [developer guide](developer-guide.md#burst-detection-experiment)) ran against the full Compose stack with the default settings: the elastic windows of [Zhu & Shasha (2003)](https://doi.org/10.1145/956750.956789) with 6 prompts in 10 seconds, 10 in 60 seconds and 40 in 600 seconds, and a 300-second cooldown. A control IP first sent 5 prompts at 1 per second. Then a simulated attacker IP sent 20 prompts at 10 per second, about 2 seconds in total. The prompts were short calculator and course questions. The following is a summary of the results:

| Source | Sent | P10 allowed | P10 blocked | First blocked prompt | Window that fired | Final blocks with a throttle reason | Mean P10 latency (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Control (1 prompt/s) | 5 | 5 | 0 | – | – | 0 | 282.78 |
| Attacker (10 prompts/s) | 20 | 5 | 15 | 6 | 10 s | 15 | 1495.91 |

All prompts fell inside every window, so all three windows counted the attacker's prompts 1–5 identically, and P10 allowed them. At prompt 6 the 10-second window reached its threshold of 6, while the 60- and 600-second windows were still at 6 of 10 and 6 of 40. P9 detected the burst, and P10 blocked the source until 300 seconds after that prompt. Prompts 7–20 were blocked as `source_blocked` under the same block, and P7 finalized all 15 throttled prompts as `block` with a throttle reason. The control source, at most 5 prompts in 10 seconds, was never throttled. All five automated checks passed.

### Retraining before and after

[Batch 4: reviewed-label retraining](#batch-4-reviewed-label-retraining) reports the validation metrics and confusion matrices for the current and candidate models. The candidate strictly improved macro-F1 and was promoted, though its validation score is not an untouched test result.

When data distributions change, concept-drift research treats retraining as one way to adapt ([Lu et al., 2019](https://doi.org/10.1109/TKDE.2018.2876857)),, which gives context for comparing the reviewed-label candidate with the active model. In this project, Batch 4 was run manually on reviewed data. P6's score-distribution alert does not trigger retraining automatically, and the strict macro-F1 promotion rule is our own selection policy.


### Guarded versus unguarded agent demonstration

### Dashboard evaluation
