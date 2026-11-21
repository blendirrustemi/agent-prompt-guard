# Research Papers

## 1. Improved Large Language Model Jailbreak Detection via Pretrained Embeddings (2024)

**Paper:** https://arxiv.org/abs/2412.01547

**Relevant project components:**
- Batch 1: Classical machine learning classifier
- Batch 2: Embedding-based classifier

**Reason for including:**

This paper investigates the use of pretrained text embeddings and machine learning classifiers for detecting jailbreak attempts. It is relevant to our project because we plan to compare a classical machine learning approach with an embedding-based approach for identifying malicious prompts.

## 2. Embedding-based classifiers can detect prompt injection attacks (CAMLIS 2024)

**Paper:** https://ceur-ws.org/Vol-3920/paper15.pdf
**Code:** https://github.com/AhsanAyub/malicious-prompt-detection

**Relevant project components:**
- Batch 2: embedding-based classifier (MiniLM + classifier head)
- Dataset pipeline

**Reason for including:**

Applies sentence embeddings (including all-MiniLM-L6-v2) with Logistic Regression, XGBoost and Random Forest to prompt-injection detection. Random Forest outperformed Logistic Regression on MiniLM embeddings. Covers direct prompt injection only.

## 3. PromptGuard: A Structured Framework for Injection-Resilient Language Models (2026)

**Paper**: https://doi.org/10.1038/s41598-025-31086-y 

**Relevant project components**: 
- Pipeline 3: Stateful Similarity Probing

**Reason for including:**

This paper looks into a multi-layered framework focused on detecting and preventing prompt injection attacks. It discusses using semantic encoders, such as MiniLM, to convert the incoming promts into embeddings and comparing them with known malicious prompt embeddings using cosine similarity.

## 4. NeMo Guardrails: A Toolkit for Controllable and Safe LLM Applications with Programmable Rails (2023)

**Authors:** Traian Rebedea, Razvan Dinu, Makesh Narsimhan Sreedhar, Christopher Parisien, and Jonathan Cohen.

**Venue:** Proceedings of the 2023 Conference on Empirical Methods in Natural Language Processing: System Demonstrations, pp. 431–445.

**Source:** [ACL Anthology](https://aclanthology.org/2023.emnlp-demo.40/). **DOI:** [10.18653/v1/2023.emnlp-demo.40](https://doi.org/10.18653/v1/2023.emnlp-demo.40).

This paper presents model-independent programmable runtime rails and pre-execution input moderation for LLM applications. This architecture motivates P7's role as a gate before agent execution. Their discussion of runtime overhead and specialized execution-rail models also gives broad motivation for Batch 1 as a lightweight local check, though the paper does not evaluate TF-IDF or Logistic Regression.   

## 5. A Sketch Framework for Approximate Data Stream Processing in Sliding Windows (2023)

**Authors:** Xiangyang Gou, Yinda Zhang, Zhoujing Hu, Long He, Ke Wang, Xilai Liu, Tong Yang, Yi Wang, and Bin Cui.

**Venue:** *IEEE Transactions on Knowledge and Data Engineering*, vol. 35, no. 5, pp. 4411–4424.

**Source:** [IEEE Xplore](https://doi.org/10.1109/TKDE.2022.3151140).

Formalizes sliding-window stream queries, including Top-K frequency queries. That formulation supports the concept behind P8's rolling Top-10 pattern summaries. Our implementation uses exact aggregation; it does not implement the paper's approximate sketch algorithm or claim its efficiency guarantees.   

## 6. Learning under Concept Drift: A Review (2019)

**Authors:** Jie Lu, Anjin Liu, Fan Dong, Feng Gu, João Gama, and Guangquan Zhang.

**Venue:** *IEEE Transactions on Knowledge and Data Engineering*, vol. 31, no. 12, pp. 2346–2363.

**Source:** [IEEE Xplore](https://doi.org/10.1109/TKDE.2018.2876857).

This paper reviews concept-drift detection and adaptation methods, including model retraining. Their survey gives context for the Batch 4 before-and-after comparison. In this project, we launch Batch 4 manually on reviewed labels, and drift alerts do not trigger retraining automatically.