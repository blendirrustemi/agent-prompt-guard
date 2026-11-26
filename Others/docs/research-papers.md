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

## 7. Efficient Elastic Burst Detection in Data Streams (2003)

**Authors:** Yunyue Zhu and Dennis Shasha.

**Venue:** *Proceedings of the Ninth ACM SIGKDD International Conference on Knowledge Discovery and Data Mining (KDD '03)*, pp. 336–345.

**Source:** [ACM Digital Library](https://doi.org/10.1145/956750.956789).

Defines elastic bursts: a stream is monitored over several sliding-window sizes at once, each with its own threshold, and a burst is reported whenever any window's aggregate reaches its threshold. This is the basis for P9's three windows per source (10 s/6, 60 s/10 and 600 s/40 prompts): the short window catches sharp floods early, and the long window catches slow, steady sources that never exceed the per-minute limit.

## 8. ReAct: Synergizing Reasoning and Acting in Language Models (2023)

**Authors:** Shunyu Yao, Jeffrey Zhao, Dian Yu, Nan Du, Izhak Shafran, Karthik Narasimhan, and Yuan Cao.

**Venue:** *International Conference on Learning Representations (ICLR 2023)*.

**Source:** [arXiv:2210.03629](https://arxiv.org/abs/2210.03629).

Introduces agents that interleave reasoning with actions, such as tool or API calls, and use each result to decide the next step. Acting on looked-up information reduces hallucination. Our agent follows this loop: LangChain's `create_agent` lets the model call `calculator` or `lookup_course_info`, reads the result, and continues until it can answer. The agent tool-use dashboard counts these actions per tool.
## 9. Perspectives on Incorporating Expert Feedback into Model Updates (2023)

**Authors:** Valerie Chen, Umang Bhatt, Hoda Heidari, Adrian Weller, and Ameet Talwalkar.

**Venue:** *Patterns*, vol. 4, no. 7, article 100780.

**Source:** [DOI](https://doi.org/10.1016/j.patter.2023.100780).

**Relevant project components:**
- Batch 4: Reviewed-label retraining

**Reason for including:**

This paper discusses ways to incorporate expert feedback into model updates, including updates to training data. It provides context for Batch 4's use of reviewed labels. Our CSV workflow, validation comparison, and promotion rule are project-specific; the paper does not evaluate this implementation.

## 10. A Meta-level Analysis of Online Anomaly Detectors (2023)

**Authors:** Antonios Ntroumpogiannis, Michail Giannoulis, Nikolaos Myrtakis, Vassilis Christophides, Eric Simon, and Ioannis Tsamardinos.

**Venue:** *The VLDB Journal*, vol. 32, no. 4, pp. 845–886.

**Source:** [DOI](https://doi.org/10.1007/s00778-022-00773-x).

**Relevant project components:**
- P5: Streaming anomaly detection

**Reason for including:**

This study compares online anomaly detectors across algorithm families and data characteristics, including tree-based methods. It provides background for selecting an online detector.

## 11. River: Machine Learning for Streaming Data in Python (2021)

**Authors:** Jacob Montiel, Max Halford, Saulo Martiello Mastelini, Geoffrey Bolmier, Raphael Sourty, Robin Vaysse, Adil Zouitine, Heitor Murilo Gomes, Jesse Read, Talel Abdessalem, and Albert Bifet.

**Venue:** *Journal of Machine Learning Research*, vol. 22, no. 110, pp. 1–8.

**Source:** [JMLR](https://www.jmlr.org/papers/v22/20-1380.html).

**Relevant project components:**
- P5: Half-Space Trees implementation
- P6: ADWIN drift detector implementation

**Reason for including:**

River is the Python library used by P5 and P6 for streaming-learning algorithms. This paper describes the library; the project's feature design, settings, and use of P4 scores with ADWIN are implementation choices.

## 12. Scalable Detection of Concept Drifts on Data Streams with Parallel Adaptive Windowing (2018)

**Authors:** Philipp M. Grulich, René Saitenmacher, Jonas Traub, Sebastian Breß, Tilmann Rabl, and Volker Markl.

**Venue:** In *Proceedings of the 21st International Conference on Extending Database Technology* (EDBT 2018), pp. 477–480.

**Source:** [DOI](https://doi.org/10.5441/002/edbt.2018.51).

**Relevant project components:**
- P6: Score drift monitoring with ADWIN

**Reason for including:**

This paper examines ADWIN and parallel approaches to scaling adaptive-window drift detection on high-velocity streams. It gives context for P6's detector choice; P6 monitors the distribution of P4 scores and does not establish changes in ground-truth attack prevalence or model accuracy.
