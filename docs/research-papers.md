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
