"""Batch 1 and Batch 2 model evaluation results."""

import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[3]
results = json.loads((ROOT / "results" / "evaluation.json").read_text(encoding="utf-8"))["results"]
models = {"batch1": "Batch 1", "batch2": "Batch 2"}
model_colors = alt.Scale(domain=list(models.values()), range=["#1f77b4", "#ff7f0e"])

st.title("Model evaluation")
st.caption("These two charts compare the Batch 1 (TF-IDF + LR) and Batch 2 (MiniLM encoder + LR) binary classification models on the two external datasets.")
dataset = st.selectbox("Dataset", ["deepset", "jackhhao"], key="emma_dataset")
selected = results[dataset]

performance = pd.DataFrame([
    {"Model": label, "Metric": metric, "Score": selected[model][field]}
    for model, label in models.items()
    for metric, field in (
        ("Precision", "precision_malicious"),
        ("Recall", "recall_malicious"),
        ("Macro-F1", "macro_f1"),
    )
])
st.subheader("Model performance")
performance_chart = alt.Chart(performance).mark_bar().encode(
    x=alt.X("Metric:N", sort=["Precision", "Recall", "Macro-F1"], title=None, axis=alt.Axis(labelAngle=0)),
    xOffset=alt.XOffset("Model:N", sort=list(models.values())),
    y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format=".2f")),
    color=alt.Color("Model:N", scale=model_colors),
    tooltip=["Model:N", "Metric:N", alt.Tooltip("Score:Q", format=".2f")],
)
st.altair_chart(performance_chart, width="stretch")

false_positives = pd.DataFrame([
    {"Dataset": name, "Model": label, "Rate": values[model]["false_positive_rate"] * 100}
    for name, values in results.items()
    for model, label in models.items()
])
st.subheader("False-positive rates")
false_positive_chart = alt.Chart(false_positives).mark_bar().encode(
    x=alt.X("Dataset:N", sort=["deepset", "jackhhao"], title=None, axis=alt.Axis(labelAngle=0)),
    xOffset=alt.XOffset("Model:N", sort=list(models.values())),
    y=alt.Y("Rate:Q", title="False-positive rate (%)", scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(format=".2f")),
    color=alt.Color("Model:N", scale=model_colors),
    tooltip=["Dataset:N", "Model:N", alt.Tooltip("Rate:Q", title="Rate (%)", format=".2f")],
)
st.altair_chart(false_positive_chart, width="stretch")

st.subheader("Results")
st.caption("Latency includes encoding and prediction.")
table = pd.DataFrame([
    {
        "Model": label,
        "Accuracy": selected[model]["accuracy"],
        "Macro-F1": selected[model]["macro_f1"],
        "ROC-AUC": selected[model]["roc_auc"],
        "Mean latency (ms)": selected[model]["latency_ms"]["mean"]
    }
    for model, label in models.items()
])
st.dataframe(
    table,
    column_config={column: st.column_config.NumberColumn(format="%.2f") for column in table.columns if column != "Model"},
    hide_index=True,
    width="stretch",
)
