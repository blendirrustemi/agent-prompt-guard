"""Human labels for the exact prompts P7 routed to review."""

import streamlit as st

from backend import db
from backend.dashboard import data

st.title("P7 review queue")
st.caption("Labels are saved against the prompt ID. Batch 4 exports only prompts that do not overlap imported datasets.")

reviewer = st.text_input("Reviewer (optional)", key="reviewer_name").strip() or None
try:
    rows = db.list_review_queue(data.database_url())
except Exception as error:
    st.error(f"Could not load the review queue ({type(error).__name__}). Is PostgreSQL running? Initialize the database schema if this is a fresh deployment.")
    st.stop()

if not rows:
    st.info("No prompts have been routed to review yet.")
else:
    for row in rows:
        prompt_id = str(row["id"])
        label = row["label"]
        exportable_reason = row["reason"] == "score_in_review_band"
        dataset_replay = bool(row["dataset_prompt_id"])
        with st.container(border=True):
            st.caption(f"{row['created_at']} · {prompt_id} · P7 reason: {row['reason'] or '—'} · score: {row['malicious_score']}")
            if dataset_replay:
                st.warning("Dataset replay: the Batch 4 exporter excludes this prompt. Label a new, non-dataset prompt for retraining.")
            elif not exportable_reason:
                st.warning("This prompt did not reach the score-based review band, so the Batch 4 exporter excludes it.")
            st.write(row["text"])
            cols = st.columns([1, 1, 3])
            choice = cols[0].selectbox(
                "Label", ["Benign (0)", "Malicious (1)"],
                index=0 if label in (None, 0) else 1, key=f"label_{prompt_id}",
                disabled=dataset_replay or not exportable_reason,
            )
            if label is not None:
                cols[1].caption(f"Saved: {'Benign' if label == 0 else 'Malicious'}")
            if cols[2].button("Save label", key=f"save_{prompt_id}",
                               disabled=dataset_replay or not exportable_reason):
                db.save_prompt_review(data.database_url(), prompt_id, int(choice[-2]), reviewer)
                data.query.clear()
                st.rerun()

st.divider()
st.write("Export labeled P7 review prompts with `python -m backend.batch.export_review_queue reviewed_p7_prompts.csv`, then pass that CSV to Batch 4.")
