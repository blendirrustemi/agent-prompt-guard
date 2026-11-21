"""Shahed, P7/P8: decision trends, attack-type counts, and repeated prompt patterns."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

ATTACK_LABELS = {
    "adversarial_attacks": "Adversarial attacks",
    "indirect_agent_manipulation": "Indirect injection / agent manipulation",
    "information_extraction": "Information extraction",
    "instruction_manipulation": "Instruction manipulation",
    "jailbreak_persona": "Jailbreak / persona manipulation",
    "obfuscation": "Obfuscation",
}
DECISION_COLORS = alt.Scale(
    domain=["allow", "block", "review"],
    range=["#2ca02c", "#d62728", "#ffbf00"],
)

st.title("Attack patterns & decision routing (P7/P8)")
st.caption(
    "Attack summaries come from P8's stored rolling-window snapshots. Decision counts come from "
    "final P7 outcomes in PostgreSQL."
)

snapshot = data.frame(
    """SELECT snapshot_at, window_start, window_end, attack_type_counts, top_patterns
       FROM attack_aggregate_current WHERE id = 1"""
)
if snapshot.empty:
    snapshot = data.frame(
        """SELECT snapshot_at, window_start, window_end, attack_type_counts, top_patterns
           FROM attack_aggregate_history ORDER BY snapshot_at DESC LIMIT 1"""
    )

if snapshot.empty:
    st.info("P8 has not stored an aggregation snapshot yet. Attack charts will appear after the aggregation worker processes prompts.")
else:
    saved = snapshot.iloc[0]
    attack_counts = saved["attack_type_counts"] or {}
    count_rows = [
        {
            "attack_type": attack_type,
            "attack_label": label,
            "prompt_count": int(attack_counts.get(attack_type, 0)),
        }
        for attack_type, label in ATTACK_LABELS.items()
    ]
    counts = pd.DataFrame(count_rows)
    snapshot_at = pd.to_datetime(saved["snapshot_at"], utc=True)
    window_start = pd.to_datetime(saved["window_start"], utc=True)
    window_end = pd.to_datetime(saved["window_end"], utc=True)

    st.subheader("Attack-type counts in the latest P8 window")
    st.caption(
        f"60-second event-time window: {window_start:%Y-%m-%d %H:%M:%S %Z} to "
        f"{window_end:%Y-%m-%d %H:%M:%S %Z}. Snapshot saved at {snapshot_at:%Y-%m-%d %H:%M:%S %Z}."
    )
    attack_chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X("prompt_count:Q", title="Classified prompts in the window", scale=alt.Scale(domainMin=0)),
            y=alt.Y("attack_label:N", title="Predicted attack type", sort="-x"),
            color=alt.Color("attack_type:N", legend=None),
            tooltip=[
                alt.Tooltip("attack_label:N", title="Attack type"),
                alt.Tooltip("prompt_count:Q", title="Classified prompts"),
            ],
        )
        .properties(title="Classified prompts by attack type", height=230)
    )
    st.altair_chart(attack_chart, width="stretch")

    pattern_map = saved["top_patterns"] or {}
    pattern_rows = []
    for attack_type, patterns in pattern_map.items():
        for item in patterns or []:
            text = str(item.get("text", ""))
            preview = text if len(text) <= 72 else text[:69] + "..."
            pattern_rows.append(
                {
                    "attack_type": attack_type,
                    "attack_label": ATTACK_LABELS.get(attack_type, attack_type),
                    "pattern": text,
                    "preview": preview,
                    "count": int(item.get("count", 0)),
                }
            )
    pattern_rows.sort(key=lambda item: (-item["count"], item["attack_label"], item["pattern"]))
    top_patterns = pd.DataFrame(pattern_rows[:10])

    st.subheader("Top 10 repeated attack prompts / patterns")
    st.caption("Ranked across attack types from the latest P8 window; a pattern must occur at least twice.")
    if top_patterns.empty:
        st.info("No repeated classified attack patterns were found in the latest P8 window.")
    else:
        pattern_chart = (
            alt.Chart(top_patterns)
            .mark_bar()
            .encode(
                x=alt.X("count:Q", title="Occurrences in the P8 window", scale=alt.Scale(domainMin=0)),
                y=alt.Y("preview:N", title="Prompt / pattern", sort="-x"),
                color=alt.Color("attack_label:N", title="Predicted attack type"),
                tooltip=[
                    alt.Tooltip("attack_label:N", title="Attack type"),
                    alt.Tooltip("pattern:N", title="Normalized prompt / pattern"),
                    alt.Tooltip("count:Q", title="Occurrences"),
                ],
            )
            .properties(title="Most frequent repeated attack patterns", height=320)
        )
        st.altair_chart(pattern_chart, width="stretch")
        st.dataframe(
            top_patterns[["attack_label", "count", "pattern"]].rename(
                columns={"attack_label": "Attack type", "count": "Occurrences", "pattern": "Normalized prompt / pattern"}
            ),
            hide_index=True,
            width="stretch",
        )

st.divider()
st.subheader("Final decisions over time")
range_label = st.selectbox("Decision time range", list(data.TIME_RANGES), index=2, key="shahed_decision_range")
since = data.time_range_start(range_label)
span = data.TIME_RANGES[range_label]
span_seconds = span.total_seconds() if span is not None else None
bucket = "minute" if span_seconds is not None and span_seconds <= 86400 else (
    "hour" if span_seconds is not None and span_seconds <= 604800 else "day"
)
decisions = data.frame(
    """SELECT date_trunc(%s, checked_at) AS decision_time, decision, count(*)::int AS prompts
       FROM guard_results
       WHERE checked_at >= %s AND decision IN ('allow', 'block', 'review')
       GROUP BY 1, 2 ORDER BY 1, 2""",
    (bucket, since),
)
if decisions.empty:
    st.info("No final P7 decisions are stored in this time range yet.")
else:
    decision_chart = (
        alt.Chart(decisions)
        .mark_bar()
        .encode(
            x=alt.X("decision_time:T", title=f"Decision time ({bucket} buckets)"),
            y=alt.Y("prompts:Q", title="Finalized prompts", stack="zero"),
            color=alt.Color("decision:N", title="P7 decision", scale=DECISION_COLORS),
            tooltip=[
                alt.Tooltip("decision_time:T", title="Time", format="%Y-%m-%d %H:%M"),
                alt.Tooltip("decision:N", title="Decision"),
                alt.Tooltip("prompts:Q", title="Prompts"),
            ],
        )
        .properties(title="Allow, block, and review decisions", height=300)
    )
    st.altair_chart(decision_chart, width="stretch")
