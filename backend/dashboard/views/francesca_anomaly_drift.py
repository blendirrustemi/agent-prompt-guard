"""Francesca, P5/P6: prompt anomaly outcomes and P4 score distribution monitoring."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

st.title("Anomaly & score drift (P5/P6)")
st.caption(
    "P5 scores individual prompts using prompt, P3 similarity, and P4 features. P6 monitors "
    "the distribution of P4 malicious scores over time; it does not measure model accuracy."
)

since = data.time_range_picker("francesca_time_range")
params = (since,)

summary = data.frame(
    """SELECT count(*) AS prompts,
              count(*) FILTER (WHERE p5_status = 'scored') AS scored,
              count(*) FILTER (WHERE p5_status = 'warming_up') AS warming,
              count(*) FILTER (WHERE p5_status IS NULL) AS pending,
              count(*) FILTER (WHERE malicious_score IS NOT NULL) AS p6_scores,
              avg(malicious_score) AS mean_score
       FROM prompts p LEFT JOIN guard_results g ON g.prompt_id = p.id
       WHERE p.created_at >= %s""",
    params,
)
stats = summary.iloc[0]
cols = st.columns(5)
cols[0].metric("Prompts", f"{int(stats['prompts']):,}")
cols[1].metric("P5 scored", f"{int(stats['scored']):,}")
cols[2].metric("P5 warming up", f"{int(stats['warming']):,}")
cols[3].metric("P5 pending", f"{int(stats['pending']):,}")
cols[4].metric("P6 score mean", "—" if pd.isna(stats["mean_score"]) else f"{stats['mean_score']:.3f}")

anomalies = data.frame(
    """SELECT p.created_at, p.source, g.anomaly_score, g.p5_status,
              g.p5_feature_version, g.malicious_score, g.decision
       FROM guard_results g JOIN prompts p ON p.id = g.prompt_id
       WHERE p.created_at >= %s AND g.p5_status IS NOT NULL
       ORDER BY p.created_at""",
    params,
)
st.subheader("P5 prompt anomaly scores")
if anomalies.empty:
    st.info("No P5 results in this time range. Scores appear after the detector warms up on 250 prompts.")
else:
    scored = anomalies[anomalies["p5_status"] == "scored"].copy()
    if scored.empty:
        st.info("P5 is still warming up; no anomaly scores are available in this time range.")
    else:
        scored["created_at"] = pd.to_datetime(scored["created_at"], utc=True)
        scored["decision_label"] = scored["decision"].fillna("not decided")
        st.caption("Higher scores indicate prompts less familiar to the Half-Space Trees detector. No fixed alert threshold is defined by P5.")
        chart = alt.Chart(scored).mark_circle(size=48, opacity=0.65).encode(
            x=alt.X("created_at:T", title="Prompt time"),
            y=alt.Y("anomaly_score:Q", title="P5 anomaly score", scale=alt.Scale(zero=True)),
            color=alt.Color("decision_label:N", title="P7 decision"),
            tooltip=["created_at:T", "source:N", "anomaly_score:Q", "malicious_score:Q", "decision_label:N"],
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
        p5_table = scored.drop(columns=["decision"])
        st.dataframe(p5_table.sort_values("created_at", ascending=False).head(100), use_container_width=True, hide_index=True)

st.subheader("P6 P4 malicious-score stream")
scores = data.frame(
    """SELECT g.scored_at, g.malicious_score
       FROM guard_results g
       WHERE g.scored_at >= %s AND g.malicious_score IS NOT NULL
       ORDER BY g.scored_at, g.prompt_id""",
    params,
)
if scores.empty:
    st.info("No persisted P4 malicious scores in this time range.")
else:
    scores["scored_at"] = pd.to_datetime(scores["scored_at"], utc=True)
    scores["score"] = pd.to_numeric(scores["malicious_score"])
    scores["bucket"] = scores["scored_at"].dt.floor("15min")
    trend = scores.groupby("bucket", as_index=False).agg(
        mean_score=("score", "mean"), p10=("score", lambda values: values.quantile(0.10)),
        p90=("score", lambda values: values.quantile(0.90)), prompts=("score", "size"),
    )
    alerts = data.frame(
        """SELECT detected_at, scored_at, prompt_id::text AS prompt_id,
                  detector_width, detector_estimate
           FROM p6_drift_alerts WHERE scored_at >= %s ORDER BY scored_at DESC""", params,
    )
    st.caption("Line shows the 15-minute mean P4 malicious probability; the shaded band shows the 10th–90th percentile. Red markers are persisted P6 ADWIN detections. A score-distribution shift does not establish attack prevalence or model accuracy.")
    band = alt.Chart(trend).mark_area(opacity=0.18, color="#4c78a8").encode(
        x=alt.X("bucket:T", title="Scoring time"),
        y=alt.Y("p10:Q", title="P4 malicious score", scale=alt.Scale(domain=[0, 1])),
        y2=alt.Y2("p90:Q"), tooltip=["bucket:T", "p10:Q", "p90:Q", "prompts:Q"],
    )
    trend_line = alt.Chart(trend).mark_line(point=True, color="#1f4e79").encode(
        x=alt.X("bucket:T", title="Scoring time"),
        y=alt.Y("mean_score:Q", title="Mean P4 malicious score", scale=alt.Scale(domain=[0, 1])),
        tooltip=["bucket:T", "mean_score:Q", "prompts:Q"],
    ).interactive()
    trend_chart = band + trend_line
    if not alerts.empty:
        alerts["scored_at"] = pd.to_datetime(alerts["scored_at"], utc=True)
        trend_chart = trend_chart + alt.Chart(alerts).mark_rule(color="#d62728", strokeWidth=2).encode(
            x="scored_at:T", tooltip=["scored_at:T", "detected_at:T", "detector_width:Q", "detector_estimate:Q"],
        )
    st.altair_chart(trend_chart, use_container_width=True)
    st.dataframe(trend.sort_values("bucket", ascending=False), use_container_width=True, hide_index=True)
    st.markdown("**Persisted ADWIN detections**")
    if alerts.empty:
        st.info("No P6 detections in this time range.")
    else:
        st.dataframe(alerts.sort_values("scored_at", ascending=False), use_container_width=True, hide_index=True)
