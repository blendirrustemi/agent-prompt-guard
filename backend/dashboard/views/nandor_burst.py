"""Nándor, P9/P10: prompts per source in the sliding window, with the burst threshold and burst starts."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

TOP_SOURCES = 8

st.title("Burst detection & throttling (P9/P10)")
st.caption(
    "P9 counts every prompt per source in a sliding window; P10 blocks a source for a cooldown "
    "once that count reaches the threshold. Data: `throttle_decisions`."
)

since = data.time_range_picker("burst_range")
totals = data.frame(
    """SELECT count(DISTINCT source) AS sources, count(*) AS prompts,
              count(*) FILTER (WHERE action = 'block') AS blocked,
              count(*) FILTER (WHERE burst_started) AS bursts
       FROM throttle_decisions WHERE event_time >= %s""",
    (since,),
)
active = data.frame(
    "SELECT count(DISTINCT source) AS active FROM throttle_decisions WHERE burst_started AND blocked_until > now()"
)
row = totals.iloc[0] if not totals.empty else {}
columns = st.columns(4)
columns[0].metric("Sources seen", int(row.get("sources", 0) or 0))
columns[1].metric("Prompts blocked by P10", f"{int(row.get('blocked', 0) or 0)} / {int(row.get('prompts', 0) or 0)}")
columns[2].metric("Bursts detected", int(row.get("bursts", 0) or 0))
columns[3].metric("Sources blocked now", int(active.iloc[0]["active"]) if not active.empty else 0)

ranked = data.frame(
    """SELECT source, count(*) AS prompts FROM throttle_decisions
       WHERE event_time >= %s GROUP BY source ORDER BY prompts DESC, source LIMIT 200""",
    (since,),
)
if ranked.empty:
    st.info("No throttle decisions in this time range. Send prompts, e.g. with `python -m backend.tests.burst_experiment`.")
    st.stop()

sources = st.multiselect(
    "Sources", ranked["source"].tolist(), default=ranked["source"].head(TOP_SOURCES).tolist(),
    help=f"Defaults to the {TOP_SOURCES} most active sources in the time range.",
)
if not sources:
    st.info("Select at least one source.")
    st.stop()

series = data.frame(
    """SELECT source, event_time, window_count, action, coalesce(reason, '') AS reason, burst_started,
              blocked_until, threshold, window_seconds
       FROM throttle_decisions WHERE event_time >= %s AND source = ANY(%s)
       ORDER BY event_time LIMIT 20000""",
    (since, list(sources)),
)
threshold = int(series["threshold"].max())
window_seconds = int(series["window_seconds"].max())
tooltip = [
    alt.Tooltip("source:N", title="Source"),
    alt.Tooltip("event_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
    alt.Tooltip("window_count:Q", title=f"Prompts in last {window_seconds} s"),
    alt.Tooltip("action:N", title="P10 action"),
    alt.Tooltip("reason:N", title="P10 reason"),
]
base = alt.Chart(series).encode(
    x=alt.X("event_time:T", title="Event time"),
    y=alt.Y("window_count:Q", title=f"Prompts from source in last {window_seconds} s (P9 window)"),
    color=alt.Color("source:N", title="Source"),
)
lines = base.mark_line(point=alt.OverlayMarkDef(size=25)).encode(tooltip=tooltip)
bursts = (
    alt.Chart(series[series["burst_started"]])
    .mark_point(shape="triangle-up", size=160, filled=True, color="#d62728")
    .encode(x="event_time:T", y="window_count:Q", tooltip=tooltip + [alt.Tooltip("blocked_until:T", title="Blocked until", format="%H:%M:%S")])
)
rule = (
    alt.Chart(pd.DataFrame({"threshold": [threshold]}))
    .mark_rule(strokeDash=[6, 4], color="gray")
    .encode(y="threshold:Q", tooltip=[alt.Tooltip("threshold:Q", title="Burst threshold")])
)
st.subheader("Prompts per source over time")
st.altair_chart((lines + rule + bursts).interactive(), width="stretch")
st.caption(f"Dashed line: burst threshold ({threshold}). Red triangles: prompts where P9 detected a burst and P10 started a block.")

with st.expander("Recent bursts"):
    started = series[series["burst_started"]].sort_values("event_time", ascending=False)
    st.dataframe(
        started[["source", "event_time", "window_count", "blocked_until"]].rename(columns={
            "source": "Source", "event_time": "Burst at", "window_count": "Prompts in window", "blocked_until": "Blocked until",
        }),
        hide_index=True, width="stretch",
    )
