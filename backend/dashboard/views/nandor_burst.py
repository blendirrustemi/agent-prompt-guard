"""Nándor, P9/P10: prompts per source in a chosen P9 window, with its threshold and burst starts."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

TOP_SOURCES = 8

st.title("Burst detection & throttling (P9/P10)")
st.caption(
    "P9 counts every prompt per source in several sliding windows (10 s, 60 s, 600 s by default); P10 blocks "
    "a source for a cooldown once any window reaches its threshold. Data: `throttle_decisions`."
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

# One row per prompt and P9 window. Rows stored before elastic windows have '[]' and one window in the columns.
all_windows = data.frame(
    """SELECT t.source, t.event_time, w.seconds AS window_seconds, w.count AS window_count, w.threshold,
              t.action, coalesce(t.reason, '') AS reason, t.burst_started, t.blocked_until,
              t.window_seconds AS fired_seconds, t.window_count AS fired_count
       FROM throttle_decisions t
       CROSS JOIN LATERAL jsonb_to_recordset(
           CASE WHEN jsonb_array_length(t.windows) > 0 THEN t.windows
                ELSE jsonb_build_array(jsonb_build_object(
                    'seconds', t.window_seconds, 'count', t.window_count, 'threshold', t.threshold))
           END
       ) AS w(seconds integer, count integer, threshold integer)
       WHERE t.event_time >= %s AND t.source = ANY(%s)
       ORDER BY t.event_time LIMIT 60000""",
    (since, list(sources)),
)
sizes = sorted(int(size) for size in all_windows["window_seconds"].unique())
window_seconds = st.selectbox(
    "P9 window", sizes, key="burst_window", format_func=lambda size: f"{size} s",
    help="P9 checks several window sizes at once (elastic bursts, Zhu & Shasha 2003). "
         "A burst fires when any window reaches its own threshold.",
)
series = all_windows[all_windows["window_seconds"] == window_seconds]
threshold = int(series["threshold"].iloc[-1])
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
    .encode(x="event_time:T", y="window_count:Q", tooltip=tooltip + [
        alt.Tooltip("fired_seconds:Q", title="Fired by window (s)"),
        alt.Tooltip("blocked_until:T", title="Blocked until", format="%H:%M:%S"),
    ])
)
rule = (
    alt.Chart(pd.DataFrame({"threshold": [threshold]}))
    .mark_rule(strokeDash=[6, 4], color="gray")
    .encode(y="threshold:Q", tooltip=[alt.Tooltip("threshold:Q", title="Burst threshold")])
)
st.subheader("Prompts per source over time")
st.altair_chart((lines + rule + bursts).interactive(), width="stretch")
st.caption(
    f"Dashed line: the {window_seconds} s window's threshold ({threshold}). Red triangles: prompts where P9 "
    "detected a burst (in any window) and P10 started a block; hover to see which window fired."
)

with st.expander("Recent bursts"):
    started = series[series["burst_started"]].sort_values("event_time", ascending=False)
    st.dataframe(
        started[["source", "event_time", "fired_seconds", "fired_count", "blocked_until"]].rename(columns={
            "source": "Source", "event_time": "Burst at", "fired_seconds": "Fired by window (s)",
            "fired_count": "Prompts in that window", "blocked_until": "Blocked until",
        }),
        hide_index=True, width="stretch",
    )
