"""Nándor, agent: how often the LangChain agent calls each tool."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

NO_TOOL = "no tool"

st.title("Agent tool use")
st.caption(
    "Tool calls made by the LangChain agent for allowed prompts: `calculator` and "
    "`lookup_course_info`. Data: `agent_runs.tool_calls`."
)

since = data.time_range_picker("agent_range")
totals = data.frame(
    """SELECT count(*) AS runs,
              count(*) FILTER (WHERE status = 'completed') AS completed,
              count(*) FILTER (WHERE status = 'completed' AND jsonb_array_length(tool_calls) > 0) AS with_tools,
              avg(latency_ms) FILTER (WHERE status = 'completed') AS mean_latency_ms
       FROM agent_runs WHERE created_at >= %s""",
    (since,),
)
row = totals.iloc[0] if not totals.empty else {}
runs, completed = int(row.get("runs", 0) or 0), int(row.get("completed", 0) or 0)
with_tools = int(row.get("with_tools", 0) or 0)
mean_latency = row.get("mean_latency_ms")
columns = st.columns(4)
columns[0].metric("Agent runs", runs)
columns[1].metric("Completed", completed)
columns[2].metric("Completed runs using a tool", f"{with_tools / completed:.0%}" if completed else "–")
columns[3].metric("Mean latency", f"{float(mean_latency) / 1000:.1f} s" if mean_latency is not None and pd.notna(mean_latency) else "–")

calls = data.frame(
    """SELECT tc->>'name' AS tool, count(*) AS calls
       FROM agent_runs, jsonb_array_elements(tool_calls) AS tc
       WHERE created_at >= %s GROUP BY 1 ORDER BY 2 DESC""",
    (since,),
)
if runs == 0:
    st.info("No agent runs in this time range. Allowed prompts are answered by the agent worker.")
    st.stop()

bars = pd.concat(
    [calls if not calls.empty else pd.DataFrame(columns=["tool", "calls"]),
     pd.DataFrame({"tool": [NO_TOOL], "calls": [completed - with_tools]})],
    ignore_index=True,
)
bars["calls"] = bars["calls"].astype(int)
bars["kind"] = ["No tool used" if tool == NO_TOOL else "Tool" for tool in bars["tool"]]
chart = (
    alt.Chart(bars)
    .mark_bar()
    .encode(
        x=alt.X("tool:N", title="Tool", sort="-y"),
        y=alt.Y("calls:Q", title="Calls"),
        color=alt.Color("kind:N", title=None, scale=alt.Scale(domain=["Tool", "No tool used"], range=["#1f77b4", "#bbbbbb"])),
        tooltip=[alt.Tooltip("tool:N", title="Tool"), alt.Tooltip("calls:Q", title="Calls")],
    )
)
st.subheader("Tool calls per tool")
st.altair_chart(chart, width="stretch")
st.caption(f"'{NO_TOOL}': completed runs that answered without calling a tool. A run can call several tools.")

with st.expander("Recent agent runs"):
    recent = data.frame(
        """SELECT created_at, status,
                  coalesce((SELECT string_agg(tc->>'name', ', ') FROM jsonb_array_elements(tool_calls) tc), '') AS tools,
                  latency_ms, model
           FROM agent_runs WHERE created_at >= %s ORDER BY created_at DESC LIMIT 50""",
        (since,),
    )
    st.dataframe(
        recent.rename(columns={
            "created_at": "Time", "status": "Status", "tools": "Tools", "latency_ms": "Latency (ms)", "model": "Model",
        }),
        hide_index=True, width="stretch",
    )
