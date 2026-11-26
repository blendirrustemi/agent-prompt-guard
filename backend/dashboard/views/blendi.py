"""Blendi, P1/P2: prompt intake, preprocessing features, and saved source history."""

import altair as alt
import pandas as pd
import streamlit as st

from backend.dashboard import data

BUCKET_WIDTHS = {
    "Last 15 minutes": (60, "minute"),
    "Last hour": (60, "minute"),
    "Last 24 hours": (3600, "hour"),
    "Last 7 days": (3600, "hour"),
    "All time": (604800, "week"),
}

st.title("Prompt intake & source context (P1/P2)")
since = data.time_range_picker("blendi_time_range")
params = (since,)

st.subheader("P1 prompt intake")
summary = data.frame(
    """SELECT count(*) AS prompts,
              count(dataset_prompt_id) AS replayed,
              count(preprocessing) AS preprocessed
       FROM prompts WHERE created_at >= %s""",
    params,
).iloc[0]
total = int(summary["prompts"])
replayed = int(summary["replayed"])
preprocessed = int(summary["preprocessed"])
coverage = f"{preprocessed / total:.1%}" if total else "—"
columns = st.columns(4)
columns[0].metric("Prompts", f"{total:,}")
columns[1].metric("Live submissions", f"{total - replayed:,}")
columns[2].metric("Dataset replay", f"{replayed:,}")
columns[3].metric("Preprocessing coverage", coverage)
if not total:
    st.info("No prompts in this time range.")
    st.stop()

bucket_seconds, bucket_unit = BUCKET_WIDTHS[st.session_state["blendi_time_range"]]
volume = data.frame(
    """WITH intake_volume AS (
           SELECT date_bin(%s * INTERVAL '1 second', created_at, TIMESTAMPTZ '2000-01-03 00:00:00+00') AS bucket,
                  CASE WHEN dataset_prompt_id IS NULL THEN 'Live submission' ELSE 'Dataset replay' END AS intake,
                  count(*) AS prompts
           FROM prompts WHERE created_at >= %s GROUP BY 1, 2
       )
       SELECT bucket, bucket + %s * INTERVAL '1 second' AS bucket_end, intake, prompts
       FROM intake_volume ORDER BY bucket, intake""",
    (bucket_seconds, since, bucket_seconds),
)
if not volume.empty:
    volume = volume.copy()
    volume["bucket"] = pd.to_datetime(volume["bucket"], utc=True)
    volume["bucket_end"] = pd.to_datetime(volume["bucket_end"], utc=True)
    volume["bucket_label"] = volume["bucket"].dt.strftime("%Y-%m-%d %H:%M UTC")
    chart = alt.Chart(volume).mark_bar().encode(
        x=alt.X("bucket:T", title="Prompt time (UTC)", scale=alt.Scale(type="utc")),
        x2="bucket_end:T",
        y=alt.Y("prompts:Q", title=f"Prompts per {bucket_unit}", stack="zero"),
        color=alt.Color("intake:N", title="Intake"),
        tooltip=[
            alt.Tooltip("bucket_label:N", title="Bucket start"),
            alt.Tooltip("intake:N", title="Intake"),
            alt.Tooltip("prompts:Q", title="Prompts"),
        ],
    )
    st.altair_chart(chart, width="stretch")

st.subheader("Recent preprocessing results")
recent = data.frame(
    """SELECT created_at AS "Prompt time", id::text AS "Prompt ID", source AS "Source",
              preprocessing IS NOT NULL AS "Preprocessed",
              (preprocessing #>> '{features,char_count}')::integer AS "Characters",
              (preprocessing #>> '{features,word_count}')::integer AS "Words",
              (preprocessing #>> '{features,symbol_ratio}')::double precision AS "Symbol ratio"
       FROM prompts WHERE created_at >= %s ORDER BY created_at DESC, id DESC LIMIT 100""",
    params,
)
if not recent.empty:
    st.dataframe(
        recent, hide_index=True, width="stretch",
        column_config={"Symbol ratio": st.column_config.NumberColumn(format="percent")},
    )

st.subheader("P2 source context")
snapshots = data.frame(
    """SELECT DISTINCT ON (source) source,
              (preprocessing #>> '{source_context,as_of}')::timestamptz AS snapshot_at,
              (preprocessing #>> '{source_context,allow_count}')::bigint AS allow_count,
              (preprocessing #>> '{source_context,block_count}')::bigint AS block_count,
              (preprocessing #>> '{source_context,review_count}')::bigint AS review_count
       FROM prompts
       WHERE created_at >= %s AND preprocessing #>> '{source_context,as_of}' IS NOT NULL
       ORDER BY source, snapshot_at DESC, id DESC""",
    params,
)
if snapshots.empty:
    st.info("No source-context snapshots were saved for prompts in this time range.")
    st.stop()

snapshots = snapshots.set_index("source")
source = st.selectbox("Source", snapshots.index.tolist(), key="blendi_source")
snapshot = snapshots.loc[source]
snapshot_at = pd.to_datetime(snapshot["snapshot_at"], utc=True)
st.caption(f"Latest saved snapshot for this source. Decisions in the 10 minutes ending {snapshot_at:%Y-%m-%d %H:%M:%S UTC}.")
for column, label, field in zip(
    st.columns(3), ("Allow", "Block", "Review"), ("allow_count", "block_count", "review_count"),
):
    column.metric(label, f"{int(snapshot[field]):,}")
