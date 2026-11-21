"""PIJD dashboards. Each project member has a sidebar section with their own pages.

Run from the project root: streamlit run backend/dashboard/app.py
To add a page: create backend/dashboard/views/<name>.py (use backend.dashboard.data.frame for
PostgreSQL), then replace your placeholder below with st.Page("views/<name>.py", title=...).
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))  # `streamlit run` only adds this file's folder

import streamlit as st  # noqa: E402

from backend.dashboard import data  # noqa: E402


def placeholder(member, stages, slug):
    def page():
        st.title(f"{member}: {stages}")
        st.info(
            f"{member}'s dashboard has not been added yet. Create `backend/dashboard/views/{slug}.py`, "
            "read data with `backend.dashboard.data.frame(sql, params)`, and register the page in "
            "`backend/dashboard/app.py` in place of this placeholder."
        )

    page.__name__ = f"{slug}_placeholder"
    return st.Page(page, title=f"{stages} dashboard", url_path=slug)


st.set_page_config(page_title="PIJD dashboards", layout="wide")

pages = {
    "Blendi (P1–P2)": [placeholder("Blendi", "P1–P2 ingestion and IP join", "blendi")],
    "Emma (P3–P4)": [placeholder("Emma", "P3–P4 similarity and classification", "emma")],
    "Francesca (P5–P6)": [placeholder("Francesca", "P5–P6 anomaly and drift", "francesca")],
    "Shahed (P7–P8)": [placeholder("Shahed", "P7–P8 routing and Top-10", "shahed")],
    "Nándor (P9–P10)": [
        st.Page("views/nandor_burst.py", title="Burst detection & throttling", url_path="nandor-burst"),
        st.Page("views/nandor_agent_tools.py", title="Agent tool use", url_path="nandor-agent-tools"),
    ],
}

navigation = st.navigation(pages)
with st.sidebar:
    st.caption(f"PostgreSQL: `{data.database_label()}`")
    st.caption(f"Data is cached for {data.CACHE_SECONDS} s.")
    if st.button("Refresh data"):
        st.cache_data.clear()
navigation.run()
