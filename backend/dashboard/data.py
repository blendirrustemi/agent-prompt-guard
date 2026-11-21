"""Shared PostgreSQL access for every dashboard page.

Pages call `frame(sql, params)`: it returns a pandas DataFrame, caches results for a few seconds,
and shows an error instead of crashing when the database is unreachable.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

from backend import db

ROOT = Path(__file__).resolve().parents[2]
CACHE_SECONDS = 10
TIME_RANGES = {
    "Last 15 minutes": timedelta(minutes=15),
    "Last hour": timedelta(hours=1),
    "Last 24 hours": timedelta(hours=24),
    "Last 7 days": timedelta(days=7),
    "All time": None,
}


def database_url():
    load_dotenv(ROOT / ".env")
    return os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")


def database_label():
    """Host, port and database name only; never the password."""
    parts = urlsplit(database_url())
    return f"{parts.hostname or 'localhost'}:{parts.port or 5432}{parts.path}"


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def query(sql, params=()):
    with db.connect(database_url()) as conn:
        rows = conn.execute(sql, params).fetchall()
    return pd.DataFrame(rows)


def frame(sql, params=()):
    """Run a read-only query; on a database error show it on the page and stop rendering."""
    try:
        return query(sql, params)
    except psycopg.Error as error:
        st.error(f"Could not read PostgreSQL at {database_label()} ({type(error).__name__}). Is the database running?")
        st.stop()


def time_range_start(label):
    """Start of a TIME_RANGES window; 'All time' starts at the Unix epoch."""
    span = TIME_RANGES[label]
    return datetime.now(timezone.utc) - span if span else datetime(1970, 1, 1, tzinfo=timezone.utc)


def time_range_picker(key):
    label = st.selectbox("Time range", list(TIME_RANGES), index=2, key=key)
    return time_range_start(label)
