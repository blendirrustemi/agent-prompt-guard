"""Render the Streamlit dashboards with fixture data (no database needed)."""

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import psycopg

try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # streamlit is a runtime dependency; skip if this environment lacks it
    AppTest = None

DASHBOARD = Path(__file__).resolve().parents[1] / "dashboard"
T0 = datetime(2026, 11, 21, 14, 30, tzinfo=timezone.utc)


def burst_query(sql, params=()):
    if "AS active" in sql:
        return pd.DataFrame([{"active": 1}])
    if "count(DISTINCT source) AS sources" in sql:
        return pd.DataFrame([{"sources": 2, "prompts": 25, "blocked": 11, "bursts": 1}])
    if "GROUP BY source" in sql:
        return pd.DataFrame([{"source": "198.51.100.66", "prompts": 20}, {"source": "198.51.100.10", "prompts": 5}])
    rows = []
    for i in range(12):
        burst = i + 1 >= 10
        rows.append({
            "source": "198.51.100.66", "event_time": T0 + timedelta(seconds=i / 10), "window_count": i + 1,
            "action": "block" if burst else "allow", "reason": "burst_detected" if i + 1 == 10 else ("source_blocked" if burst else ""),
            "burst_started": i + 1 == 10, "blocked_until": T0 + timedelta(seconds=300) if burst else None,
            "threshold": 10, "window_seconds": 60,
        })
    rows.append({
        "source": "198.51.100.10", "event_time": T0, "window_count": 1, "action": "allow", "reason": "",
        "burst_started": False, "blocked_until": None, "threshold": 10, "window_seconds": 60,
    })
    return pd.DataFrame(rows)


def agent_query(sql, params=()):
    if "AS runs" in sql:
        return pd.DataFrame([{"runs": 6, "completed": 5, "with_tools": 3, "mean_latency_ms": 4200.0}])
    if "GROUP BY 1" in sql:
        return pd.DataFrame([{"tool": "calculator", "calls": 3}, {"tool": "lookup_course_info", "calls": 1}])
    return pd.DataFrame([{"created_at": T0, "status": "completed", "tools": "calculator", "latency_ms": 4200, "model": "qwen3:1.7b"}])


def empty_query(sql, params=()):
    if "AS runs" in sql:
        return pd.DataFrame([{"runs": 0, "completed": 0, "with_tools": 0, "mean_latency_ms": None}])
    if "count(DISTINCT source) AS sources" in sql:
        return pd.DataFrame([{"sources": 0, "prompts": 0, "blocked": 0, "bursts": 0}])
    if "AS active" in sql:
        return pd.DataFrame([{"active": 0}])
    return pd.DataFrame()


def database_down(sql, params=()):
    raise psycopg.OperationalError("connection refused")


@unittest.skipIf(AppTest is None, "streamlit is not installed")
class DashboardTests(unittest.TestCase):
    def run_page(self, page, query):
        app = AppTest.from_file(str(DASHBOARD / "views" / page), default_timeout=30)
        with patch("backend.dashboard.data.query", side_effect=query):
            app.run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        return app

    def charts(self, app):
        # The element name differs between Streamlit versions.
        return app.get("vega_lite_chart") + app.get("arrow_vega_lite_chart")

    def test_navigation_lists_every_member(self):
        app = AppTest.from_file(str(DASHBOARD / "app.py"), default_timeout=30)
        app.run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        self.assertIn("Blendi", app.info[0].value)

    def test_burst_page_shows_metrics_and_chart(self):
        app = self.run_page("nandor_burst.py", burst_query)
        self.assertEqual([m.label for m in app.metric][:3], ["Sources seen", "Prompts blocked by P10", "Bursts detected"])
        self.assertEqual(app.metric[1].value, "11 / 25")
        self.assertEqual(len(self.charts(app)), 1)

    def test_agent_page_shows_metrics_and_chart(self):
        app = self.run_page("nandor_agent_tools.py", agent_query)
        self.assertEqual(app.metric[2].value, "60%")
        self.assertEqual(app.metric[3].value, "4.2 s")
        self.assertEqual(len(self.charts(app)), 1)

    def test_empty_data_shows_a_hint(self):
        for page in ("nandor_burst.py", "nandor_agent_tools.py"):
            with self.subTest(page=page):
                app = self.run_page(page, empty_query)
                self.assertEqual(len(self.charts(app)), 0)
                self.assertTrue(app.info)

    def test_database_error_is_shown(self):
        for page in ("nandor_burst.py", "nandor_agent_tools.py"):
            with self.subTest(page=page):
                app = self.run_page(page, database_down)
                self.assertIn("Could not read PostgreSQL", app.error[0].value)


if __name__ == "__main__":
    unittest.main()
