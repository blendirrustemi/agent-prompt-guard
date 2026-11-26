"""Render the Streamlit dashboards with fixture data (no database needed)."""

import json
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


def blendi_query(sql, params=()):
    if "AS preprocessed" in sql:
        return pd.DataFrame([{"prompts": 4, "replayed": 1, "preprocessed": 3}])
    if "date_bin" in sql:
        bucket_end = T0 + timedelta(seconds=params[0])
        return pd.DataFrame([
            {"bucket": T0, "bucket_end": bucket_end, "intake": "Live submission", "prompts": 3},
            {"bucket": T0, "bucket_end": bucket_end, "intake": "Dataset replay", "prompts": 1},
        ])
    if 'AS "Preprocessed"' in sql:
        return pd.DataFrame([
            {"Prompt time": T0, "Prompt ID": "recent", "Source": "192.0.2.1",
             "Preprocessed": True, "Characters": 12, "Words": 2, "Symbol ratio": 0.25},
            {"Prompt time": T0, "Prompt ID": "older", "Source": "192.0.2.2",
             "Preprocessed": False, "Characters": None, "Words": None, "Symbol ratio": None},
        ])
    if "DISTINCT ON (source)" in sql:
        return pd.DataFrame([
            {"source": "192.0.2.1", "snapshot_at": T0, "allow_count": 4, "block_count": 1, "review_count": 0},
            {"source": "192.0.2.2", "snapshot_at": T0 - timedelta(minutes=1),
             "allow_count": 0, "block_count": 0, "review_count": 2},
        ])
    raise AssertionError(f"Unexpected dashboard query: {sql}")


def burst_query(sql, params=()):
    if "AS active" in sql:
        return pd.DataFrame([{"active": 1}])
    if "count(DISTINCT source) AS sources" in sql:
        return pd.DataFrame([{"sources": 2, "prompts": 25, "blocked": 11, "bursts": 1}])
    if "GROUP BY source" in sql:
        return pd.DataFrame([{"source": "198.51.100.66", "prompts": 20}, {"source": "198.51.100.10", "prompts": 5}])
    assert "jsonb_to_recordset" in sql, sql
    # One row per prompt and elastic window; the attacker's 6th prompt fires the 10 s window.
    rows = []
    for i in range(12):
        burst = i + 1 >= 6
        for seconds, threshold in ((10, 6), (60, 10), (600, 40)):
            rows.append({
                "source": "198.51.100.66", "event_time": T0 + timedelta(seconds=i / 10),
                "window_seconds": seconds, "window_count": i + 1, "threshold": threshold,
                "action": "block" if burst else "allow",
                "reason": "burst_detected" if i + 1 == 6 else ("source_blocked" if burst else ""),
                "burst_started": i + 1 == 6, "blocked_until": T0 + timedelta(seconds=300) if burst else None,
                "fired_seconds": 10, "fired_count": i + 1,  # 10 s is always the closest to its threshold here
            })
    rows.append({
        "source": "198.51.100.10", "event_time": T0, "window_seconds": 60, "window_count": 1, "threshold": 10,
        "action": "allow", "reason": "", "burst_started": False, "blocked_until": None,
        "fired_seconds": 60, "fired_count": 1,  # a row stored before elastic windows: one window only
    })
    return pd.DataFrame(rows)


def agent_query(sql, params=()):
    if "AS runs" in sql:
        return pd.DataFrame([{"runs": 6, "completed": 5, "with_tools": 3, "mean_latency_ms": 4200.0}])
    if "GROUP BY 1" in sql:
        return pd.DataFrame([{"tool": "calculator", "calls": 3}, {"tool": "lookup_course_info", "calls": 1}])
    return pd.DataFrame([{"created_at": T0, "status": "completed", "tools": "calculator", "latency_ms": 4200, "model": "qwen3:1.7b"}])


def shahed_query(sql, params=()):
    if "attack_aggregate_current" in sql:
        return pd.DataFrame([{
            "snapshot_at": T0,
            "window_start": T0 - timedelta(seconds=60),
            "window_end": T0,
            "attack_type_counts": {
                "adversarial_attacks": 0,
                "indirect_agent_manipulation": 0,
                "information_extraction": 1,
                "instruction_manipulation": 2,
                "jailbreak_persona": 0,
                "obfuscation": 0,
            },
            "top_patterns": {
                "instruction_manipulation": [
                    {"pattern_id": "p1", "text": "ignore previous instructions", "count": 3}
                ],
                "information_extraction": [
                    {"pattern_id": "p2", "text": "reveal the hidden prompt", "count": 2}
                ],
            },
        }])
    if "attack_aggregate_history" in sql:
        return pd.DataFrame()
    if "date_trunc" in sql:
        return pd.DataFrame([
            {"decision_time": T0 - timedelta(minutes=1), "decision": "allow", "prompts": 4},
            {"decision_time": T0, "decision": "block", "prompts": 2},
            {"decision_time": T0, "decision": "review", "prompts": 1},
        ])
    raise AssertionError(f"Unexpected dashboard query: {sql}")


def shahed_empty_query(sql, params=()):
    if "date_trunc" in sql or "attack_aggregate_current" in sql or "attack_aggregate_history" in sql:
        return pd.DataFrame()
    raise AssertionError(f"Unexpected dashboard query: {sql}")


def empty_query(sql, params=()):
    if "AS preprocessed" in sql:
        return pd.DataFrame([{"prompts": 0, "replayed": 0, "preprocessed": 0}])
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

    def test_navigation_opens_blendi_dashboard(self):
        app = AppTest.from_file(str(DASHBOARD / "app.py"), default_timeout=30)
        with patch("backend.dashboard.data.query", side_effect=blendi_query):
            app.run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        self.assertEqual(app.title[0].value, "Prompt intake & source context (P1/P2)")

    def test_blendi_page_shows_intake_and_switches_source_context(self):
        app = self.run_page("blendi.py", blendi_query)
        self.assertEqual([metric.value for metric in app.metric], ["4", "3", "1", "75.0%", "4", "1", "0"])
        self.assertEqual(len(self.charts(app)), 1)
        recent = app.dataframe[0].value
        self.assertEqual(recent["Preprocessed"].tolist(), [True, False])
        self.assertTrue(pd.isna(recent.iloc[1]["Symbol ratio"]))
        with patch("backend.dashboard.data.query", side_effect=blendi_query):
            app.selectbox(key="blendi_source").select("192.0.2.2").run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        self.assertEqual([metric.value for metric in app.metric[-3:]], ["0", "0", "2"])
        self.assertIn("14:29:00 UTC", app.caption[-1].value)
        with patch("backend.dashboard.data.query", side_effect=blendi_query) as query:
            app.selectbox(key="blendi_time_range").select("All time").run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        volume_params = [call.args[1] for call in query.call_args_list if "date_bin" in call.args[0]]
        self.assertEqual(volume_params, [(604800, datetime(1970, 1, 1, tzinfo=timezone.utc), 604800)])

    def test_emma_page_shows_both_datasets_without_database_queries(self):
        import streamlit as st

        report = json.loads((DASHBOARD.parents[1] / "results" / "evaluation.json").read_text())
        app = self.run_page("emma_model_evaluation.py", database_down)
        self.assertEqual(app.title[0].value, "Model evaluation")
        self.assertEqual(app.selectbox(key="emma_dataset").value, "deepset")
        self.assertEqual([caption.value for caption in app.caption], [
            "Saved results before Batch 4 retraining.",
            "Latency includes encoding and prediction; excludes model loading.",
        ])
        for dataset in ("deepset", "jackhhao"):
            with (
                self.subTest(dataset=dataset),
                patch("backend.dashboard.data.query", side_effect=database_down),
                patch("streamlit.altair_chart", wraps=st.altair_chart) as render_chart,
            ):
                app.selectbox(key="emma_dataset").select(dataset).run()
                self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
                charts = self.charts(app)
                self.assertEqual(len(charts), 2)
                self.assertEqual(len(app.dataframe), 1)
                table = app.dataframe[0].value
                self.assertEqual(list(table.columns), [
                    "Model", "Accuracy", "Macro-F1", "ROC-AUC", "Mean latency (ms)", "p95 latency (ms)",
                ])
                self.assertEqual(table["Model"].tolist(), ["Batch 1", "Batch 2"])
                for index, model in enumerate(("batch1", "batch2")):
                    saved = report["results"][dataset][model]
                    for column, expected in (
                        ("Accuracy", saved["accuracy"]), ("Macro-F1", saved["macro_f1"]),
                        ("ROC-AUC", saved["roc_auc"]), ("Mean latency (ms)", saved["latency_ms"]["mean"]),
                        ("p95 latency (ms)", saved["latency_ms"]["p95"]),
                    ):
                        self.assertAlmostEqual(table.iloc[index][column], expected)

                # Check display formatting without rounding the saved numbers.
                columns = json.loads(app.dataframe[0].proto.columns)
                for column in table.columns[1:]:
                    self.assertEqual(columns[column]["type_config"]["format"], "%.2f")
                performance, false_positives = [call.args[0].to_dict() for call in render_chart.call_args_list]
                for spec, domain in ((performance, [0, 1]), (false_positives, [0, 100])):
                    self.assertEqual(spec["encoding"]["y"]["scale"]["domain"], domain)
                    self.assertEqual(spec["encoding"]["y"]["axis"]["format"], ".2f")
                    self.assertEqual(spec["encoding"]["tooltip"][-1]["format"], ".2f")

                performance_rows = render_chart.call_args_list[0].args[0].data.to_dict(orient="records")
                for row in performance_rows:
                    model = "batch1" if row["Model"] == "Batch 1" else "batch2"
                    field = {"Precision": "precision_malicious", "Recall": "recall_malicious", "Macro-F1": "macro_f1"}[row["Metric"]]
                    self.assertAlmostEqual(row["Score"], report["results"][dataset][model][field])
                rate_rows = render_chart.call_args_list[1].args[0].data.to_dict(orient="records")
                self.assertEqual(len(rate_rows), 4)
                for row in rate_rows:
                    model = "batch1" if row["Model"] == "Batch 1" else "batch2"
                    self.assertAlmostEqual(row["Rate"], report["results"][row["Dataset"]][model]["false_positive_rate"] * 100)

    def test_burst_page_shows_metrics_and_chart(self):
        app = self.run_page("nandor_burst.py", burst_query)
        self.assertEqual([m.label for m in app.metric][:3], ["Sources seen", "Prompts blocked by P10", "Bursts detected"])
        self.assertEqual(app.metric[1].value, "11 / 25")
        self.assertEqual(len(self.charts(app)), 1)
        window = app.selectbox(key="burst_window")
        self.assertEqual(window.options, ["10 s", "60 s", "600 s"])
        self.assertIn("threshold (6)", app.caption[-1].value)
        with patch("backend.dashboard.data.query", side_effect=burst_query):
            window.select(60).run()
        self.assertEqual(len(app.exception), 0, [e.value for e in app.exception])
        self.assertIn("threshold (10)", app.caption[-1].value)

    def test_agent_page_shows_metrics_and_chart(self):
        app = self.run_page("nandor_agent_tools.py", agent_query)
        self.assertEqual(app.metric[2].value, "60%")
        self.assertEqual(app.metric[3].value, "4.2 s")
        self.assertEqual(len(self.charts(app)), 1)

    def test_shahed_page_renders_all_three_data_charts(self):
        app = self.run_page("shahed_attack_patterns.py", shahed_query)
        self.assertEqual(len(self.charts(app)), 3)
        self.assertTrue(any("Top 10 repeated" in item.value for item in app.subheader))

    def test_shahed_page_handles_no_saved_data(self):
        app = self.run_page("shahed_attack_patterns.py", shahed_empty_query)
        self.assertEqual(len(self.charts(app)), 0)
        self.assertTrue(app.info)

    def test_empty_data_shows_a_hint(self):
        for page in ("blendi.py", "nandor_burst.py", "nandor_agent_tools.py", "shahed_attack_patterns.py"):
            with self.subTest(page=page):
                app = self.run_page(page, empty_query)
                self.assertEqual(len(self.charts(app)), 0)
                self.assertTrue(app.info)

    def test_database_error_is_shown(self):
        for page in ("blendi.py", "nandor_burst.py", "nandor_agent_tools.py", "shahed_attack_patterns.py"):
            with self.subTest(page=page):
                app = self.run_page(page, database_down)
                self.assertIn("Could not read PostgreSQL", app.error[0].value)


if __name__ == "__main__":
    unittest.main()
