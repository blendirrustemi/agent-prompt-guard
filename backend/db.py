"""Small PostgreSQL helpers; each submission uses a single transaction."""

from pathlib import Path
from datetime import timedelta
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def connect(database_url):
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=3)


def init_db(database_url):
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connect(database_url) as conn:
        conn.execute(schema, prepare=False)


def serialize_prompt(row):
    return {
        "id": str(row["id"]),
        "text": row["text"],
        "created_at": row["created_at"].isoformat(),
        "decision": row["decision"],
        "dataset_prompt_id": str(row["dataset_prompt_id"]) if row["dataset_prompt_id"] else None,
    }


def serialize_guard_result(row):
    return {
        "prompt_id": str(row["prompt_id"]),
        "decision": row["decision"],
        "malicious_score": row["malicious_score"],
        "attack_type": row["attack_type"],
        "reason": row["reason"],
        "model_version": row["model_version"],
        "checked_at": row["checked_at"].isoformat() if row["checked_at"] else None,
    }


def save_prompt(database_url, text, dataset_prompt_id=None, source="web", prompt_id=None, created_at=None):
    """Store a prompt; pass dataset_prompt_id when it was replayed from dataset_prompts."""
    prompt_id = prompt_id or uuid4()
    with connect(database_url) as conn:
        row = conn.execute(
            """INSERT INTO prompts (id, text, source, created_at, dataset_prompt_id)
               VALUES (%s, %s, %s, COALESCE(%s, NOW()), %s)
               RETURNING id, text, created_at, dataset_prompt_id""",
            (prompt_id, text, source, created_at, dataset_prompt_id),
        ).fetchone()
        conn.execute("INSERT INTO guard_results (prompt_id) VALUES (%s)", (prompt_id,))
    # The context manager commits both inserts together before reporting success.
    row["decision"] = "pending"
    return serialize_prompt(row)


def save_raw_event(database_url, event, preprocessing=None):
    """Store intake and P1/P2 atomically; preserve the first snapshot and verdict."""
    with connect(database_url) as conn:
        conn.execute(
            """INSERT INTO prompts (id, text, source, created_at, dataset_prompt_id, preprocessing)
               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING""",
            (event["prompt_id"], event["text"], event["source"], event["created_at"], event["dataset_prompt_id"],
             Jsonb(preprocessing) if preprocessing is not None else None),
        )
        row = conn.execute("SELECT * FROM prompts WHERE id = %s", (event["prompt_id"],)).fetchone()
        dataset_id = str(row["dataset_prompt_id"]) if row["dataset_prompt_id"] else None
        if (row["text"], row["source"], dataset_id) != (
            event["text"], event["source"], event["dataset_prompt_id"]
        ):
            raise ValueError("Prompt ID already exists with different content or source metadata.")
        if row["preprocessing"] is None and preprocessing is not None:
            conn.execute(
                "UPDATE prompts SET preprocessing = %s WHERE id = %s AND preprocessing IS NULL",
                (Jsonb(preprocessing), event["prompt_id"]),
            )
        # Retries can have a new timestamp; preserve the first stored timestamp.
        conn.execute(
            "INSERT INTO guard_results (prompt_id) VALUES (%s) ON CONFLICT (prompt_id) DO NOTHING",
            (event["prompt_id"],),
        )
        result = conn.execute("SELECT decision FROM guard_results WHERE prompt_id = %s", (event["prompt_id"],)).fetchone()
        row["decision"] = result["decision"]
    return serialize_prompt(row)


def get_source_context(database_url, source, *, as_of):
    """Count recently checked decisions, not raw arrivals or dataset labels."""
    with connect(database_url) as conn:
        row = conn.execute(
            """SELECT count(*) FILTER (WHERE g.decision = 'allow') AS allow_count,
                      count(*) FILTER (WHERE g.decision = 'block') AS block_count,
                      count(*) FILTER (WHERE g.decision = 'review') AS review_count
               FROM prompts p JOIN guard_results g ON g.prompt_id = p.id
               WHERE p.source = %s AND g.checked_at >= %s AND g.checked_at <= %s
                 AND g.decision IN ('allow', 'block', 'review')""",
            (source, as_of - timedelta(minutes=10), as_of),
        ).fetchone()
    return {**row, "as_of": as_of.isoformat()}


def get_prompt(database_url, prompt_id):
    with connect(database_url) as conn:
        row = conn.execute(
            """SELECT p.id, p.text, p.created_at, p.dataset_prompt_id, g.decision
               FROM prompts p JOIN guard_results g ON g.prompt_id = p.id WHERE p.id = %s""",
            (prompt_id,),
        ).fetchone()
    return serialize_prompt(row) if row else None


def list_prompts(database_url):
    with connect(database_url) as conn:
        rows = conn.execute(
            """SELECT p.id, p.text, p.created_at, p.dataset_prompt_id, g.decision
               FROM prompts AS p
               JOIN guard_results AS g ON g.prompt_id = p.id
               ORDER BY p.created_at DESC, p.id DESC
               LIMIT 50"""
        ).fetchall()
    return [serialize_prompt(row) for row in reversed(rows)]


def save_guard_result(
    database_url, prompt_id, decision, malicious_score=None, attack_type=None, reason=None, model_version=None
):
    """Record the classifier's verdict; returns None if the prompt does not exist."""
    with connect(database_url) as conn:
        row = conn.execute(
            """UPDATE guard_results
               SET decision = %s, malicious_score = %s, attack_type = %s,
                   reason = %s, model_version = %s, checked_at = NOW()
               WHERE prompt_id = %s
               RETURNING *""",
            (decision, malicious_score, attack_type, reason, model_version, prompt_id),
        ).fetchone()
    return serialize_guard_result(row) if row else None


def get_guard_result(database_url, prompt_id):
    with connect(database_url) as conn:
        row = conn.execute("SELECT * FROM guard_results WHERE prompt_id = %s", (prompt_id,)).fetchone()
    return serialize_guard_result(row) if row else None


def get_prompt_text(database_url, prompt_id):
    """Return the stored text of a prompt, or None if it does not exist."""
    with connect(database_url) as conn:
        row = conn.execute("SELECT text FROM prompts WHERE id = %s", (prompt_id,)).fetchone()
    return row["text"] if row else None


def serialize_agent_run(row):
    return {
        "prompt_id": str(row["prompt_id"]),
        "status": row["status"],
        "response": row["response"],
        "tool_calls": row["tool_calls"],
        "latency_ms": row["latency_ms"],
        "model": row["model"],
        "error": row["error"],
        "created_at": row["created_at"].isoformat(),
    }


def save_agent_run(
    database_url, prompt_id, status, response=None, tool_calls=(), latency_ms=None, model=None, error=None
):
    """Record one agent run; returns None if this prompt already has one (e.g. Kafka redelivery)."""
    with connect(database_url) as conn:
        row = conn.execute(
            """INSERT INTO agent_runs (prompt_id, status, response, tool_calls, latency_ms, model, error)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (prompt_id) DO NOTHING
               RETURNING *""",
            (prompt_id, status, response, Jsonb(list(tool_calls)), latency_ms, model, error),
        ).fetchone()
    return serialize_agent_run(row) if row else None


def get_agent_run(database_url, prompt_id):
    with connect(database_url) as conn:
        row = conn.execute("SELECT * FROM agent_runs WHERE prompt_id = %s", (prompt_id,)).fetchone()
    return serialize_agent_run(row) if row else None


THROTTLE_FIELDS = (
    "prompt_id", "source", "action", "reason", "window_count", "window_seconds", "threshold",
    "blocked_until", "burst_started", "event_time", "decided_at",
)


def serialize_throttle_decision(row):
    """Return the stored P10 decision with the same fields as the `throttle_decisions` event (minus schema_version)."""
    return {
        **{field: row[field] for field in THROTTLE_FIELDS},
        "prompt_id": str(row["prompt_id"]),
        "blocked_until": row["blocked_until"].isoformat() if row["blocked_until"] else None,
        "event_time": row["event_time"].isoformat(),
        "decided_at": row["decided_at"].isoformat(),
    }


def save_throttle_decision(database_url, decision):
    """Store a P10 decision once; on redelivery the first stored decision is returned unchanged."""
    columns = ", ".join(THROTTLE_FIELDS)
    placeholders = ", ".join(["%s"] * len(THROTTLE_FIELDS))
    with connect(database_url) as conn:
        conn.execute(
            f"INSERT INTO throttle_decisions ({columns}) VALUES ({placeholders}) ON CONFLICT (prompt_id) DO NOTHING",
            tuple(decision[field] for field in THROTTLE_FIELDS),
        )
        row = conn.execute(
            "SELECT * FROM throttle_decisions WHERE prompt_id = %s", (decision["prompt_id"],)
        ).fetchone()
    return serialize_throttle_decision(row)


def get_throttle_decision(database_url, prompt_id):
    with connect(database_url) as conn:
        row = conn.execute("SELECT * FROM throttle_decisions WHERE prompt_id = %s", (prompt_id,)).fetchone()
    return serialize_throttle_decision(row) if row else None


def get_active_source_blocks(database_url, now):
    """Return {source: blocked_until} for blocks still running at `now`, to restore P10 after a restart."""
    with connect(database_url) as conn:
        rows = conn.execute(
            """SELECT source, MAX(blocked_until) AS blocked_until
               FROM throttle_decisions
               WHERE burst_started AND blocked_until > %s
               GROUP BY source""",
            (now,),
        ).fetchall()
    return {row["source"]: row["blocked_until"] for row in rows}
