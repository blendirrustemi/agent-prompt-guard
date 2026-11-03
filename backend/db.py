"""Small PostgreSQL helpers; each submission uses a single transaction."""

from pathlib import Path
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


def save_raw_event(database_url, event):
    """Store a `prompts_raw` Kafka event; every event field maps onto an existing prompts column."""
    return save_prompt(
        database_url,
        event["text"],
        dataset_prompt_id=event["dataset_prompt_id"],
        source=event["source"],
        prompt_id=event["prompt_id"],
        created_at=event["created_at"],
    )


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
