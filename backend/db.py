"""Small PostgreSQL helpers; each submission uses a single transaction."""

from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row


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


def save_prompt(database_url, text):
    prompt_id = uuid4()
    with connect(database_url) as conn:
        row = conn.execute(
            "INSERT INTO prompts (id, text) VALUES (%s, %s) RETURNING id, text, created_at",
            (prompt_id, text),
        ).fetchone()
        conn.execute("INSERT INTO guard_results (prompt_id) VALUES (%s)", (prompt_id,))
    # The context manager commits both inserts together before reporting success.
    row["decision"] = "pending"
    return serialize_prompt(row)


def list_prompts(database_url):
    with connect(database_url) as conn:
        rows = conn.execute(
            """SELECT p.id, p.text, p.created_at, g.decision
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
