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
        "p3_similarity_score": row.get("p3_similarity_score"),
        "anomaly_score": row.get("anomaly_score"),
        "p5_status": row.get("p5_status"),
        "p5_feature_version": row.get("p5_feature_version"),
        "p5_processed_at": row["p5_processed_at"].isoformat() if row.get("p5_processed_at") else None,
        "p5_sequence": row.get("p5_sequence"),
        "checked_at": row["checked_at"].isoformat() if row["checked_at"] else None,
        "scored_at": row["scored_at"].isoformat() if row.get("scored_at") else None,
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


def get_similarity_training_rows(database_url):
    """P3 references: only malicious training rows, never evaluation/feedback labels."""
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT id, text FROM dataset_prompts
               WHERE source_dataset = 'neuralchemy' AND split = 'train' AND label = 1
               ORDER BY id"""
        ).fetchall()


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


def save_guard_score(
    database_url, prompt_id, malicious_score, attack_type, model_version, reason=None, checked_at=None,
    similarity_score=None,
):
    """Persist P4 output while leaving the final allow/block/review choice to P7."""
    with connect(database_url) as conn:
        row = conn.execute(
            """UPDATE guard_results
               SET malicious_score = %s, attack_type = %s, p3_similarity_score = %s,
                   reason = CASE WHEN decision = 'pending' THEN %s ELSE reason END,
                   model_version = %s, scored_at = COALESCE(%s, NOW()),
                   checked_at = CASE WHEN decision = 'pending' THEN COALESCE(%s, NOW()) ELSE checked_at END
               WHERE prompt_id = %s
               RETURNING *""",
            (malicious_score, attack_type, similarity_score, reason, model_version, checked_at, checked_at, prompt_id),
        ).fetchone()
    return serialize_guard_result(row) if row else None


def get_p5_input(database_url, prompt_id):
    """Read persisted P1/P3/P4 inputs and P5 idempotency state for one prompt."""
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT p.preprocessing, g.malicious_score, g.p3_similarity_score,
                      g.anomaly_score, g.p5_status, g.p5_feature_version
               FROM prompts p JOIN guard_results g ON g.prompt_id = p.id
               WHERE p.id = %s""",
            (prompt_id,),
        ).fetchone()


def get_p5_rebuild_rows(database_url, feature_version, window_size):
    """Return the recent feature tail needed to rebuild P5 and the total trained count."""
    with connect(database_url) as conn:
        total = conn.execute(
            """SELECT count(*) AS n FROM guard_results
               WHERE p5_feature_version = %s AND p5_sequence IS NOT NULL""",
            (feature_version,),
        ).fetchone()["n"]
        # HST carries two rolling windows. Include the current window's offset so a
        # restart resumes with the same window phase.
        limit = min(total, 2 * window_size + (total % window_size))
        rows = conn.execute(
            """SELECT p.preprocessing, g.malicious_score, g.p3_similarity_score
               FROM guard_results g JOIN prompts p ON p.id = g.prompt_id
               WHERE g.p5_feature_version = %s AND g.p5_sequence IS NOT NULL
               ORDER BY g.p5_sequence DESC
               LIMIT %s""",
            (feature_version, limit),
        ).fetchall()
    rows.reverse()
    return {"sample_count": total, "rows": rows}


def save_p5_result(database_url, prompt_id, anomaly_score, status, feature_version):
    """Save P5 once; return False when this prompt already has this feature version."""
    with connect(database_url) as conn:
        row = conn.execute(
            """UPDATE guard_results
               SET anomaly_score = %s, p5_status = %s, p5_feature_version = %s,
                   p5_processed_at = NOW(), p5_sequence = nextval('p5_event_sequence')
               WHERE prompt_id = %s
                 AND p5_feature_version IS DISTINCT FROM %s
               RETURNING prompt_id""",
            (anomaly_score, status, feature_version, prompt_id, feature_version),
        ).fetchone()
    return row is not None


def route_guard_result(database_url, prompt_id, decision, reason):
    """Finalize one pending guard result and enqueue allowed prompts atomically."""
    if decision not in {"allow", "block", "review"}:
        raise ValueError("A final guard decision must be allow, block, or review.")
    with connect(database_url) as conn:
        row = conn.execute(
            """UPDATE guard_results
               SET decision = %s, reason = %s, checked_at = NOW()
               WHERE prompt_id = %s AND decision = 'pending'
               RETURNING prompt_id""",
            (decision, reason, prompt_id),
        ).fetchone()
        if row and decision == "allow":
            conn.execute(
                """INSERT INTO allowed_prompt_outbox (prompt_id)
                   VALUES (%s) ON CONFLICT (prompt_id) DO NOTHING""",
                (prompt_id,),
            )
    return bool(row)


def get_pending_routes(database_url, *, ready_before):
    """List P10 blocks and allows ready for P7 routing or past their score deadline."""
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT p.id AS prompt_id, p.text, t.action AS throttle_action,
                      t.reason AS throttle_reason, t.decided_at,
                      g.malicious_score, g.attack_type, g.model_version, g.reason,
                      g.checked_at
               FROM throttle_decisions t
               JOIN guard_results g ON g.prompt_id = t.prompt_id
               JOIN prompts p ON p.id = t.prompt_id
               WHERE g.decision = 'pending'
                 AND (t.action = 'block' OR (t.action = 'allow'
                   AND (g.malicious_score IS NOT NULL OR t.decided_at <= %s)))
               ORDER BY t.decided_at, t.prompt_id""",
            (ready_before,),
        ).fetchall()


def get_unpublished_allowed_prompts(database_url, limit=100):
    """Read the durable allowed-prompt outbox for at-least-once Kafka publishing."""
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT o.prompt_id, p.text, g.model_version
               FROM allowed_prompt_outbox o
               JOIN prompts p ON p.id = o.prompt_id
               JOIN guard_results g ON g.prompt_id = o.prompt_id
               WHERE o.published_at IS NULL AND g.decision = 'allow'
               ORDER BY o.created_at, o.prompt_id
               LIMIT %s""",
            (limit,),
        ).fetchall()


def mark_allowed_prompts_published(database_url, prompt_ids):
    if not prompt_ids:
        return 0
    with connect(database_url) as conn:
        result = conn.execute(
            """UPDATE allowed_prompt_outbox SET published_at = NOW()
               WHERE prompt_id = ANY(%s) AND published_at IS NULL""",
            (list(prompt_ids),),
        )
    return result.rowcount


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


def get_attack_window_rows(database_url, window_start, window_end):
    """Fetch completed P4 classifications in the event-time interval (start, end]."""
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT p.id AS prompt_id, p.text, p.preprocessing,
                      p.created_at, g.attack_type
               FROM prompts p
               JOIN guard_results g ON g.prompt_id = p.id
               WHERE p.created_at > %s AND p.created_at <= %s
                 AND g.malicious_score IS NOT NULL
                 AND g.attack_type IS NOT NULL
               ORDER BY p.created_at, p.id""",
            (window_start, window_end),
        ).fetchall()


def save_attack_aggregate_snapshot(database_url, snapshot, *, history_before=None):
    """Atomically replace the current P8 snapshot and append its history record."""
    fields = (
        snapshot["snapshot_at"], snapshot["window_start"], snapshot["window_end"],
        Jsonb(snapshot["attack_type_counts"]), Jsonb(snapshot["top_patterns"]),
    )
    with connect(database_url) as conn:
        conn.execute(
            """INSERT INTO attack_aggregate_history
                   (snapshot_at, window_start, window_end, attack_type_counts, top_patterns)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (snapshot_at) DO NOTHING""",
            fields,
        )
        if history_before is not None:
            conn.execute(
                "DELETE FROM attack_aggregate_history WHERE snapshot_at < %s",
                (history_before,),
            )
        conn.execute(
            """INSERT INTO attack_aggregate_current
                   (id, snapshot_at, window_start, window_end, attack_type_counts, top_patterns)
               VALUES (1, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET
                   snapshot_at = EXCLUDED.snapshot_at,
                   window_start = EXCLUDED.window_start,
                   window_end = EXCLUDED.window_end,
                   attack_type_counts = EXCLUDED.attack_type_counts,
                   top_patterns = EXCLUDED.top_patterns
               WHERE attack_aggregate_current.snapshot_at <= EXCLUDED.snapshot_at""",
            fields,
        )


def prune_attack_aggregate_history(database_url, before):
    with connect(database_url) as conn:
        result = conn.execute(
            "DELETE FROM attack_aggregate_history WHERE snapshot_at < %s", (before,)
        )
    return result.rowcount


def get_current_attack_aggregate(database_url):
    with connect(database_url) as conn:
        return conn.execute("SELECT * FROM attack_aggregate_current WHERE id = 1").fetchone()


def get_attack_aggregate_history(database_url, limit=1000):
    with connect(database_url) as conn:
        return conn.execute(
            """SELECT snapshot_at, window_start, window_end, attack_type_counts, top_patterns
               FROM attack_aggregate_history ORDER BY snapshot_at DESC LIMIT %s""",
            (limit,),
        ).fetchall()
