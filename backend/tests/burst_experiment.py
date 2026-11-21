"""Burst experiment: one simulated IP floods the guard, P9 detects it and P10 blocks it.

A control source first sends a few prompts at a normal rate, then an attacker source sends many
prompts quickly. All prompts are intended to be benign; P4 may still score some as malicious, so
the final P7 reason shows whether a block came from throttling (`throttle_*`) or the model score.
The script checks the stored P9/P10 decisions and the final P7 routing, prints a timeline, and
writes per-prompt and per-source CSV files.

Run from the project root: python -m backend.tests.burst_experiment
Requires the running guard services (burst worker, consumer, router), Kafka and PostgreSQL.
"""

import argparse
import csv
import math
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import psycopg
from confluent_kafka import KafkaException
from dotenv import load_dotenv

from backend import db
from backend.intake.events import build_raw_event
from backend.intake.producer import create_producer, send_raw_event

ATTACKER_IP, CONTROL_IP = "198.51.100.66", "198.51.100.10"  # documentation range (RFC 5737)
BENIGN_PROMPTS = (
    "What is 12 times 7?",
    "What is 144 divided by 12?",
    "Who owns the burst detection stage?",
    "What is 250 plus 175?",
    "When is the final submission deadline?",
    "What is 81 minus 19?",
    "Which datasets does the project use?",
    "What is 9 times 9?",
)
THROTTLE_REASONS = {"throttle_burst_detected", "throttle_source_blocked"}


def send_phase(producer, role, source, count, rate, start):
    """Send `count` benign prompts from one source at up to `rate` per second."""
    print(f"{role}: sending {count} prompts from {source} at up to {rate:g}/second", flush=True)
    records = []
    next_send = monotonic()
    for index in range(count):
        sleep(max(0, next_send - monotonic()))
        # Timestamp AFTER pacing, so the event time is the real send time.
        event = build_raw_event(BENIGN_PROMPTS[index % len(BENIGN_PROMPTS)], source=source)
        next_send = monotonic() + 1 / rate
        send_raw_event(producer, event)
        records.append({
            "role": role, "index": index + 1, "source": source, "prompt_id": event["prompt_id"],
            "sent_offset_s": round((datetime.fromisoformat(event["created_at"]) - start).total_seconds(), 3),
        })
    return records


def collect(conn, records, timeout):
    """Wait until every prompt has a P10 decision and a final P7 decision, or the timeout passes."""
    ids = [UUID(record["prompt_id"]) for record in records]
    deadline = monotonic() + timeout
    while True:
        rows = conn.execute(
            """SELECT p.id, t.action, t.reason, t.window_count, t.window_seconds, t.threshold,
                      t.blocked_until, t.burst_started, t.event_time, t.decided_at AS throttled_at,
                      g.decision, g.reason AS final_reason, g.malicious_score
               FROM prompts p
               LEFT JOIN throttle_decisions t ON t.prompt_id = p.id
               LEFT JOIN guard_results g ON g.prompt_id = p.id
               WHERE p.id = ANY(%s)""",
            (ids,),
        ).fetchall()
        by_id = {str(row["id"]): row for row in rows}
        done = [
            prompt_id for prompt_id, row in by_id.items()
            if row["action"] is not None and row["decision"] not in (None, "pending")
        ]
        if len(done) == len(records) or monotonic() >= deadline:
            break
        sleep(0.1)

    for record in records:
        row = by_id.get(record["prompt_id"])
        throttled = row is not None and row["action"] is not None
        record.update({
            "p9_window_count": row["window_count"] if throttled else "",
            "p10_action": row["action"] if throttled else "missing",
            "p10_reason": (row["reason"] or "") if throttled else "",
            "burst_started": row["burst_started"] if throttled else "",
            "blocked_until": row["blocked_until"].isoformat() if throttled and row["blocked_until"] else "",
            "p10_latency_ms": round((row["throttled_at"] - row["event_time"]).total_seconds() * 1000, 3) if throttled else "",
            "p7_decision": row["decision"] if row is not None and row["decision"] else "missing",
            "p7_reason": (row["final_reason"] or "") if row is not None else "",
            "p4_score": round(row["malicious_score"], 4) if row is not None and row["malicious_score"] is not None else "",
            "_threshold": row["threshold"] if throttled else None,
            "_window": row["window_seconds"] if throttled else None,
            "_cooldown": (row["blocked_until"] - row["event_time"]).total_seconds()
            if throttled and row["burst_started"] else None,
        })
    return records


def check(records):
    """Return [(passed, description)] for the expected P9/P10/P7 behaviour."""
    attacker = [r for r in records if r["role"] == "attacker"]
    control = [r for r in records if r["role"] == "control"]
    threshold = next((r["_threshold"] for r in records if r["_threshold"]), None)
    if threshold is None:
        return [(False, "No P10 decisions were stored; is the burst worker running?")]
    if len(attacker) <= threshold:
        return [(False, f"Send more than {threshold} attacker prompts to observe a block.")]

    before, trigger, after = attacker[:threshold - 1], attacker[threshold - 1], attacker[threshold:]
    blocked = [r for r in records if r["p10_action"] == "block"]
    return [
        (all(r["p10_action"] == "allow" and r["p9_window_count"] == r["index"] for r in before),
         f"P9 counts attacker prompts 1-{threshold - 1} (window_count 1-{threshold - 1}) and P10 allows them"),
        (trigger["p10_action"] == "block" and trigger["p10_reason"] == "burst_detected" and trigger["burst_started"] is True,
         f"Attacker prompt #{threshold} reaches the threshold: P9 detects the burst and P10 starts a block"),
        (all(r["p10_action"] == "block" and r["p10_reason"] == "source_blocked"
             and r["blocked_until"] == trigger["blocked_until"] for r in after),
         f"Attacker prompts #{threshold + 1}-{len(attacker)} stay blocked during the cooldown (same blocked_until)"),
        (all(r["p7_decision"] == "block" and r["p7_reason"] in THROTTLE_REASONS for r in blocked),
         "P7 finalizes every P10-blocked prompt as block with a throttle reason"),
        (all(r["p10_action"] == "allow" and r["p7_reason"] not in THROTTLE_REASONS for r in control),
         "The control source at a normal rate is never throttled"),
    ]


def summarize(records, role):
    rows = [r for r in records if r["role"] == role]
    latencies = [r["p10_latency_ms"] for r in rows if r["p10_latency_ms"] != ""]
    first_block = next((r["index"] for r in rows if r["p10_action"] == "block"), "")
    cooldown = next((r["_cooldown"] for r in rows if r["_cooldown"] is not None), "")
    return {
        "role": role, "source": rows[0]["source"], "sent": len(rows),
        "p10_allow": sum(r["p10_action"] == "allow" for r in rows),
        "p10_block": sum(r["p10_action"] == "block" for r in rows),
        "first_blocked_index": first_block,
        "threshold": next((r["_threshold"] for r in rows if r["_threshold"]), ""),
        "window_s": next((r["_window"] for r in rows if r["_window"]), ""),
        "cooldown_s": cooldown,
        "p7_allow": sum(r["p7_decision"] == "allow" for r in rows),
        "p7_block": sum(r["p7_decision"] == "block" for r in rows),
        "p7_review": sum(r["p7_decision"] == "review" for r in rows),
        "mean_p10_latency_ms": round(statistics.mean(latencies), 3) if latencies else "",
        "max_p10_latency_ms": max(latencies) if latencies else "",
    }


PROMPT_COLUMNS = (
    "role", "index", "source", "prompt_id", "sent_offset_s", "p9_window_count", "p10_action", "p10_reason",
    "burst_started", "blocked_until", "p10_latency_ms", "p7_decision", "p7_reason", "p4_score",
)


def write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def table(rows, columns):
    lines = ["| " + " | ".join(columns) + " |", "|" + " --- |" * len(columns)]
    lines.extend("| " + " | ".join(str(row[c]) for c in columns) + " |" for row in rows)
    return "\n".join(lines)


def positive(kind):
    def parse(value):
        number = kind(value)
        if (isinstance(number, float) and not math.isfinite(number)) or number <= 0:
            raise argparse.ArgumentTypeError("Use a number greater than zero.")
        return number
    return parse


def main():
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=positive(int), default=20, help="Attacker prompts (default: 20)")
    parser.add_argument("--rate", type=positive(float), default=10, help="Attacker prompts/second (default: 10)")
    parser.add_argument("--control-count", type=positive(int), default=5, help="Control prompts (default: 5)")
    parser.add_argument("--control-rate", type=positive(float), default=1, help="Control prompts/second (default: 1)")
    parser.add_argument("--timeout", type=positive(float), default=60, help="Wait for decisions after sending, in seconds")
    parser.add_argument("--output-dir", type=Path, default=root / "results", help="Directory for the CSV files")
    args = parser.parse_args()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL is not configured.")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    # Per-run sources, so a cooldown left over from an earlier run cannot affect this one.
    attacker_source, control_source = f"burst-{run_id}-{ATTACKER_IP}", f"burst-{run_id}-{CONTROL_IP}"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        producer = create_producer()
        start = datetime.now(timezone.utc)
        records = send_phase(producer, "control", control_source, args.control_count, args.control_rate, start)
        records += send_phase(producer, "attacker", attacker_source, args.count, args.rate, start)
        print(f"Waiting up to {args.timeout:g}s for P10 and P7 decisions...", flush=True)
        with db.connect(database_url) as conn:
            conn.autocommit = True
            collect(conn, records, args.timeout)
    except (psycopg.Error, KafkaException, BufferError) as error:
        print(f"Experiment stopped ({type(error).__name__}). Check database/Kafka connectivity and the guard services.")
        return 1

    summaries = [summarize(records, "control"), summarize(records, "attacker")]
    write_csv(args.output_dir / "burst_experiment_prompts.csv", records, PROMPT_COLUMNS)
    write_csv(args.output_dir / "burst_experiment_summary.csv", summaries, summaries[0].keys())

    timeline = ("role", "index", "sent_offset_s", "p9_window_count", "p10_action", "p10_reason",
                "p10_latency_ms", "p7_decision", "p7_reason")
    print(f"\nRun {run_id}\n\n" + table(records, timeline))
    print("\n" + table(summaries, list(summaries[0])))
    results = check(records)
    print()
    for passed, description in results:
        print(f"{'PASS' if passed else 'FAIL'}  {description}")
    print(f"\nSaved {args.output_dir / 'burst_experiment_prompts.csv'} and {args.output_dir / 'burst_experiment_summary.csv'}")
    return 0 if all(passed for passed, _ in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
