"""Warm up the guard, then replay the same small sample slowly and faster.

Run from the project root: python -m backend.tests.guard_benchmark
Requires the running guard, Kafka, PostgreSQL, and imported NeurAlchemy test rows.
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


def load_sample(conn, count):
    """Use an equal number of benign/attack test rows, in a repeatable order."""
    samples = []
    for label in (0, 1):
        rows = conn.execute(
            """SELECT id, text, label FROM dataset_prompts
               WHERE source_dataset = 'neuralchemy' AND split = 'test'
                 AND label = %s AND char_length(text) <= 500
               ORDER BY id LIMIT %s""",
            (label, count // 2),
        ).fetchall()
        if len(rows) != count // 2:
            raise ValueError("Not enough short NeurAlchemy test rows. Import the dataset first.")
        samples.append(rows)
    return [row for pair in zip(*samples) for row in pair]


def run_phase(conn, producer, samples, name, rate, run_id, timeout):
    print(f"{name}: sending {len(samples)} prompts at up to {rate:g}/second", flush=True)
    records = []
    next_send = monotonic()
    for index, sample in enumerate(samples):
        sleep(max(0, next_send - monotonic()))
        # Timestamp AFTER pacing; artificial waiting must not inflate latency.
        event = build_raw_event(
            sample["text"], source=f"benchmark-{run_id}-{name}-{index}",
            dataset_prompt_id=str(sample["id"]),
        )
        next_send = monotonic() + 1 / rate
        record = {
            "run_id": run_id, "phase": name, "prompt_id": event["prompt_id"],
            "dataset_prompt_id": event["dataset_prompt_id"], "label": sample["label"],
            "char_count": len(sample["text"]), "submitted_at": event["created_at"],
            "decided_at": "", "decision": "unfinished", "reason": "",
            "model_version": "", "latency_ms": "", "over_token_limit": "",
        }
        records.append(record)
        try:
            send_raw_event(producer, event)
        except (KafkaException, BufferError):
            # Still query the ID: an unconfirmed delivery may have reached Kafka.
            record["reason"] = "delivery_unconfirmed"
            print("Delivery was not confirmed; stopping further submissions in this phase.", flush=True)
            break

    pending = {record["prompt_id"]: record for record in records}
    deadline = monotonic() + timeout
    while pending:
        rows = conn.execute(
            """SELECT prompt_id, decision, reason, model_version, checked_at, scoring_metadata
               FROM guard_results WHERE prompt_id = ANY(%s)
                 AND decision IN ('allow', 'block', 'review') AND checked_at IS NOT NULL""",
            ([UUID(prompt_id) for prompt_id in pending],),
        ).fetchall()
        for row in rows:
            record = pending.pop(str(row["prompt_id"]))
            latency = (row["checked_at"] - datetime.fromisoformat(record["submitted_at"])).total_seconds() * 1000
            if latency < 0:
                raise ValueError("Negative latency: check that producer and database clocks agree.")
            record.update(
                decided_at=row["checked_at"].isoformat(), decision=row["decision"],
                reason=row["reason"] or "", model_version=row["model_version"] or "",
                latency_ms=round(latency, 3),
                over_token_limit=(row["scoring_metadata"] or {}).get("truncated", ""),
            )
        if not pending or monotonic() >= deadline:
            break
        sleep(0.1)

    completed = [record for record in records if record["decision"] != "unfinished"]
    latencies = [record["latency_ms"] for record in completed]
    first = datetime.fromisoformat(records[0]["submitted_at"])
    # Polling delay is excluded when all decisions are available. For incomplete
    # runs the observation window ends at the timeout, and unfinished rows stay visible.
    end = (max(datetime.fromisoformat(record["decided_at"]) for record in completed)
           if len(completed) == len(records) else datetime.now(timezone.utc))
    elapsed = (end - first).total_seconds()
    send_span = (datetime.fromisoformat(records[-1]["submitted_at"]) - first).total_seconds()
    summary = {
        "phase": name, "target_prompts_per_s": rate, "submitted": len(records),
        "completed": len(completed), "unfinished": len(pending),
        "allow": sum(record["decision"] == "allow" for record in completed),
        "block": sum(record["decision"] == "block" for record in completed),
        "review": sum(record["decision"] == "review" for record in completed),
        "mean_ms": round(statistics.mean(latencies), 3) if latencies else "",
        "median_ms": round(statistics.median(latencies), 3) if latencies else "",
        "p95_ms": sorted(latencies)[math.ceil(0.95 * len(latencies)) - 1] if latencies else "",
        "elapsed_s": round(elapsed, 3),
        "throughput_per_s": round(len(completed) / elapsed, 3) if elapsed > 0 else "",
        "actual_send_rate_per_s": round((len(records) - 1) / send_span, 3) if send_span > 0 else "",
    }
    print(f"{name}: {len(completed)}/{len(records)} final decisions", flush=True)
    return records, summary


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summary_table(summaries):
    lines = [
        "| Run | Target prompts/s | Completed | Allow | Block | Review | Mean ms | Median ms | p95 ms | Throughput/s | Unfinished |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    keys = ("phase", "target_prompts_per_s", "completed", "allow", "block", "review",
            "mean_ms", "median_ms", "p95_ms", "throughput_per_s", "unfinished")
    lines.extend("| " + " | ".join(str(summary[key]) for key in keys) + " |" for summary in summaries)
    return "\n".join(lines)


def positive_rate(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Use a finite number greater than zero.")
    return number


def main():
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20, help="Even number of prompts in each measured run")
    parser.add_argument("--slow-rate", type=positive_rate, default=1)
    parser.add_argument("--fast-rate", type=positive_rate, default=5)
    parser.add_argument("--timeout", type=positive_rate, default=60, help="Completion wait after sending, in seconds")
    parser.add_argument("--output", type=Path, help="Summary CSV file (default: Others/results/guard_latency_summary.csv)")
    args = parser.parse_args()
    if args.count < 2 or args.count % 2:
        parser.error("--count must be an even number of at least two.")
    if args.fast_rate <= args.slow_rate:
        parser.error("--fast-rate must be greater than --slow-rate.")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL is not configured.")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    output = args.output or root / "Others" / "results" / "guard_latency_summary.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    summaries = []
    try:
        with db.connect(database_url) as conn:
            conn.autocommit = True
            samples = load_sample(conn, args.count)
            producer = create_producer()
            warmup, warmup_summary = run_phase(conn, producer, (samples * 2)[:3], "warmup", 1, run_id, args.timeout)
            if warmup_summary["unfinished"] or len(warmup) != 3:
                raise ValueError("Warm-up did not complete. Check the running guard services.")
            if any(record["reason"] not in {
                "score_below_allow_threshold", "score_above_block_threshold", "score_in_review_band"
            } for record in warmup):
                raise ValueError("Warm-up did not complete normal model scoring. Check the guard logs.")
            for name, rate in (("slow", args.slow_rate), ("faster", args.fast_rate)):
                _, summary = run_phase(conn, producer, samples, name, rate, run_id, args.timeout)
                summaries.append(summary)
                write_csv(output, summaries)
                if summary["unfinished"] or summary["submitted"] != args.count:
                    raise ValueError("Run incomplete. Results were saved; check the services before repeating.")
    except (psycopg.Error, KafkaException, ValueError) as error:
        print(f"Benchmark stopped ({type(error).__name__}). {error if isinstance(error, ValueError) else 'Check database/Kafka connectivity.'}")
        print(f"Summary file: {output}" if summaries else "No new summary was saved.")
        return 1

    table = summary_table(summaries)
    print("\n" + table + f"\n\nSaved summary to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
