"""Replay imported dataset rows through the same raw-prompt path as the browser."""

import argparse
import math
import sys
from pathlib import Path
from time import monotonic, sleep
import os

import psycopg
from confluent_kafka import KafkaException
from dotenv import load_dotenv

from backend import db
from .events import build_raw_event
from .producer import create_producer, send_raw_event


def select_rows(database_url, dataset, split, limit, label=None, attack_group=None):
    conditions = ["source_dataset = %s", "split = %s"]
    parameters = [dataset, split]
    if label is not None:
        conditions.append("label = %s")
        parameters.append(label)
    if attack_group is not None:
        conditions.append("attack_group = %s")
        parameters.append(attack_group)
    parameters.append(limit)
    with db.connect(database_url) as conn:
        return conn.execute(
            "SELECT id, text FROM dataset_prompts WHERE " + " AND ".join(conditions)
            + " ORDER BY id LIMIT %s", parameters,
        ).fetchall()


def replay_rows(rows, *, rate=5, sources=("192.0.2.1",), clock=monotonic, wait=sleep):
    """Return an exit code; confirm each send and never catch up with a sudden burst."""
    acknowledged = skipped = 0
    producer = None
    next_send = None
    try:
        for row in rows:
            try:
                event = build_raw_event(
                    row["text"], source=sources[acknowledged % len(sources)],
                    dataset_prompt_id=row["id"],
                )
            except ValueError as error:
                print(f"Skipped dataset row {row['id']}: {error}", file=sys.stderr)
                skipped += 1
                continue
            if next_send is not None:
                delay = next_send - clock()
                if delay > 0:
                    wait(delay)
            started = clock()
            try:
                if producer is None:
                    producer = create_producer()
                send_raw_event(producer, event)
            except (KafkaException, BufferError) as error:
                print(
                    f"Could not confirm delivery of prompt {event['prompt_id']} "
                    f"(dataset row {row['id']}): {type(error).__name__}. Stopping.",
                    file=sys.stderr,
                )
                return 1
            acknowledged += 1
            next_send = started + 1 / rate if rate else None
        return 0 if acknowledged else 1
    finally:
        print(f"Selected: {len(rows)}; acknowledged: {acknowledged}; skipped: {skipped}")


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Must be a positive integer.")
    return number


def replay_rate(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("Rate must be finite and non-negative (0 means no delay).")
    return number


def source_list(value):
    sources = tuple(part.strip() for part in value.split(","))
    if any(not source or "\x00" in source for source in sources):
        raise argparse.ArgumentTypeError("Sources must be non-empty identifiers without null characters.")
    return sources


def nonblank(value):
    if not value.strip() or "\x00" in value:
        raise argparse.ArgumentTypeError("Must be non-blank text without null characters.")
    return value


def main(argv=None):
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=("neuralchemy", "deepset", "jackhhao"))
    parser.add_argument("--split", required=True, type=nonblank)
    parser.add_argument("--limit", type=positive_int, default=100)
    parser.add_argument("--rate", type=replay_rate, default=5, help="Prompts/second; 0 disables delays")
    parser.add_argument("--sources", type=source_list, default="192.0.2.1", help="Comma-separated source identifiers")
    parser.add_argument("--label", type=int, choices=(0, 1))
    parser.add_argument("--attack-group", type=nonblank)
    args = parser.parse_args(argv)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is not configured.", file=sys.stderr)
        return 1
    try:
        rows = select_rows(database_url, args.dataset, args.split, args.limit, args.label, args.attack_group)
        return replay_rows(rows, rate=args.rate, sources=args.sources)
    except psycopg.Error as error:
        print(f"Replay stopped: database unavailable ({type(error).__name__}).", file=sys.stderr)
        print("Selected: 0; acknowledged: 0; skipped: 0")
        return 1
    except KeyboardInterrupt:
        print("Replay interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
