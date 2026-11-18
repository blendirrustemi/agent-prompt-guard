"""Run the agent on prompts the guard allowed.

Reads `prompts_allowed` from Kafka, re-checks the stored guard decision, runs the agent only for 'allow', and
records every outcome in agent_runs. Run from the project root: python -m backend.agent.worker [--once]
"""

import argparse
import json
import logging
import math
import os
import signal
import subprocess
import sys
import time
from functools import partial
from pathlib import Path
from dotenv import load_dotenv

from .. import db
from ..intake.topics import PROMPTS_ALLOWED, bootstrap_servers
from ..intake.events import validate_allowed_event
from ..readiness import mark_ready
from .agent import AgentAnswer, model_name

log = logging.getLogger("agent-worker")


def ask_in_subprocess(text, *, model, timeout=120):
    """Bound the entire model/tool loop and kill it before reporting a timeout."""
    try:
        process = subprocess.run(
            [sys.executable, "-m", "backend.agent.agent", "--model", model, "--json"],
            input=text, text=True, capture_output=True, timeout=timeout, check=False,
            cwd=Path(__file__).resolve().parents[2],
        )
    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Agent exceeded its {timeout:g}-second execution limit.") from None
    if process.returncode:
        # Child stderr can contain prompts/model output; do not expose it through the API.
        raise RuntimeError("Agent process failed; check Ollama connectivity and the configured model.")
    result = json.loads(process.stdout)
    return AgentAnswer(result["response"], result["tool_calls"])


def handle_message(database_url, event, ask_fn, model, timeout_seconds=120):
    """Process one `prompts_allowed` event; returns the stored agent run, or None if nothing was stored."""
    prompt_id = event["prompt_id"]
    if db.get_agent_run(database_url, prompt_id):
        log.info("%s already handled; skipping redelivery", prompt_id)
        return None
    # Run the stored text, not the message text, so the agent runs exactly what the guard checked.
    text = db.get_prompt_text(database_url, prompt_id)
    if text is None:
        log.warning("%s is not in the prompts table; skipping", prompt_id)
        return None

    guard = db.get_guard_result(database_url, prompt_id)
    decision = guard["decision"] if guard else "missing"
    if decision != "allow":
        log.warning("%s has stored decision '%s'; not running the agent", prompt_id, decision)
        return db.save_agent_run(
            database_url, prompt_id, "rejected", model=model,
            error=f"Stored guard decision is '{decision}', not 'allow'.",
        )

    token = db.claim_agent_run(database_url, prompt_id, model, timeout_seconds)
    if token is None:
        return None
    started = time.perf_counter()
    try:
        answer = ask_fn(text)
    except Exception as error:
        latency_ms = round((time.perf_counter() - started) * 1000)
        log.error("Agent failed on %s (%s)", prompt_id, type(error).__name__)
        return db.finish_agent_run(
            database_url, prompt_id, token, "failed", latency_ms=latency_ms,
            error=str(error) if isinstance(error, TimeoutError) else f"Agent execution failed ({type(error).__name__}).",
        )
    latency_ms = round((time.perf_counter() - started) * 1000)
    log.info("Answered %s in %d ms with %d tool call(s)", prompt_id, latency_ms, len(answer.tool_calls))
    return db.finish_agent_run(
        database_url, prompt_id, token, "completed", response=answer.response,
        tool_calls=answer.tool_calls, latency_ms=latency_ms,
    )


def run(database_url, ask_fn, model, servers=None, group_id="agent-worker", once=False, idle_seconds=5.0,
        timeout_seconds=120):
    """Consume `prompts_allowed` until stopped; with once=True, stop after idle_seconds without messages."""
    # Imported here so handle_message works (and is testable) without the Kafka client library.
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": servers or bootstrap_servers(),
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": int(max(300, timeout_seconds + 60) * 1000),
        "auto.offset.reset": "earliest",
    })
    # The idle timer starts only once Kafka has assigned the topic, since joining the group takes a few seconds.
    last_message = None

    def on_assign(consumer, partitions):
        nonlocal last_message
        last_message = time.monotonic()
        mark_ready("agent")

    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    if not once:
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
    consumer.subscribe([PROMPTS_ALLOWED], on_assign=on_assign)
    try:
        while running and not (once and last_message and time.monotonic() - last_message >= idle_seconds):
            db.expire_agent_runs(database_url)
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log.error("Kafka error: %s", message.error())
                continue
            try:
                event = validate_allowed_event(json.loads(message.value()))
            except (ValueError, TypeError, UnicodeDecodeError):
                log.error("Skipping an invalid allowed event at offset %s", message.offset())
            else:
                handle_message(database_url, event, ask_fn, model, timeout_seconds)
            # Commit only after the run is stored; a crash before this means redelivery, which is skipped above.
            consumer.commit(message=message, asynchronous=False)
            # Measured after handling, so a slow model answer does not count as idle time.
            last_message = time.monotonic()
    finally:
        mark_ready("agent", False)
        consumer.close()


def main():
    mark_ready("agent", False)
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run the agent on prompts from prompts_allowed.")
    parser.add_argument("--once", action="store_true", help="Stop once no message has arrived for a few seconds")
    parser.add_argument("--model", help="Ollama model to use (default: OLLAMA_MODEL from .env)")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not configured.")
    model = model_name(args.model)
    timeout = float(os.getenv("AGENT_TIMEOUT_SECONDS", "120"))
    if not math.isfinite(timeout) or timeout <= 0:
        raise SystemExit("AGENT_TIMEOUT_SECONDS must be finite and positive.")
    # Check that the configured model exists before advertising startup readiness.
    from ollama import Client
    Client(host=os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434"), timeout=10).show(model)
    log.info("Agent worker using %s; waiting for %s", model, PROMPTS_ALLOWED)
    run(database_url, partial(ask_in_subprocess, model=model, timeout=timeout), model,
        once=args.once, timeout_seconds=timeout)


if __name__ == "__main__":
    main()
