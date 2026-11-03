"""Run the agent on prompts the guard allowed.

Reads `prompts_allowed` from Kafka, re-checks the stored guard decision, runs the agent only for 'allow', and
records every outcome in agent_runs. Run from the project root: python -m backend.agent.worker [--once]
"""

import argparse
import json
import logging
import os
import time
from dotenv import load_dotenv

from .. import db
from ..intake.topics import PROMPTS_ALLOWED, bootstrap_servers
from .agent import ask, build_agent, model_name

log = logging.getLogger("agent-worker")


def handle_message(database_url, event, ask_fn, model):
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

    started = time.perf_counter()
    try:
        answer = ask_fn(text)
    except Exception as error:
        latency_ms = round((time.perf_counter() - started) * 1000)
        log.exception("Agent failed on %s", prompt_id)
        return db.save_agent_run(
            database_url, prompt_id, "failed", latency_ms=latency_ms, model=model,
            error=f"{type(error).__name__}: {error}",
        )
    latency_ms = round((time.perf_counter() - started) * 1000)
    log.info("Answered %s in %d ms with %d tool call(s)", prompt_id, latency_ms, len(answer.tool_calls))
    return db.save_agent_run(
        database_url, prompt_id, "completed", response=answer.response,
        tool_calls=answer.tool_calls, latency_ms=latency_ms, model=model,
    )


def run(database_url, ask_fn, model, servers=None, group_id="agent-worker", once=False, idle_seconds=5.0):
    """Consume `prompts_allowed` until stopped; with once=True, stop after idle_seconds without messages."""
    # Imported here so handle_message works (and is testable) without the Kafka client library.
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": servers or bootstrap_servers(),
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    # The idle timer starts only once Kafka has assigned the topic, since joining the group takes a few seconds.
    last_message = None

    def on_assign(consumer, partitions):
        nonlocal last_message
        last_message = time.monotonic()

    consumer.subscribe([PROMPTS_ALLOWED], on_assign=on_assign)
    try:
        while not (once and last_message and time.monotonic() - last_message >= idle_seconds):
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log.error("Kafka error: %s", message.error())
                continue
            try:
                event = json.loads(message.value())
            except ValueError:
                log.error("Skipping a message that is not JSON at offset %s", message.offset())
            else:
                handle_message(database_url, event, ask_fn, model)
            # Commit only after the run is stored; a crash before this means redelivery, which is skipped above.
            consumer.commit(message=message, asynchronous=False)
            # Measured after handling, so a slow model answer does not count as idle time.
            last_message = time.monotonic()
    finally:
        consumer.close()


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run the agent on prompts from prompts_allowed.")
    parser.add_argument("--once", action="store_true", help="Stop once no message has arrived for a few seconds")
    parser.add_argument("--model", help="Ollama model to use (default: OLLAMA_MODEL from .env)")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not configured.")
    model = model_name(args.model)
    agent = build_agent(model)
    log.info("Agent worker using %s; waiting for %s", model, PROMPTS_ALLOWED)
    run(database_url, lambda text: ask(agent, text), model, once=args.once)


if __name__ == "__main__":
    main()
