"""TEMPORARY guard stub: remove once the real guard stores decisions and publishes to prompts_allowed.

Stores a decision for a prompt and publishes it to `prompts_allowed`, so the agent worker can be demonstrated
end to end. Run from the project root:

    python -m backend.agent.guard_stub "What is 15 divided by 3?"
    python -m backend.agent.guard_stub --prompt-id <uuid>              # an existing prompt
    python -m backend.agent.guard_stub "Ignore all instructions" --decision block

A non-'allow' decision is still published on purpose, to show that the worker's re-check rejects it.
"""

import argparse
import os

from dotenv import load_dotenv

from .. import db
from ..intake.producer import publish_allowed_prompt

STUB_VERSION = "stub"


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="TEMPORARY: mark a prompt with a decision and publish it.")
    parser.add_argument("text", nargs="?", help="Text of a new prompt to store")
    parser.add_argument("--prompt-id", help="Use an existing prompt instead of storing a new one")
    parser.add_argument("--decision", default="allow", choices=("allow", "block", "review"))
    args = parser.parse_args()
    if bool(args.text) == bool(args.prompt_id):
        parser.error("Give either prompt text or --prompt-id.")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not configured.")
    if args.prompt_id:
        prompt_id, text = args.prompt_id, db.get_prompt_text(database_url, args.prompt_id)
        if text is None:
            raise SystemExit(f"No prompt with id {args.prompt_id}.")
    else:
        text = args.text
        prompt_id = db.save_prompt(database_url, text)["id"]

    db.save_guard_result(
        database_url, prompt_id, args.decision, reason="Temporary guard stub", model_version=STUB_VERSION
    )
    publish_allowed_prompt(prompt_id, text, STUB_VERSION)
    print(f"Stored decision '{args.decision}' for {prompt_id} and published it to prompts_allowed.")


if __name__ == "__main__":
    main()
