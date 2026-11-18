"""Submit a prompt through HTTP and wait for its guarded agent answer."""

import argparse
import json
import math
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

TERMINAL_STATES = {"completed", "blocked", "review", "agent_failed"}


def request_json(url, body=None, timeout=10):
    request = Request(url, data=json.dumps(body).encode() if body is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def wait_for_result(base_url, prompt_id, *, timeout=180, poll_seconds=1, on_state=None):
    deadline = time.monotonic() + timeout
    previous = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Still waiting for prompt {prompt_id}; resume with --resume {prompt_id}.")
        try:
            result = request_json(f"{base_url.rstrip('/')}/api/prompts/{prompt_id}", timeout=min(10, remaining))
        except HTTPError as error:
            if error.code not in {404, 503}:
                raise
        except (URLError, TimeoutError):
            pass  # The accepted prompt can still be processing during an API outage.
        else:
            state = result["state"]
            if state != previous and on_state:
                on_state(state)
            previous = state
            if state in TERMINAL_STATES:
                return result
        time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", help="Prompt text; asks interactively when omitted")
    parser.add_argument("--url", default="http://127.0.0.1:5050", help="Flask base URL")
    parser.add_argument("--timeout", type=float, default=180, help="Maximum polling time in seconds")
    parser.add_argument("--prompt-id", type=UUID, help="Reuse an ID after an uncertain submission")
    parser.add_argument("--resume", type=UUID, help="Wait for an existing prompt without submitting again")
    parser.add_argument("--json", action="store_true", help="Print the complete final API result")
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")
    if args.resume and (args.text is not None or args.prompt_id):
        parser.error("--resume cannot be combined with prompt text or --prompt-id")
    prompt_id = str(args.resume or args.prompt_id or uuid4())
    print(f"Prompt ID: {prompt_id}", file=sys.stderr)
    try:
        if not args.resume:
            if args.text is None:
                print("Prompt: ", end="", file=sys.stderr, flush=True)
                text = input()
            else:
                text = args.text
            try:
                request_json(f"{args.url.rstrip('/')}/api/prompts", {"text": text, "prompt_id": prompt_id})
            except HTTPError as error:
                if error.code >= 500:
                    print(f"Submission was not confirmed. Retry the same text with --prompt-id {prompt_id}.", file=sys.stderr)
                raise
            except (URLError, TimeoutError):
                print(f"Submission was not confirmed. Retry the same text with --prompt-id {prompt_id}.", file=sys.stderr)
                raise
            print("queued", file=sys.stderr)
        result = wait_for_result(args.url, prompt_id, timeout=args.timeout,
                                 on_state=lambda state: print(state, file=sys.stderr))
    except (URLError, TimeoutError, ValueError, EOFError) as error:
        if isinstance(error, HTTPError):
            try:
                message = json.load(error).get("error", str(error))
            except (ValueError, AttributeError):
                message = str(error)
        else:
            message = str(error)
        print(message, file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2))
    elif result["state"] == "completed":
        print(result["agent"]["response"])
    elif result["state"] == "agent_failed":
        print(result["agent"]["error"] or "Agent execution failed.")
    else:
        print(f"{result['state']}: {result['guard']['reason']}")
    return 0 if result["state"] == "completed" else 1 if result["state"] == "agent_failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
