"""The LangChain agent: an Ollama model with the calculator and course-info tools.

The model is configuration, never code: set OLLAMA_MODEL in .env (or pass --model).
Ask it directly (no Kafka or database needed): python -m backend.agent.agent "What is 12 * 7?"
"""

import argparse
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_ollama import ChatOllama

from .tools import TOOLS

SYSTEM_PROMPT = (
    "You are a helpful assistant for a university course project. "
    "For any arithmetic, call the calculator tool. "
    "For any question about the course or the project (deadlines, submission, defenses, timeline, team, stages, "
    "datasets, models, deliverables), always call lookup_course_info first and answer only from its result. "
    "Answer briefly. If a request cannot be answered with these two tools, say so politely."
)


@dataclass
class AgentAnswer:
    response: str
    tool_calls: list = field(default_factory=list)


def model_name(model=None):
    """Return the configured Ollama model; there is deliberately no built-in default."""
    model = model or os.getenv("OLLAMA_MODEL")
    if not model:
        raise RuntimeError("No Ollama model configured. Set OLLAMA_MODEL in .env or pass --model.")
    return model


def build_agent(model=None, base_url=None):
    # reasoning=False turns off thinking mode on models that support it: faster, and no <think> text in answers.
    llm = ChatOllama(
        model=model_name(model),
        base_url=base_url or os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),
        temperature=0,
        reasoning=False,
    )
    return create_agent(llm, tools=TOOLS, system_prompt=SYSTEM_PROMPT)


def ask(agent, text):
    """Run one prompt through the agent; returns the final answer and every tool call it made."""
    result = agent.invoke({"messages": [{"role": "user", "content": text}]})
    tool_calls = [
        {"name": call["name"], "args": call["args"]}
        for message in result["messages"]
        for call in getattr(message, "tool_calls", None) or []
    ]
    return AgentAnswer(response=result["messages"][-1].text.strip(), tool_calls=tool_calls)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Ask the agent one question.")
    parser.add_argument("text", help="The prompt to send to the agent")
    parser.add_argument("--model", help="Ollama model to use (default: OLLAMA_MODEL from .env)")
    args = parser.parse_args()

    answer = ask(build_agent(args.model), args.text)
    for call in answer.tool_calls:
        print(f"[tool] {call['name']}({call['args']})")
    print(answer.response)


if __name__ == "__main__":
    main()
