"""The agent's two deterministic tools: a calculator and a course-information lookup."""

import json
from pathlib import Path

from langchain.tools import tool

COURSE_INFO = json.loads(Path(__file__).with_name("course_info.json").read_text(encoding="utf-8"))
OPERATIONS = {
    "add": lambda a, b: a + b,
    "subtract": lambda a, b: a - b,
    "multiply": lambda a, b: a * b,
    "divide": lambda a, b: a / b,
}


def calculate(a, operation, b):
    """Apply one arithmetic operation without eval; errors come back as text for the model."""
    operation = operation.strip().lower()
    if operation not in OPERATIONS:
        return f"Unknown operation '{operation}'. Use one of: {', '.join(OPERATIONS)}."
    if operation == "divide" and b == 0:
        return "Cannot divide by zero."
    result = OPERATIONS[operation](a, b)
    return str(int(result)) if float(result).is_integer() else str(result)


def find_course_info(topic):
    """Return the entry whose name or keywords match the topic, or list the available topics."""
    words = topic.strip().lower()
    for name, entry in COURSE_INFO.items():
        if name in words or any(keyword in words for keyword in entry["keywords"]):
            return f"{name}: {entry['info']}"
    return f"No information about '{topic}'. Available topics: {', '.join(COURSE_INFO)}."


@tool
def calculator(a: float, operation: str, b: float) -> str:
    """Calculate `a <operation> b`. operation is one of: add, subtract, multiply, divide."""
    return calculate(a, operation, b)


@tool
def lookup_course_info(topic: str) -> str:
    """Look up facts about this course project: deadlines, timeline, team members and who owns stages P1-P10,
    datasets, models and deliverables. Pass a short topic such as 'deadlines' or 'stages'."""
    return find_course_info(topic)


TOOLS = [calculator, lookup_course_info]
