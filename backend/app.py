"""Local prompt intake. Run from the project root with: flask --app backend.app run --port 5050."""

import os
from pathlib import Path
from threading import Lock

import click
import psycopg
from confluent_kafka import KafkaException
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from . import db
from .intake.events import build_raw_event
from .intake.producer import create_producer, send_raw_event
from .intake.topics import bootstrap_servers

ROOT = Path(__file__).resolve().parents[1]


def create_app(test_config=None):
    load_dotenv(ROOT / ".env")
    app = Flask(
        __name__,
        template_folder=str(ROOT / "frontend" / "templates"),
        static_folder=str(ROOT / "frontend" / "static"),
        static_url_path="/static",
    )
    app.config.update(
        DATABASE_URL=os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd"),
        KAFKA_BOOTSTRAP_SERVERS=bootstrap_servers(),
        MAX_PROMPT_LENGTH=8_000,
        MAX_CONTENT_LENGTH=64 * 1024,
        TEMPLATES_AUTO_RELOAD=True,
    )
    if test_config:
        app.config.update(test_config)
    producer_lock = Lock()

    @app.get("/")
    def index():
        return render_template("index.html", max_length=app.config["MAX_PROMPT_LENGTH"])

    @app.get("/api/prompts")
    def list_prompts():
        return jsonify(prompts=db.list_prompts(app.config["DATABASE_URL"]))

    @app.get("/api/prompts/<uuid:prompt_id>")
    def get_prompt(prompt_id):
        prompt = db.get_prompt(app.config["DATABASE_URL"], prompt_id)
        if prompt is None:
            return jsonify(error="This prompt is not stored yet."), 404
        return jsonify(prompt=prompt, status="stored")

    @app.post("/api/prompts")
    def submit_prompt():
        if not request.is_json:
            return jsonify(error="Send the prompt as JSON."), 415
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or not isinstance(body.get("text"), str):
            return jsonify(error="Enter a prompt as text."), 400
        text = body["text"]
        if not text.strip():
            return jsonify(error="Enter a prompt before sending."), 400
        if len(text) > app.config["MAX_PROMPT_LENGTH"]:
            return jsonify(error="Keep your prompt within 8,000 characters."), 400
        try:
            event = build_raw_event(text, source=request.remote_addr or "web", prompt_id=body.get("prompt_id"))
        except ValueError as error:
            return jsonify(error=str(error)), 400
        try:
            with producer_lock:
                if "kafka_producer" not in app.extensions:
                    app.extensions["kafka_producer"] = create_producer(app.config["KAFKA_BOOTSTRAP_SERVERS"])
            send_raw_event(app.extensions["kafka_producer"], event)
        except (KafkaException, BufferError) as error:
            app.logger.error("Kafka delivery failed for %s: %s", event["prompt_id"], type(error).__name__)
            return jsonify(error="Could not confirm that your prompt was queued. Please retry."), 503
        prompt = {
            "id": event["prompt_id"], "text": event["text"], "created_at": event["created_at"],
            "decision": "pending", "dataset_prompt_id": None,
        }
        return jsonify(prompt=prompt, status="queued"), 202

    @app.errorhandler(psycopg.Error)
    def database_error(error):
        app.logger.error("PostgreSQL operation failed: %s", type(error).__name__)
        return jsonify(error="Storage is unavailable. Please try again shortly."), 503

    @app.errorhandler(413)
    def request_too_large(error):
        return jsonify(error="This prompt is too large. Keep it within 8,000 characters."), 413

    @app.cli.command("init-db")
    def init_db_command():
        """Create the project tables in the configured database."""
        db.init_db(app.config["DATABASE_URL"])
        click.echo("Created prompts, guard_results and dataset_prompts tables.")

    return app
