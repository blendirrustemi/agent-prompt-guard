"""Start/stop an isolated development database using installed PostgreSQL binaries."""

import argparse
import shlex
import shutil
import subprocess
from pathlib import Path

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "tmp" / "postgres-dev"
PORT = 55432
URL = f"postgresql://pijd@127.0.0.1:{PORT}/pijd"


def run(*command):
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise SystemExit(result.stdout + result.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop", action="store_true", help="Stop this project's instance, retaining data")
    args = parser.parse_args()
    for binary in ("initdb", "pg_ctl"):
        if not shutil.which(binary):
            raise SystemExit(f"{binary} is missing. Use an existing PostgreSQL server or Docker Compose instead.")
    if args.stop:
        run("pg_ctl", "-D", str(DATA), "-m", "fast", "-w", "stop")
        print("Stopped the project database; its data is retained.")
        return

    DATA.parent.mkdir(parents=True, exist_ok=True)
    if not (DATA / "PG_VERSION").exists():
        run("initdb", "-D", str(DATA), "-U", "pijd", "--auth=trust", "--encoding=UTF8", "--locale=C")
    running = subprocess.run(["pg_ctl", "-D", str(DATA), "status"], capture_output=True).returncode == 0
    if not running:
        options = f"-h 127.0.0.1 -p {PORT} -k {shlex.quote(str(DATA.parent))}"
        run("pg_ctl", "-D", str(DATA), "-l", str(DATA.parent / "postgres-dev.log"), "-o", options, "-w", "start")

    with psycopg.connect(f"postgresql://pijd@127.0.0.1:{PORT}/postgres", autocommit=True) as conn:
        for name in ("pijd", "pijd_test"):
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    with psycopg.connect(URL) as conn:
        conn.execute((ROOT / "backend" / "schema.sql").read_text(), prepare=False)

    env = ROOT / ".env"
    if not env.exists():
        env.write_text(f"DATABASE_URL={URL}\n")
        print("Created .env with the project database connection.")
    else:
        print("Kept your existing .env; ensure DATABASE_URL selects this instance if you want to use it.")
    print(f"Project PostgreSQL is ready on 127.0.0.1:{PORT}.")
    print("Start the page: python3 -m flask --app backend.app run --host 127.0.0.1 --port 5050")


if __name__ == "__main__":
    main()
