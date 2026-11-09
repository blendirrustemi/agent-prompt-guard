"""Initialize storage and topics once the Compose dependencies are healthy."""

import os

from backend import db
from backend.intake.topics import bootstrap_servers
from backend.scripts.create_topics import create_topics


def main():
    db.init_db(os.environ["DATABASE_URL"])
    created = create_topics(bootstrap_servers())
    print(f"Database ready; created topics: {', '.join(created) or 'none (already exist)'}", flush=True)


if __name__ == "__main__":
    main()
