"""Create the project's Kafka topics. Run from the project root: python -m backend.scripts.create_topics"""

import argparse

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from backend.intake.topics import ALL_TOPICS, bootstrap_servers


def create_topics(servers, partitions=1, timeout=15):
    """Create any missing topics; existing topics are left unchanged. Returns the names created."""
    admin = AdminClient({"bootstrap.servers": servers})
    existing = admin.list_topics(timeout=timeout).topics
    missing = [name for name in ALL_TOPICS if name not in existing]
    if not missing:
        return []
    # One broker, so replication factor 1.
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=1) for name in missing],
        request_timeout=timeout,
    )
    for future in futures.values():
        future.result()
    return missing


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=bootstrap_servers(), help="Kafka address (default: %(default)s)")
    parser.add_argument("--partitions", type=int, default=1, help="Partitions for new topics (default: 1)")
    args = parser.parse_args()

    created = create_topics(args.bootstrap_servers, args.partitions)
    for name in ALL_TOPICS:
        print(f"{name}: {'created' if name in created else 'already exists'}")


if __name__ == "__main__":
    main()
