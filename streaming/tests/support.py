"""Shared constants and Kafka helpers for the Task 3 tests (importable, unlike conftest)."""

import uuid
from pathlib import Path

from common.config import KAFKA_BOOTSTRAP

STREAMING_DIR = Path(__file__).resolve().parents[1]
MOCK_DIR = STREAMING_DIR / "mock_data"
MOCK_SILVER = MOCK_DIR / "clean_data"
MOCK_MODEL = MOCK_DIR / "baseline_model"
MOCK_READY = all(
    (MOCK_DIR / name).exists()
    for name in ("clean_data", "baseline_model", "meter_profile", "meter_thresholds")
)


def kafka_is_up(bootstrap=KAFKA_BOOTSTRAP):
    """True if a Kafka broker answers within a few seconds."""
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
        return True
    except Exception:
        return False


def create_topic(bootstrap=KAFKA_BOOTSTRAP):
    """Create a unique 1-partition topic so tests never touch the real 'energy-readings'."""
    from confluent_kafka.admin import AdminClient, NewTopic

    name = f"test-energy-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(
        timeout=15
    )
    return name


def delete_topic(name, bootstrap=KAFKA_BOOTSTRAP):
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})  # keep a reference until done
    admin.delete_topics([name])[name].result(timeout=15)
