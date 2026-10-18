"""Shared pytest setup for the Task 3 (streaming) tests.

Run from the streaming/ folder:
    ../.venv/Scripts/python -m pytest -q              # everything
    ../.venv/Scripts/python -m pytest -q -m "not integration"   # no Kafka needed

Windows: HADOOP_HOME must point to winutils (see streaming/README.md), else Spark tests are skipped.
"""

import os
import sys
import uuid
from pathlib import Path

import pytest

STREAMING_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = STREAMING_DIR.parent
sys.path.insert(0, str(REPO_ROOT))       # config.py
sys.path.insert(0, str(STREAMING_DIR))   # kafka_producer, streaming_job, spark_session

from config import KAFKA_BOOTSTRAP  # noqa: E402

MOCK_SILVER = STREAMING_DIR / "mock_data" / "clean_data"
MOCK_MODEL = STREAMING_DIR / "mock_data" / "baseline_model"


def pytest_configure(config):
    # The marker is registered here because the team rule only allows files under streaming/tests/.
    config.addinivalue_line("markers", "integration: needs Kafka on localhost:9094, runs the real scripts")


def kafka_is_up(bootstrap=KAFKA_BOOTSTRAP):
    """True if a Kafka broker answers within a few seconds."""
    from confluent_kafka.admin import AdminClient
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip integration tests (with a clear reason) when Kafka is not reachable."""
    if not any("integration" in item.keywords for item in items):
        return
    if kafka_is_up():
        return
    skip = pytest.mark.skip(reason=f"Kafka not reachable at {KAFKA_BOOTSTRAP} (docker compose up first)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole run (startup takes ~15 s)."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("HADOOP_HOME is not set (Windows needs winutils), see streaming/README.md")
    from spark_session import build_spark
    session = build_spark("bdg2-streaming-tests")
    yield session
    session.stop()


@pytest.fixture
def mock_inputs():
    """Paths of the mock Silver table and mock model made by mock_inputs.py."""
    if not (MOCK_SILVER.exists() and MOCK_MODEL.exists()):
        pytest.skip("mock_data/ missing, run: python mock_inputs.py")
    return MOCK_SILVER, MOCK_MODEL


def create_topic(bootstrap=KAFKA_BOOTSTRAP):
    """Create a unique 1-partition topic so tests never touch the real 'energy-readings'."""
    from confluent_kafka.admin import AdminClient, NewTopic
    name = f"test-energy-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(timeout=15)
    return name


def delete_topic(name, bootstrap=KAFKA_BOOTSTRAP):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})  # keep a reference until the future is done
    admin.delete_topics([name])[name].result(timeout=15)
