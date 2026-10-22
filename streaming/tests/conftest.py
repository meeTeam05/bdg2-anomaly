"""Shared pytest setup for the Task 3 (streaming) tests.

Run from the repo root:
    pytest streaming                      # everything (integration tests need Kafka)
    pytest streaming -m "not integration" # no Kafka needed
Mock inputs for the Spark tests: python streaming/mock_inputs.py

Windows: HADOOP_HOME must point to winutils (see streaming/README.md), else Spark tests are skipped.
"""

import os

import pytest

from common.config import KAFKA_BOOTSTRAP
from streaming.tests.support import MOCK_DIR, MOCK_READY, kafka_is_up


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: needs Kafka on localhost:9094, runs the real scripts"
    )


def pytest_collection_modifyitems(config, items):
    """Skip integration tests (with a clear reason) when Kafka is not reachable."""
    if not any("integration" in item.keywords for item in items):
        return
    if kafka_is_up():
        return
    skip = pytest.mark.skip(
        reason=f"Kafka not reachable at {KAFKA_BOOTSTRAP} (make infra-up first)"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole run (startup takes ~15 s)."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("HADOOP_HOME is not set (Windows needs winutils), see streaming/README.md")
    from streaming.streaming_job import build_streaming_spark

    session = build_streaming_spark("bdg2-streaming-tests")
    yield session
    session.stop()


@pytest.fixture
def mock_inputs():
    """The mock_data/ folder made by mock_inputs.py (Silver + Task 2 outputs)."""
    if not MOCK_READY:
        pytest.skip("mock_data/ missing, run: python streaming/mock_inputs.py")
    return MOCK_DIR
