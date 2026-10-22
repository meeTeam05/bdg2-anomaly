import pytest

from common.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    # build_spark() also pins the process to UTC, so naive datetimes below are UTC.
    session = build_spark("data_pipeline_tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
