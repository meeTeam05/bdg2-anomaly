import pandas as pd
from deltalake import DeltaTable

from common.config import ENERGY_RESULTS_SCHEMA
from dashboard import logic
from dashboard import mock_stream as m


def test_batch_scenarios():
    early = m.make_batch(0).set_index("building_id")
    normal = [b for b in m.BUILDINGS if b != m.ZERO_BASELINE_BUILDING]
    assert not early["is_anomaly"][normal].any()
    assert early.loc[m.ZERO_BASELINE_BUILDING, "baseline"] == 0
    assert early.loc[m.ZERO_BASELINE_BUILDING, "is_anomaly"]

    late = m.make_batch(m.SPIKE_FROM_TICK).set_index("building_id")
    assert late.loc[m.SPIKE_BUILDING, "is_anomaly"]
    assert m.STALL_BUILDING in m.make_batch(m.STALL_AFTER_TICK)["building_id"].values
    assert m.STALL_BUILDING not in m.make_batch(m.STALL_AFTER_TICK + 1)["building_id"].values


def test_batch_matches_contract_residual():
    b = m.make_batch(3)
    assert ((b["meter_reading"] - b["baseline"]) - b["residual"]).abs().max() < 1e-9


def test_write_roundtrip_matches_energy_results_schema(tmp_path):
    path = tmp_path / "energy_results"
    m.write_batch(path, m.make_batch(0))
    m.write_batch(path, m.make_batch(1))

    dt = DeltaTable(str(path))
    assert dt.version() == 1
    assert dt.metadata().partition_columns == ["site_id"]
    delta_types = {f.name: f.type.type for f in dt.schema().fields}
    assert delta_types == {
        "building_id": "string",
        "meter_type": "string",
        "site_id": "string",
        "timestamp": "timestamp",
        "meter_reading": "double",
        "baseline": "double",
        "residual": "double",
        "is_anomaly": "boolean",
    }
    assert set(delta_types) == {c for c, _ in ENERGY_RESULTS_SCHEMA}

    df = logic.read_window(str(path), 24)
    assert len(df) == 2 * len(m.BUILDINGS)
    assert isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype)
