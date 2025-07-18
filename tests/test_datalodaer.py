import sys
import pytest
from datetime import datetime, timedelta

from backtestament.data.data_loader import DataLoader

@pytest.fixture
def single_asset() -> DataLoader:
    date_start = datetime.fromisoformat("2025-01-02 10:00:00")
    date_end = datetime.fromisoformat("2025-07-01 10:00:00")
    assets = ["WIN"]
    path = "tests/test_data"
    dl = DataLoader(assets, path, date_start, date_end)
    return dl


def test_load_success_single_asset(single_asset: DataLoader) -> None:
    single_asset.load()
    assert 1 == single_asset.get_num_frames()

    date_start = datetime.fromisoformat("2025-01-02 10:00:00")
    date_end = datetime.fromisoformat("2025-07-01 10:00:00")
    time_delta_expect: timedelta = date_end - date_start
    time_delta: timedelta = single_asset.get_max_time('WIN') - single_asset.get_min_time('WIN')
    assert abs((time_delta - time_delta_expect).total_seconds()) < (10 * 60)

if __name__ == '__main__':
    sys.exit(pytest.main(__file__))

