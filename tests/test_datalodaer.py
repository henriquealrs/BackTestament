import sys
import io
import pytest
import unittest
from unittest import mock
from datetime import datetime, timedelta

import pandas as pd

from backtestament.data.data_loader import DataLoader, _DataFeed, DATE_TIME_COL


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



class TestDataLoaderMultipleAssets(unittest.TestCase):

    def setUp(self):
        # Simulated CSV content
        self.mock_files = {
            "AAPL.csv":
"""DATE,TIME,OPEN
2024-01-01,09:01:00,150
2024-01-01,09:01:01,151
2024-01-01,09:03:00,152""",

            "MSFT.csv":
"""DATE,TIME,OPEN
2024-01-01,09:00:00,300
2024-01-01,09:02:00,305"""
        }

    def mocked_open(self, file, *args, **kwargs):
        filename = file.split('/')[-1]
        if filename in self.mock_files:
            return io.StringIO(self.mock_files[filename])
        raise FileNotFoundError(f"No mock data for: {filename}")

    @mock.patch("builtins.open")
    def test_multiple_asset_feed_in_chronological_order(self, mock_open):
        mock_open.side_effect = self.mocked_open

        loader = DataLoader(
            assets=["AAPL", "MSFT"],
            path="mock_path",  # path is irrelevant due to mocking
            start=datetime(2024, 1, 1, 9, 0),
            end=datetime(2024, 1, 1, 9, 5)
        )
        loader.load()

        expected = [
            ("MSFT", pd.Timestamp("2024-01-01 09:00:00")),
            ("AAPL", pd.Timestamp("2024-01-01 09:01:00")),
            ("AAPL", pd.Timestamp("2024-01-01 09:01:01")),
            ("MSFT", pd.Timestamp("2024-01-01 09:02:00")),
            ("AAPL", pd.Timestamp("2024-01-01 09:03:00")),
        ]

        actual = []
        while True:
            asset, row = loader.get_next_data()
            if row.empty:
                break
            actual.append((asset, row[DATE_TIME_COL]))

        self.assertEqual(expected, actual)


if __name__ == '__main__':
    sys.exit(pytest.main(__file__))

