from datetime import date, time, datetime, timedelta
from typing import List
import pandas as pd


DATE_COL = 'DATE'
TIME_COL = 'TIME'
DATE_TIME_COL = 'DATE_TIME'


class _DataFeed:
    def __init__(self, df: pd.DataFrame, start: datetime, end: datetime):
        self._next_row_index = 0
        self._data_finished = False
        df = self._merge_date_time_cols(df)
        min_time = pd.Timestamp(start)
        max_time = pd.Timestamp(end)
        mask = (df[DATE_TIME_COL] >= max_time) & (df[DATE_TIME_COL] <= min_time)
        df = df.loc[mask, :]
        print(df[DATE_TIME_COL])
        self._df = df

    def data_finished(self) -> bool:
        return self._data_finished

    def get_next_row(self) -> pd.Series:
        ret = self._df.iloc[self._next_row_index, :]
        self._next_row_index += 1
        if self._next_row_index >= self._df.shape[0]:
            self._data_finished = True
        return ret

    def get_next_row_timestamp(self) -> pd.Timestamp:
        return self._df.iloc[self._next_row_index][DATE_TIME_COL]

    def _merge_date_time_cols(self, frame: pd.DataFrame) -> pd.DataFrame:
        merge_date_time_str = frame[DATE_COL] + ' ' + frame[TIME_COL]
        merge_date_time = pd.to_datetime(merge_date_time_str)
        frame[DATE_TIME_COL] = merge_date_time
        frame.drop([DATE_COL, TIME_COL], axis=1, inplace=True)
        return frame

    def get_max_time(self) -> pd.Timestamp:
        return self._df.iloc[-1][DATE_TIME_COL]

    def get_min_time(self) -> pd.Timestamp:
        return self._df.iloc[0][DATE_TIME_COL]


class DataLoader:
    def __init__(self, assets: List[str], path: str, start: datetime, end: datetime):
        self._assets = assets
        self._path = path[:-1] if path[-1] == '/' else path
        self._start = start
        self._end = end

    def load(self) -> None:
        self._frames = {asset: _DataFeed(pd.read_csv(f"{self._path}/{asset}.csv"),
                                         self._start, self._end) for asset in self._assets}
        self._last_data_timestamp = pd.Timestamp(0)

    def get_next_data(self) -> tuple[str, pd.Series]:
        time_diff = pd.Timedelta.max
        ret = ("", pd.Series())
        for asset, feed in self._frames.items():
            if feed.data_finished():
                continue
            diff = feed.get_next_row_timestamp() - self._last_data_timestamp
            if diff >= time_diff:
                continue
            time_diff = diff
            ret = (asset, feed.get_next_row())
            self._last_data_timestamp
        return ret

    def get_num_frames(self) -> int:
        return len(self._frames)

    def get_max_time(self, asset: str) -> datetime:
        return self._frames[asset].get_max_time().to_pydatetime()

    def get_min_time(self, asset: str) -> datetime:
        return self._frames[asset].get_min_time().to_pydatetime()
