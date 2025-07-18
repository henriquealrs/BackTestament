from datetime import date, time, datetime, timedelta
from typing import List
import pandas as pd


DATE_COL = 'DATE'
TIME_COL = 'TIME'
DATE_TIME_COL = 'DATE_TIME'

class DataLoader:
    def __init__(self, assets: List[str], path: str, start: datetime, end: datetime):
        self.assets = assets
        self.path = path[:-1] if path[-1] == '/' else path
        self.start = start
        self.end = end

    def _merge_date_time_cols(self, frame: pd.DataFrame) -> None:
        merge_date_time_str = frame[DATE_COL] + ' ' + frame[TIME_COL]
        merge_date_time = pd.to_datetime(merge_date_time_str)
        frame[DATE_TIME_COL] = merge_date_time
        frame.drop([DATE_COL, TIME_COL], axis=1)

    def load(self) -> None:
        self.frames = {asset: pd.read_csv(f"{self.path}/{asset}.csv") for asset in self.assets}
        for asset in self.frames.keys():
            frame = self.frames[asset]
            self._merge_date_time_cols(frame)
            max_time = pd.Timestamp(self.start)
            min_time = pd.Timestamp(self.end)
            mask = (frame[DATE_TIME_COL] >= max_time) & (frame[DATE_TIME_COL] <= min_time)
            frame = frame.loc[mask, :]
            frame.index = frame[DATE_TIME_COL]
            self.frames[asset] = frame

    def get_num_frames(self) -> int:
        return len(self.frames)

    def get_max_time(self, asset: str) -> datetime:
        idx = self.frames[asset].index[-1]
        return idx.to_pydatetime()

    def get_min_time(self, asset: str) -> datetime:
        idx = self.frames[asset].index[0]
        return idx.to_pydatetime()
