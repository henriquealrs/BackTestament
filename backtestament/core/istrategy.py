from abc import ABC, abstractmethod
from overrides import overrides
from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd


class IStrategy(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_assets(self) -> List[str]:
        pass

    @abstractmethod
    def on_data(self, time_data: pd.Series) -> Dict[str, float]:
        pass

    @abstractmethod
    def on_order_fill(self, symbol: str, price: float, qtd: float) -> None:
        pass

    @abstractmethod
    def get_positio(self) -> Dict[str, float]:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass
