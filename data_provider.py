"""
backtest-only

ss21 (backtest data interface)
defines a DataProvider protocol (get_window + common timestamps) to keep strategy/backtest code decoupled from where data comes from.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Iterable, Optional, Dict, List, Tuple

@dataclass(frozen=True)
class Bar:
    ts: datetime
    close: float

class DataProvider(Protocol):
    def get_window(
        self,
        symbol: str,
        end_ts: datetime,
        lookback: int,
    ) -> List[Bar]:
        """return the most recent `lookback` bars with ts <= end_ts, ascending by ts."""
        ...

    def get_common_timestamps(
        self,
        symbols: List[str],
        start_ts: datetime,
        end_ts: datetime,
    ) -> List[datetime]:
        """return timestamps where all symbols have bars between [start_ts, end_ts]."""
        ...
