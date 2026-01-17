"""
ss21
DB-backed providers that return Bar windows and iterate timestamps, letting the same strategy logic run on historical data.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Iterator, Optional
import os

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Bar:
    ts: datetime
    open: float
    close: float

class LiveDBProvider:
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not set")
        self.engine = create_engine(self.database_url, future=True)

    def get_window(self, symbol: str, end_ts: datetime, lookback: int) -> List[Bar]:
        q = text("""
            SELECT ts, open, close
            FROM prices
            WHERE symbol = :symbol AND ts <= :end_ts
            ORDER BY ts DESC
            LIMIT :lookback
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"symbol": symbol, "end_ts": end_ts, "lookback": lookback}).fetchall()

        rows = list(rows)[::-1]
        return [Bar(ts=r.ts, open=float(r.open), close=float(r.close)) for r in rows]

    def get_bar(self, symbol: str, ts: datetime) -> Optional[Bar]:
        q = text("""
            SELECT ts, open, close
            FROM prices
            WHERE symbol = :symbol AND ts = :ts
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol, "ts": ts}).fetchone()
        if not row:
            return None
        return Bar(ts=row.ts, open=float(row.open), close=float(row.close))

class BacktestDBProvider(LiveDBProvider):
    def __init__(self, database_url: Optional[str], start_ts: datetime, end_ts: datetime):
        super().__init__(database_url)
        self.start_ts = start_ts
        self.end_ts = end_ts

    def iter_times(self, symbol: str) -> Iterator[datetime]:
        q = text("""
            SELECT ts
            FROM prices
            WHERE symbol = :symbol AND ts >= :start_ts AND ts <= :end_ts
            ORDER BY ts ASC
        """)
        with self.engine.connect() as conn:
            for row in conn.execute(q, {"symbol": symbol, "start_ts": self.start_ts, "end_ts": self.end_ts}):
                yield row.ts
