"""
backtest-only

ss23
Loads backtest CSV outputs and computes metrics such as total return, max drawdown, turnover, and win rate from the equity/trade events.
"""

import csv
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime

@dataclass
class TradeRow:
    ts: datetime
    symbol: str
    side: str
    qty: float
    price: float
    notional: float
    fee: float
    slip: float
    reason: str

def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", ""))

def load_equity_csv(path: str) -> List[Tuple[datetime, float]]:
    out = []
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            out.append((_dt(row["ts"]), float(row["equity"])))
    return sorted(out, key=lambda x: x[0])


def load_trades_csv(path: str) -> List[TradeRow]:
    out = []
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(TradeRow(
                ts=_dt(row["ts"]),
                symbol=row["symbol"],
                side=row["side"],
                qty=float(row["qty"]),
                price=float(row["price"]),
                notional=float(row["notional"]),
                fee=float(row["fee"]),
                slip=float(row["slip"]),
                reason=row.get("reason", ""),
            ))
    return sorted(out, key=lambda t: t.ts)


def total_return(equity: List[Tuple[datetime, float]]) -> float:
    if len(equity) < 2:
        return 0.0
    return equity[-1][1] / equity[0][1] - 1.0


def max_drawdown(equity: List[Tuple[datetime, float]]) -> float:
    peak = equity[0][1]
    mdd = 0.0
    for _, eq in equity:
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1.0)
    return mdd


def turnover(trades: List[TradeRow], equity: List[Tuple[datetime, float]]) -> float:
    if not equity:
        return 0.0
    avg_eq = sum(eq for _, eq in equity) / len(equity)
    vol = sum(abs(t.notional) for t in trades)
    return vol / avg_eq if avg_eq else 0.0


def trade_stats(equity: List[Tuple[datetime, float]], trades: List[TradeRow]):
    eq_map = {ts: eq for ts, eq in equity}
    events = []
    seen = set()
    for t in trades:
        key = (t.ts, t.reason)
        if key not in seen:
            seen.add(key)
            events.append(key)
    events.sort()

    open_ts = None
    pnls = []

    for ts, reason in events:
        if "ENTER_" in reason and open_ts is None:
            open_ts = ts
        elif "EXIT" in reason and open_ts is not None:
            if open_ts in eq_map and ts in eq_map:
                pnls.append(eq_map[ts] - eq_map[open_ts])
            open_ts = None

    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p <= 0)
    avg_pnl = sum(pnls) / len(pnls) if pnls else 0.0
    win_rate = wins / len(pnls) if pnls else 0.0
    return wins, losses, avg_pnl, win_rate


def summarize(trades_csv="bt_trades.csv", equity_csv="bt_equity.csv") -> Dict[str, float]:
    trades = load_trades_csv(trades_csv)
    equity = load_equity_csv(equity_csv)

    wins, losses, avg_pnl, win_rate = trade_stats(equity, trades)

    return {
        "total_return_pct": total_return(equity) * 100,
        "max_drawdown_pct": max_drawdown(equity) * 100,
        "win_rate_pct": win_rate * 100,
        "avg_trade_pnl_$": avg_pnl,
        "turnover_x": turnover(trades, equity),
        "round_trips": wins + losses,
        "fills": len(trades),
    }


def print_report(s: Dict[str, float]):
    print("=== BACKTEST REPORT ===")
    print(f"Total return:  {s['total_return_pct']:.2f}%")
    print(f"Max drawdown:  {s['max_drawdown_pct']:.2f}%")
    print(f"Win rate:      {s['win_rate_pct']:.2f}%")
    print(f"Avg trade PnL: ${s['avg_trade_pnl_$']:.2f}")
    print(f"Turnover:      {s['turnover_x']:.3f}x")
    print(f"Round trips:   {int(s['round_trips'])}")
    print(f"Fills:         {int(s['fills'])}")


if __name__ == "__main__":
    s = summarize()
    print_report(s)
