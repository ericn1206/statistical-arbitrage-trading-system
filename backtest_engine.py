"""
backtest-only

ss22
Simulates fills at next-bar open/close with simple bps fee+slippage, tracks a portfolio, and outputs trades and an equity series.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from providers import BacktestDBProvider, Bar
from strategy import compute_pair_action

@dataclass
class Fill:
    ts: datetime
    symbol: str
    side: str      # "buy" or "sell"
    qty: float
    price: float
    notional: float
    fee: float
    slip: float
    reason: str

class Portfolio:
    def __init__(self, starting_cash: float):
        self.cash = float(starting_cash)
        self.pos: Dict[str, float] = {}     # symbol -> shares
        self.trades: List[Fill] = []

    def _apply_costs(self, side: str, raw_price: float, bps_slip: float) -> Tuple[float, float]:
        # returns (slipped_price, slip_amount_per_share)
        slip_mult = 1.0 + (bps_slip / 10000.0) if side == "buy" else 1.0 - (bps_slip / 10000.0)
        slipped = raw_price * slip_mult
        return slipped, (slipped - raw_price)

    def transact(self, ts: datetime, symbol: str, side: str, qty: float, raw_price: float,
        bps_fee: float, bps_slip: float, reason: str):
        qty = float(qty)
        raw_price = float(raw_price)

        fill_price, slip_per_share = self._apply_costs(side, raw_price, bps_slip)
        notional = qty * fill_price

        fee = abs(notional) * (bps_fee / 10000.0)

        if side == "buy":
            self.cash -= notional
            self.cash -= fee
            self.pos[symbol] = self.pos.get(symbol, 0.0) + qty
        else:
            self.cash += notional
            self.cash -= fee
            self.pos[symbol] = self.pos.get(symbol, 0.0) - qty

        self.trades.append(Fill(
            ts=ts,
            symbol=symbol,
            side=side,
            qty=qty,
            price=fill_price,
            notional=notional,
            fee=fee,
            slip=slip_per_share * qty,
            reason=reason,
        ))

    def equity(self, marks: Dict[str, float]) -> float:
        eq = self.cash
        for sym, sh in self.pos.items():
            eq += sh * float(marks.get(sym, 0.0))
        return eq

class BacktestBroker:
    """
    executes target position changes at next bar open/close.
    """
    def __init__(self, provider: BacktestDBProvider, fill_at: str = "open",
                 bps_fee: float = 1.0, bps_slip: float = 2.0):
        assert fill_at in ("open", "close")
        self.provider = provider
        self.fill_at = fill_at
        self.bps_fee = float(bps_fee)
        self.bps_slip = float(bps_slip)

    def _fill_price(self, bar: Bar) -> float:
        return bar.open if self.fill_at == "open" else bar.close

    def execute_target_delta(self, pf: Portfolio, ts_fill: datetime, symbol: str, delta_shares: float, reason: str):
        if abs(delta_shares) < 1e-12:
            return

        bar = self.provider.get_bar(symbol, ts_fill)
        if bar is None:
            return  # skip if missing bar

        px = self._fill_price(bar)
        side = "buy" if delta_shares > 0 else "sell"
        pf.transact(ts_fill, symbol, side, abs(delta_shares), px, self.bps_fee, self.bps_slip, reason)

def run_pair_backtest(
    provider: BacktestDBProvider,
    symbol_a: str,
    symbol_b: str,
    hedge_ratio: float,
    start_cash: float = 100000.0,
    lookback: int = 120,
    fill_at: str = "open",
    bps_fee: float = 1.0,
    bps_slip: float = 2.0,
    notional_per_leg: float = 10000.0,
) -> Tuple[List[Fill], List[Tuple[datetime, float]]]:
    """
    Decision time t uses window ending at t.
    Fills occur at next timestamp t_next at open/close.
    """
    broker = BacktestBroker(provider, fill_at=fill_at, bps_fee=bps_fee, bps_slip=bps_slip)
    pf = Portfolio(starting_cash=start_cash)

    times = list(provider.iter_times(symbol_a))
    if len(times) < lookback + 2:
        raise ValueError("Not enough data in range for lookback + next-bar fill")

    # Track whether currently in a pair position: -1 short spread, +1 long spread, 0 flat
    state = 0

    equity_series: List[Tuple[datetime, float]] = []

    for i in range(lookback, len(times) - 1):
        t_decide = times[i]
        t_fill = times[i + 1]

        # marks at decision time (for equity curve)
        bar_a_m = provider.get_bar(symbol_a, t_decide)
        bar_b_m = provider.get_bar(symbol_b, t_decide)
        if (bar_a_m is None) or (bar_b_m is None):
            continue
        marks = {symbol_a: bar_a_m.close, symbol_b: bar_b_m.close}
        equity_series.append((t_decide, pf.equity(marks)))

        bars_a = provider.get_window(symbol_a, t_decide, lookback)
        bars_b = provider.get_window(symbol_b, t_decide, lookback)
        z, action = compute_pair_action(bars_a, bars_b, hedge_ratio)

        # determine desired new state based on action
        desired = state
        if action == "ENTER_LONG":
            desired = +1
        elif action == "ENTER_SHORT":
            desired = -1
        elif action == "EXIT":
            desired = 0

        if desired == state:
            continue

        # compute target shares for each leg using notional_per_leg at fill price
        bar_a_f = provider.get_bar(symbol_a, t_fill)
        bar_b_f = provider.get_bar(symbol_b, t_fill)
        if (bar_a_f is None) or (bar_b_f is None):
            continue

        px_a = bar_a_f.open if fill_at == "open" else bar_a_f.close
        px_b = bar_b_f.open if fill_at == "open" else bar_b_f.close

        base_shares_a = notional_per_leg / px_a
        base_shares_b = (notional_per_leg / px_b) * abs(hedge_ratio)

        # current shares:
        cur_a = pf.pos.get(symbol_a, 0.0)
        cur_b = pf.pos.get(symbol_b, 0.0)

        # target shares based on desired state:
        if desired == 0:
            tgt_a, tgt_b = 0.0, 0.0
            reason = f"{action} z={z:.3f}"
        elif desired == +1:
            # long spread: +A, -B*hr
            tgt_a = +base_shares_a
            tgt_b = -base_shares_b
            reason = f"{action} z={z:.3f}"
        else:
            # short spread: -A, +B*hr
            tgt_a = -base_shares_a
            tgt_b = +base_shares_b
            reason = f"{action} z={z:.3f}"

        # execute deltas at fill time
        broker.execute_target_delta(pf, t_fill, symbol_a, tgt_a - cur_a, reason)
        broker.execute_target_delta(pf, t_fill, symbol_b, tgt_b - cur_b, reason)

        state = desired

    # final equity point at last time
    t_last = times[-1]
    bar_a_last = provider.get_bar(symbol_a, t_last)
    bar_b_last = provider.get_bar(symbol_b, t_last)
    if bar_a_last and bar_b_last:
        marks = {symbol_a: bar_a_last.close, symbol_b: bar_b_last.close}
        equity_series.append((t_last, pf.equity(marks)))

    return pf.trades, equity_series
