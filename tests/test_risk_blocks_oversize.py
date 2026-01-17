from risk import risk_check

def test_risk_blocks_oversized_order():
    allowed, reasons = risk_check(
        symbols_for_order=["AAPL"],
        max_gross_exposure=0.01,
        max_position_value_per_symbol=0.01,
        max_orders_per_run=0,
        stale_seconds=0,
        orders_submitted_in_run=0,
    )
    assert allowed is False
    assert len(reasons) > 0
