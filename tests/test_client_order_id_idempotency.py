from idempotent_execute import build_client_order_id

def test_client_order_id_idempotency():
    a = build_client_order_id(1, "2024-01-10T00:00:00", "ENTER_LONG")
    b = build_client_order_id(1, "2024-01-10T00:00:00", "ENTER_LONG")
    c = build_client_order_id(1, "2024-01-10T00:00:00", "EXIT")

    assert a == b
    assert a != c
    assert len(a) <= 48
