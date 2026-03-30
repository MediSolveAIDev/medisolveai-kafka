import pytest

from medisolveai_kafka.events.bay.v1.low_stock import LowStockAlert


def test_create():
    event = LowStockAlert(type="product", id=1, current_quantity=2, threshold=5)

    assert event.current_quantity == 2
    assert event.threshold == 5
    assert event.TOPIC == "bay.low-stock.v1"


def test_business_rule_negative_threshold():
    event = LowStockAlert(type="product", id=1, current_quantity=2, threshold=-1)

    with pytest.raises(ValueError, match="임계값"):
        event.validate_business_rules()


def test_business_rule_zero_threshold_ok():
    event = LowStockAlert(type="product", id=1, current_quantity=0, threshold=0)
    event.validate_business_rules()  # 0은 허용


def test_serialize_roundtrip():
    event = LowStockAlert(type="material", id=7, current_quantity=1, threshold=10)
    restored = LowStockAlert.deserialize(event.serialize())

    assert restored.current_quantity == 1
    assert restored.threshold == 10
    assert restored.event_id == event.event_id
