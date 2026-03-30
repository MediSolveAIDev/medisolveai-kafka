import pytest

from medisolveai_kafka.events.bay.v1.receive import StockReceive


def test_create():
    event = StockReceive(type="product", id=1, quantity=10)

    assert event.type == "product"
    assert event.supplier is None
    assert event.TOPIC == "bay.receive.v1"


def test_with_supplier():
    event = StockReceive(type="product", id=1, quantity=10, supplier="ABC Corp")

    assert event.supplier == "ABC Corp"


def test_business_rule_zero_quantity():
    event = StockReceive(type="product", id=1, quantity=0)

    with pytest.raises(ValueError, match="수량"):
        event.validate_business_rules()


def test_serialize_roundtrip():
    event = StockReceive(type="material", id=3, quantity=5, supplier="XYZ")
    restored = StockReceive.deserialize(event.serialize())

    assert restored.type == "material"
    assert restored.supplier == "XYZ"
    assert restored.event_id == event.event_id
