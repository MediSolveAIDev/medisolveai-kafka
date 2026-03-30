import json

import pytest

from medisolveai_kafka.events.bay.v1.dispatch import SupplyDispatch


def test_create():
    event = SupplyDispatch(type="product", id=1, quantity=10)

    assert event.type == "product"
    assert event.id == 1
    assert event.quantity == 10
    assert event.TOPIC == "bay.dispatch.v1"
    assert event.VERSION == "1.0"


def test_type_literal():
    with pytest.raises(Exception):
        SupplyDispatch(type="invalid", id=1, quantity=10)


def test_business_rule_zero_quantity():
    event = SupplyDispatch(type="product", id=1, quantity=0)

    with pytest.raises(ValueError, match="수량"):
        event.validate_business_rules()


def test_business_rule_negative_quantity():
    event = SupplyDispatch(type="product", id=1, quantity=-1)

    with pytest.raises(ValueError, match="수량"):
        event.validate_business_rules()


def test_serialize_roundtrip():
    event = SupplyDispatch(type="material", id=5, quantity=3)
    data = event.serialize()
    restored = SupplyDispatch.deserialize(data)

    assert restored.type == "material"
    assert restored.id == 5
    assert restored.quantity == 3
    assert restored.event_id == event.event_id
