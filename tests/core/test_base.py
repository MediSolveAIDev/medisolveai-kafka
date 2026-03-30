import json
from typing import ClassVar

import pytest

from medisolveai_kafka.core.base import BaseEvent
from medisolveai_kafka.core.exceptions import EventValidationError, SchemaError


class ValidEvent(BaseEvent):
    TOPIC: ClassVar[str] = "test.valid.v1"
    VERSION: ClassVar[str] = "1.0"

    name: str
    value: int

    def validate_business_rules(self) -> None:
        if self.value < 0:
            raise ValueError("value must be >= 0")


def test_auto_meta_fields():
    event = ValidEvent(name="test", value=1)

    assert event.event_id  # UUID 자동 생성
    assert event.event_type == "ValidEvent"
    assert event.version == "1.0"
    assert event.timestamp is not None


def test_unique_event_id():
    e1 = ValidEvent(name="a", value=1)
    e2 = ValidEvent(name="b", value=2)

    assert e1.event_id != e2.event_id


def test_topic_and_version():
    assert ValidEvent.TOPIC == "test.valid.v1"
    assert ValidEvent.VERSION == "1.0"


def test_missing_topic_raises():
    with pytest.raises(TypeError, match="TOPIC"):
        class BadEvent(BaseEvent):
            VERSION: ClassVar[str] = "1.0"

            def validate_business_rules(self) -> None:
                pass


def test_missing_version_raises():
    with pytest.raises(TypeError, match="VERSION"):
        class BadEvent(BaseEvent):
            TOPIC: ClassVar[str] = "test.bad.v1"

            def validate_business_rules(self) -> None:
                pass


def test_extra_fields_forbidden():
    with pytest.raises(Exception):
        ValidEvent(name="test", value=1, unknown="bad")


def test_serialize():
    event = ValidEvent(name="test", value=1)
    data = event.serialize()

    assert isinstance(data, bytes)
    parsed = json.loads(data)
    assert parsed["name"] == "test"
    assert parsed["value"] == 1
    assert parsed["event_type"] == "ValidEvent"


def test_serialize_validation_failure():
    event = ValidEvent(name="test", value=-1)

    with pytest.raises(EventValidationError):
        event.serialize()


def test_deserialize():
    event = ValidEvent(name="test", value=1)
    data = event.serialize()

    restored = ValidEvent.deserialize(data)

    assert restored.name == "test"
    assert restored.value == 1
    assert restored.event_id == event.event_id


def test_deserialize_invalid_data():
    with pytest.raises(SchemaError):
        ValidEvent.deserialize(b"invalid json")


def test_deserialize_schema_mismatch():
    with pytest.raises(SchemaError):
        ValidEvent.deserialize(b'{"name": "test"}')  # value 누락


def test_nested_model():
    from pydantic import BaseModel

    class Inner(BaseModel):
        x: int
        y: int

    class NestedEvent(BaseEvent):
        TOPIC: ClassVar[str] = "test.nested.v1"
        VERSION: ClassVar[str] = "1.0"

        data: Inner

        def validate_business_rules(self) -> None:
            pass

    event = NestedEvent(data=Inner(x=1, y=2))
    restored = NestedEvent.deserialize(event.serialize())

    assert restored.data.x == 1
    assert restored.data.y == 2
