from typing import ClassVar
from unittest.mock import AsyncMock, patch

import pytest

from medisolveai_kafka.broker.producer import AsyncProducer
from medisolveai_kafka.core.base import BaseEvent
from medisolveai_kafka.core.config import KafkaConfig
from medisolveai_kafka.core.exceptions import DLQSendError, EventValidationError


class DummyEvent(BaseEvent):
    TOPIC: ClassVar[str] = "test.dummy.v1"
    VERSION: ClassVar[str] = "1.0"

    name: str

    def validate_business_rules(self) -> None:
        if not self.name:
            raise ValueError("name is required")


@pytest.fixture
def config():
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        max_retries=2,
        retry_backoff_ms=10,
    )


@pytest.fixture
def producer(config):
    p = AsyncProducer(config=config)
    p._producer = AsyncMock()
    return p


@pytest.mark.asyncio
async def test_send_success(producer):
    event = DummyEvent(name="test")

    await producer.send(event)

    producer._producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_send_with_key(producer):
    event = DummyEvent(name="test")

    await producer.send(event, key="my-key")

    call_kwargs = producer._producer.send_and_wait.call_args
    assert call_kwargs.kwargs["key"] == b"my-key"


@pytest.mark.asyncio
async def test_send_retry_then_success(producer):
    producer._producer.send_and_wait.side_effect = [
        Exception("fail 1"),
        None,  # 성공
    ]

    await producer.send(DummyEvent(name="test"))

    assert producer._producer.send_and_wait.call_count == 2


@pytest.mark.asyncio
async def test_send_all_retries_fail_to_dlq(producer):
    producer._producer.send_and_wait.side_effect = [
        Exception("fail 1"),
        Exception("fail 2"),
        Exception("fail 3"),
        None,  # DLQ 전송 성공
    ]

    await producer.send(DummyEvent(name="test"))

    # 원본 3회 (1 + 2 retries) + DLQ 1회 = 4회
    assert producer._producer.send_and_wait.call_count == 4
    last_call = producer._producer.send_and_wait.call_args
    assert last_call.args[0] == "test.dummy.v1.dlq"


@pytest.mark.asyncio
async def test_send_dlq_also_fails(producer):
    producer._producer.send_and_wait.side_effect = Exception("always fail")

    with pytest.raises(DLQSendError):
        await producer.send(DummyEvent(name="test"))


@pytest.mark.asyncio
async def test_send_validation_failure(producer):
    event = DummyEvent(name="")

    with pytest.raises(EventValidationError):
        await producer.send(event)
