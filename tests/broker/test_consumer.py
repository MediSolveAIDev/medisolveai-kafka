from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest

from medisolveai_kafka.broker.consumer import AsyncConsumer
from medisolveai_kafka.core.base import BaseEvent
from medisolveai_kafka.core.config import KafkaConfig
from medisolveai_kafka.core.exceptions import NonRetryableError


class DummyEvent(BaseEvent):
    TOPIC: ClassVar[str] = "test.dummy.v1"
    VERSION: ClassVar[str] = "1.0"

    name: str

    def validate_business_rules(self) -> None:
        pass


@pytest.fixture
def config():
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        max_retries=2,
        retry_backoff_ms=10,
    )


@pytest.fixture
def consumer(config):
    return AsyncConsumer(config=config)


def _make_message(event: BaseEvent) -> MagicMock:
    msg = MagicMock()
    msg.topic = event.TOPIC
    msg.value = event.serialize()
    return msg


def test_on_decorator_registers_handler(consumer):
    @consumer.on(DummyEvent)
    async def handle(event):
        pass

    assert "test.dummy.v1" in consumer._handlers
    assert consumer._handlers["test.dummy.v1"].event_cls is DummyEvent


def test_on_decorator_custom_options(consumer):
    @consumer.on(DummyEvent, max_retries=10, commit_strategy="immediate")
    async def handle(event):
        pass

    handler = consumer._handlers["test.dummy.v1"]
    assert handler.max_retries == 10
    assert handler.commit_strategy == "immediate"


@pytest.mark.asyncio
async def test_process_message_after_process_success(consumer):
    handler_called = []

    @consumer.on(DummyEvent)
    async def handle(event):
        handler_called.append(event)

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    event = DummyEvent(name="test")
    msg = _make_message(event)

    await consumer._process_message(msg)

    assert len(handler_called) == 1
    assert handler_called[0].name == "test"
    consumer._consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_after_process_retry_then_success(consumer):
    call_count = 0

    @consumer.on(DummyEvent)
    async def handle(event):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("transient error")

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    await consumer._process_message(_make_message(DummyEvent(name="test")))

    assert call_count == 2
    consumer._consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_after_process_all_fail_to_dlq(consumer):
    @consumer.on(DummyEvent)
    async def handle(event):
        raise RuntimeError("always fail")

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    await consumer._process_message(_make_message(DummyEvent(name="test")))

    consumer._dlq_producer.send_and_wait.assert_called_once()
    dlq_topic = consumer._dlq_producer.send_and_wait.call_args.args[0]
    assert dlq_topic == "test.dummy.v1.dlq"
    consumer._consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_non_retryable_goes_to_dlq(consumer):
    @consumer.on(DummyEvent)
    async def handle(event):
        raise NonRetryableError("schema mismatch")

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    await consumer._process_message(_make_message(DummyEvent(name="test")))

    # NonRetryableError는 재시도 없이 바로 DLQ
    consumer._dlq_producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_immediate_commit(consumer):
    handler_called = []

    @consumer.on(DummyEvent, commit_strategy="immediate")
    async def handle(event):
        handler_called.append(event)

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    await consumer._process_message(_make_message(DummyEvent(name="test")))

    assert len(handler_called) == 1
    consumer._consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_immediate_no_retry_on_fail(consumer):
    call_count = 0

    @consumer.on(DummyEvent, commit_strategy="immediate")
    async def handle(event):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("fail")

    consumer._consumer = AsyncMock()
    consumer._dlq_producer = AsyncMock()

    await consumer._process_message(_make_message(DummyEvent(name="test")))

    assert call_count == 1  # 재시도 없음
    consumer._dlq_producer.send_and_wait.assert_not_called()  # DLQ도 없음
