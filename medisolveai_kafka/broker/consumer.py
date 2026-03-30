import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Type

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..core.base import BaseEvent
from ..core.config import KafkaConfig
from ..core.exceptions import DLQSendError, NonRetryableError

logger = logging.getLogger(__name__)


@dataclass
class _HandlerEntry:
    event_cls: Type[BaseEvent]
    func: Callable
    max_retries: int
    retry_backoff_ms: int
    dlq_enabled: bool
    commit_strategy: str


class AsyncConsumer:
    def __init__(self, config: KafkaConfig | None = None):
        self._config = config or KafkaConfig()
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._handlers: dict[str, _HandlerEntry] = {}
        self._running = False

    def on(
        self,
        event_cls: Type[BaseEvent],
        *,
        max_retries: int | None = None,
        retry_backoff_ms: int | None = None,
        dlq_enabled: bool | None = None,
        commit_strategy: str = "after_process",
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            self._handlers[event_cls.TOPIC] = _HandlerEntry(
                event_cls=event_cls,
                func=func,
                max_retries=max_retries if max_retries is not None else self._config.max_retries,
                retry_backoff_ms=retry_backoff_ms if retry_backoff_ms is not None else self._config.retry_backoff_ms,
                dlq_enabled=dlq_enabled if dlq_enabled is not None else self._config.dlq_enabled,
                commit_strategy=commit_strategy,
            )
            return func
        return decorator

    async def start(self) -> None:
        topics = list(self._handlers.keys())
        if not topics:
            raise ValueError("등록된 핸들러가 없습니다.")

        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._config.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._config.auto_offset_reset,
            security_protocol=self._config.security_protocol,
            sasl_mechanism=self._config.sasl_mechanism,
            sasl_plain_username=self._config.sasl_username,
            sasl_plain_password=self._config.sasl_password,
        )
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            security_protocol=self._config.security_protocol,
            sasl_mechanism=self._config.sasl_mechanism,
            sasl_plain_username=self._config.sasl_username,
            sasl_plain_password=self._config.sasl_password,
        )
        await self._consumer.start()
        await self._dlq_producer.start()
        self._running = True
        asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._dlq_producer:
            await self._dlq_producer.stop()

    async def _consume_loop(self) -> None:
        async for message in self._consumer:
            if not self._running:
                break
            await self._process_message(message)

    async def _process_message(self, message) -> None:
        handler = self._handlers.get(message.topic)
        if not handler:
            logger.warning("핸들러 없는 토픽: %s", message.topic)
            return

        # immediate 모드: 즉시 커밋 후 핸들러 실행
        if handler.commit_strategy == "immediate":
            await self._consumer.commit()
            try:
                event = handler.event_cls.deserialize(message.value)
                await handler.func(event)
            except Exception as e:
                logger.error("핸들러 실패 (immediate, 재처리 없음): %s", e)
            return

        # after_process 모드: 핸들러 성공 후 커밋
        last_error: Exception | None = None

        for attempt in range(handler.max_retries + 1):
            try:
                event = handler.event_cls.deserialize(message.value)
                await handler.func(event)
                await self._consumer.commit()
                return
            except NonRetryableError as e:
                last_error = e
                logger.error("재시도 불가 에러: %s", e)
                break
            except Exception as e:
                last_error = e
                if attempt < handler.max_retries:
                    wait_ms = handler.retry_backoff_ms * (2 ** attempt)
                    logger.warning(
                        "핸들러 실패 (시도 %d/%d), %dms 후 재시도: %s",
                        attempt + 1, handler.max_retries + 1, wait_ms, e,
                    )
                    await asyncio.sleep(wait_ms / 1000)

        # 재시도 전부 실패 → DLQ
        if handler.dlq_enabled:
            await self._send_to_dlq(message.topic, message.value, last_error)
        else:
            logger.error("DLQ 비활성화, 메시지 폐기: %s", last_error)
        await self._consumer.commit()

    async def _send_to_dlq(self, topic: str, data: bytes, error: Exception) -> None:
        dlq_topic = topic + self._config.dlq_suffix
        try:
            await self._dlq_producer.send_and_wait(dlq_topic, value=data)
            logger.info("DLQ 전송 완료: %s", dlq_topic)
        except Exception as e:
            raise DLQSendError(original_error=e, event_data=data) from e
