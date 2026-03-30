import asyncio
import logging

from aiokafka import AIOKafkaProducer

from ..core.base import BaseEvent
from ..core.config import KafkaConfig
from ..core.exceptions import DLQSendError, NonRetryableError, RetryableError

logger = logging.getLogger(__name__)


class AsyncProducer:
    def __init__(self, config: KafkaConfig | None = None):
        self._config = config or KafkaConfig()
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            security_protocol=self._config.security_protocol,
            sasl_mechanism=self._config.sasl_mechanism,
            sasl_plain_username=self._config.sasl_username,
            sasl_plain_password=self._config.sasl_password,
            acks=int(self._config.acks) if self._config.acks.isdigit() else self._config.acks,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()

    async def send(
        self,
        event: BaseEvent,
        *,
        key: str | None = None,
        max_retries: int | None = None,
        retry_backoff_ms: int | None = None,
    ) -> None:
        # 직렬화 (실패 시 NonRetryableError → 재시도 안 함)
        data = event.serialize()
        topic = event.TOPIC
        key_bytes = key.encode("utf-8") if key else None

        retries = max_retries if max_retries is not None else self._config.max_retries
        backoff = retry_backoff_ms if retry_backoff_ms is not None else self._config.retry_backoff_ms

        last_error: Exception | None = None

        for attempt in range(retries + 1):
            try:
                await self._producer.send_and_wait(
                    topic,
                    value=data,
                    key=key_bytes,
                )
                return
            except Exception as e:
                last_error = e
                if attempt < retries:
                    wait_ms = backoff * (2 ** attempt)
                    logger.warning(
                        "전송 실패 (시도 %d/%d), %dms 후 재시도: %s",
                        attempt + 1, retries + 1, wait_ms, e,
                    )
                    await asyncio.sleep(wait_ms / 1000)

        # 재시도 전부 실패 → DLQ
        logger.error("전송 실패 (재시도 소진), DLQ로 전송: %s", last_error)
        if self._config.dlq_enabled:
            await self._send_to_dlq(topic, data, last_error)
        else:
            raise RetryableError(f"전송 실패: {last_error}") from last_error

    async def _send_to_dlq(self, topic: str, data: bytes, error: Exception) -> None:
        dlq_topic = topic + self._config.dlq_suffix
        try:
            await self._producer.send_and_wait(
                dlq_topic,
                value=data,
            )
            logger.info("DLQ 전송 완료: %s", dlq_topic)
        except Exception as e:
            raise DLQSendError(original_error=e, event_data=data) from e
