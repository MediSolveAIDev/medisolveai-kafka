from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    # 접속
    bootstrap_servers: str  # 필수값 — KAFKA_BOOTSTRAP_SERVERS 미설정 시 에러
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    # Producer 기본값
    acks: str = "1"
    max_retries: int = 3
    retry_backoff_ms: int = 300

    # Consumer 기본값
    group_id: str | None = None
    auto_offset_reset: str = "earliest"
    max_poll_records: int = 500

    # DLQ
    dlq_enabled: bool = True
    dlq_suffix: str = ".dlq"

    model_config = {
        "env_prefix": "KAFKA_",
        "env_file": ".env",
        "extra": "ignore",
    }
