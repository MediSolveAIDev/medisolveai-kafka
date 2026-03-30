import pytest

from medisolveai_kafka.core.config import KafkaConfig


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "test-kafka:9092")
    monkeypatch.setenv("KAFKA_GROUP_ID", "test-group")

    config = KafkaConfig()

    assert config.bootstrap_servers == "test-kafka:9092"
    assert config.group_id == "test-group"
    assert config.acks == "1"
    assert config.max_retries == 3
    assert config.dlq_enabled is True


def test_config_bootstrap_servers_required(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    with pytest.raises(Exception):
        KafkaConfig()


def test_config_override():
    config = KafkaConfig(
        bootstrap_servers="custom:9092",
        acks="all",
        max_retries=10,
    )

    assert config.bootstrap_servers == "custom:9092"
    assert config.acks == "all"
    assert config.max_retries == 10


def test_config_defaults(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    config = KafkaConfig()

    assert config.security_protocol == "PLAINTEXT"
    assert config.sasl_mechanism is None
    assert config.sasl_username is None
    assert config.sasl_password is None
    assert config.auto_offset_reset == "earliest"
    assert config.max_poll_records == 500
    assert config.retry_backoff_ms == 300
    assert config.dlq_suffix == ".dlq"
    assert config.group_id is None
