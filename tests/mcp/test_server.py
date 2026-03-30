from medisolveai_kafka.mcp.server import (
    get_consumer_usage,
    get_event_schema,
    get_producer_usage,
    get_topic_naming,
    list_events,
    validate_event_code,
)


def test_list_events_all():
    result = list_events()
    assert "bay" in result
    assert "SupplyDispatch" in result
    assert "StockReceive" in result
    assert "LowStockAlert" in result


def test_list_events_filter_domain():
    result = list_events(domain="bay")
    assert "bay" in result

    result = list_events(domain="nonexistent")
    assert "등록된 이벤트가 없습니다" in result


def test_list_events_filter_version():
    result = list_events(version="v1")
    assert "v1" in result

    result = list_events(version="v99")
    assert "등록된 이벤트가 없습니다" in result


def test_get_event_schema_found():
    result = get_event_schema(domain="bay", event_name="SupplyDispatch")
    assert "SupplyDispatch" in result
    assert "bay.dispatch.v1" in result
    assert "quantity" in result
    assert "import" in result


def test_get_event_schema_not_found():
    result = get_event_schema(domain="bay", event_name="NotExist")
    assert "찾을 수 없습니다" in result


def test_get_event_schema_bad_domain():
    result = get_event_schema(domain="nonexistent", event_name="SupplyDispatch")
    assert "찾을 수 없습니다" in result


def test_get_topic_naming():
    result = get_topic_naming()
    assert "도메인" in result
    assert "dlq" in result.lower()


def test_get_producer_usage():
    result = get_producer_usage()
    assert "AsyncProducer" in result
    assert "lifespan" in result
    assert "send" in result
    assert "DLQ" in result
    assert "KAFKA_BOOTSTRAP_SERVERS" in result


def test_get_consumer_usage():
    result = get_consumer_usage()
    assert "AsyncConsumer" in result
    assert "after_process" in result
    assert "immediate" in result
    assert "lifespan" in result
    assert "commit" in result
    assert "max_retries" in result


def test_validate_event_code_pass():
    code = """
from medisolveai_kafka import AsyncProducer
from medisolveai_kafka.core.base import BaseEvent

async def lifespan(app):
    await producer.start()
"""
    result = validate_event_code(code)
    assert "통과" in result


def test_validate_event_code_missing_import():
    code = "import kafka"
    result = validate_event_code(code)
    assert "medisolveai_kafka" in result


def test_validate_event_code_missing_lifespan():
    code = """
from medisolveai_kafka import AsyncProducer
"""
    result = validate_event_code(code)
    assert "start" in result or "lifespan" in result
