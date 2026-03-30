from medisolveai_kafka.core.exceptions import (
    DLQSendError,
    EventValidationError,
    KafkaBaseError,
    NonRetryableError,
    RetryableError,
    SchemaError,
    SerializationError,
)


def test_error_hierarchy():
    assert issubclass(RetryableError, KafkaBaseError)
    assert issubclass(NonRetryableError, KafkaBaseError)
    assert issubclass(EventValidationError, NonRetryableError)
    assert issubclass(SerializationError, NonRetryableError)
    assert issubclass(SchemaError, NonRetryableError)
    assert issubclass(DLQSendError, KafkaBaseError)


def test_dlq_send_error_fields():
    original = ValueError("connection lost")
    data = b'{"test": true}'
    err = DLQSendError(original_error=original, event_data=data)

    assert err.original_error is original
    assert err.event_data == data
    assert "DLQ" in str(err)
