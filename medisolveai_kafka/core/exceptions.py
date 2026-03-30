class KafkaBaseError(Exception):
    """라이브러리 최상위 에러"""
    pass


class RetryableError(KafkaBaseError):
    """재시도 가능한 에러 (네트워크 타임아웃, 브로커 일시 장애, 리더 파티션 변경 등)"""
    pass


class NonRetryableError(KafkaBaseError):
    """재시도 불가능한 에러"""
    pass


class EventValidationError(NonRetryableError):
    """비즈니스 룰 검증 실패 (validate_business_rules)"""
    pass


class SerializationError(NonRetryableError):
    """직렬화/역직렬화 실패"""
    pass


class SchemaError(NonRetryableError):
    """스키마 불일치 (알 수 없는 필드, 타입 불일치 등)"""
    pass


class DLQSendError(KafkaBaseError):
    """DLQ 전송 자체가 실패한 경우"""

    def __init__(self, original_error: Exception, event_data: bytes):
        self.original_error = original_error
        self.event_data = event_data
        super().__init__(f"DLQ 전송 실패: {original_error}")
