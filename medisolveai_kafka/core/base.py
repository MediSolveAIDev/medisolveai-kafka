from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import ClassVar
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from .exceptions import EventValidationError, SchemaError, SerializationError


class BaseEvent(BaseModel, ABC):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    version: str = ""

    TOPIC: ClassVar[str]
    VERSION: ClassVar[str]

    model_config = {"extra": "forbid"}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "__abstractmethods__", None):
            if not hasattr(cls, "TOPIC") or not hasattr(cls, "VERSION"):
                raise TypeError(f"{cls.__name__}에 TOPIC 또는 VERSION이 정의되지 않았습니다.")

    @model_validator(mode="before")
    @classmethod
    def _inject_meta(cls, values: dict) -> dict:
        values.setdefault("event_type", cls.__name__)
        values.setdefault("version", cls.VERSION)
        return values

    @abstractmethod
    def validate_business_rules(self) -> None:
        ...

    def serialize(self) -> bytes:
        try:
            self.validate_business_rules()
        except ValueError as e:
            raise EventValidationError(str(e)) from e
        try:
            return self.model_dump_json().encode("utf-8")
        except Exception as e:
            raise SerializationError(f"직렬화 실패: {e}") from e

    @classmethod
    def deserialize(cls, data: bytes) -> "BaseEvent":
        try:
            return cls.model_validate_json(data)
        except Exception as e:
            raise SchemaError(f"역직렬화 실패: {e}") from e
