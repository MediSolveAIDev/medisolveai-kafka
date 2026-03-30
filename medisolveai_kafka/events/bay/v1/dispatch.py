from typing import ClassVar, Literal

from medisolveai_kafka.core.base import BaseEvent


class SupplyDispatch(BaseEvent):
    TOPIC: ClassVar[str] = "bay.dispatch.v1"
    VERSION: ClassVar[str] = "1.0"

    type: Literal["product", "material"]
    id: int
    quantity: int

    def validate_business_rules(self) -> None:
        if self.quantity <= 0:
            raise ValueError("수량은 0보다 커야 합니다.")
