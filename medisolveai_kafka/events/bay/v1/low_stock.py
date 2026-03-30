from typing import ClassVar, Literal

from medisolveai_kafka.core.base import BaseEvent


class LowStockAlert(BaseEvent):
    TOPIC: ClassVar[str] = "bay.low-stock.v1"
    VERSION: ClassVar[str] = "1.0"

    type: Literal["product", "material"]
    id: int
    current_quantity: int
    threshold: int

    def validate_business_rules(self) -> None:
        if self.threshold < 0:
            raise ValueError("임계값은 0 이상이어야 합니다.")
