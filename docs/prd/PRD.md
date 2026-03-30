# medisolveai-kafka 라이브러리 설계 계획서

## 개요

여러 사내 서비스(시술관리, bay, 차트관리 등)가 Kafka를 일관되게 사용할 수 있도록
공통 라이브러리로 추상화한다. 서비스 담당자는 카프카 내부 동작을 몰라도 이벤트 정의와
비즈니스 로직만 작성하면 된다.

이 프로젝트는 두 가지로 구성된다:
1. **라이브러리** — 런타임에 import하여 사용하는 Python 패키지 (PyPI 배포)
2. **MCP 서버** — 담당자가 Claude Code에 연결하면, AI가 라이브러리 규격에 맞는 코드를 생성해주는 도구 (uvx로 실행)

---

## 디렉토리 구조

```
medisolveai-kafka/
├── medisolveai_kafka/               # 라이브러리 (PyPI 배포)
│   ├── __init__.py                  # public API re-export
│   ├── core/                        # 핵심 추상화 (변경 거의 없음)
│   │   ├── __init__.py
│   │   ├── base.py                  # BaseEvent 추상클래스 (절대 변경 없음)
│   │   ├── config.py                # 환경변수 기반 설정
│   │   └── exceptions.py            # 공통 에러 정의
│   ├── broker/                      # Kafka 통신 (내부 구현)
│   │   ├── __init__.py
│   │   ├── producer.py              # AsyncProducer (재시도 + DLQ 포함)
│   │   └── consumer.py              # AsyncConsumer (offset 관리 + 재시도 + DLQ 포함)
│   └── events/                      # 이벤트 스키마 정의
│       └── bay/
│           └── v1/
│               ├── __init__.py
│               ├── dispatch.py      # SupplyDispatch (소모발송)
│               ├── receive.py       # StockReceive (입고등록)
│               └── low_stock.py     # LowStockAlert (재고부족알림)
│   └── mcp/                         # MCP 서버 (uvx로 실행)
│       ├── __init__.py
│       └── server.py                # MCP 엔드포인트 정의
├── tests/
├── pyproject.toml                   # 라이브러리 + MCP 서버 통합 패키지
└── README.md
```

---

## 모듈별 인터페이스 설계

### base.py — 절대 불변

`BaseEvent`는 카프카가 없어질 때까지 변경하지 않는다. 모든 이벤트는 반드시 `BaseEvent`를 상속받아야 하며, 공통 메타 필드와 비즈니스 룰 검사를 강제한다.

```python
from abc import ABC, abstractmethod
from datetime import datetime
from typing import ClassVar
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class BaseEvent(BaseModel, ABC):
    # 자동 주입 (담당자가 건드리지 않음)
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = ""          # __init_subclass__에서 자동 세팅
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = ""             # __init_subclass__에서 자동 세팅

    TOPIC: ClassVar[str]          # 서브클래스에서 반드시 정의
    VERSION: ClassVar[str]        # 서브클래스에서 반드시 정의

    model_config = {"extra": "forbid"}  # 정의되지 않은 필드 추가 금지

    def __init_subclass__(cls, **kwargs):
        """서브클래스 정의 시 TOPIC, VERSION 존재 여부를 검증한다."""
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "__abstractmethods__", None):  # 구체 클래스만 검증
            if not hasattr(cls, "TOPIC") or not hasattr(cls, "VERSION"):
                raise TypeError(f"{cls.__name__}에 TOPIC 또는 VERSION이 정의되지 않았습니다.")

    @model_validator(mode="before")
    @classmethod
    def _inject_meta(cls, values: dict) -> dict:
        """event_type, version을 자동 주입한다."""
        values.setdefault("event_type", cls.__name__)
        values.setdefault("version", cls.VERSION)
        return values

    @abstractmethod
    def validate_business_rules(self) -> None:
        """서브클래스에서 반드시 구현. 비즈니스 규칙 위반 시 ValueError를 raise한다."""
        ...

    def serialize(self) -> bytes:
        """Kafka 전송용 JSON bytes로 직렬화한다."""
        self.validate_business_rules()
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "BaseEvent":
        """JSON bytes → 이벤트 객체로 역직렬화한다."""
        return cls.model_validate_json(data)
```

---

### config.py — 환경변수 기반 설정

`pydantic-settings`로 환경변수를 파싱한다. 담당자는 `.env`만 작성하면 된다.

```python
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    # 접속
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"       # PLAINTEXT | SASL_SSL
    sasl_mechanism: str | None = None           # PLAIN | SCRAM-SHA-256
    sasl_username: str | None = None
    sasl_password: str | None = None

    # Producer 기본값
    acks: str = "1"                             # 0 | 1 | all
    max_retries: int = 3
    retry_backoff_ms: int = 300

    # Consumer 기본값
    group_id: str | None = None
    auto_offset_reset: str = "earliest"         # earliest | latest
    max_poll_records: int = 500

    # DLQ
    dlq_enabled: bool = True
    dlq_suffix: str = ".dlq"                    # 토픽명 뒤에 붙임

    model_config = {
        "env_prefix": "KAFKA_",                 # KAFKA_BOOTSTRAP_SERVERS 등
        "env_file": ".env",
        "extra": "ignore",
    }
```

**사용법:**
```python
config = KafkaConfig()                          # .env에서 자동 로드
config = KafkaConfig(bootstrap_servers="custom:9092")  # 직접 오버라이드
```

---

### exceptions.py — 공통 에러 정의

에러를 재시도 가능/불가능으로 구분하여 Producer/Consumer가 적절히 처리한다.

```python
class KafkaBaseError(Exception):
    """라이브러리 최상위 에러"""
    pass


class RetryableError(KafkaBaseError):
    """재시도 가능한 에러 (네트워크 타임아웃, 브로커 일시 장애 등)"""
    pass


class NonRetryableError(KafkaBaseError):
    """재시도 불가능한 에러 (직렬화 실패, 스키마 불일치 등)"""
    pass


class DLQSendError(KafkaBaseError):
    """DLQ 전송 자체가 실패한 경우"""
    def __init__(self, original_error: Exception, event_data: bytes):
        self.original_error = original_error
        self.event_data = event_data
        super().__init__(f"DLQ 전송 실패: {original_error}")


class EventValidationError(NonRetryableError):
    """이벤트 비즈니스 룰 검증 실패"""
    pass
```

---

### producer.py — AsyncProducer 인터페이스

```python
from aiokafka import AIOKafkaProducer

from .base import BaseEvent
from .config import KafkaConfig
from .exceptions import DLQSendError, NonRetryableError


class AsyncProducer:
    def __init__(self, config: KafkaConfig | None = None):
        """
        config가 None이면 환경변수에서 자동 로드한다.
        """
        self._config = config or KafkaConfig()
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Kafka Producer 연결을 시작한다. FastAPI lifespan에서 호출."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            security_protocol=self._config.security_protocol,
            sasl_mechanism=self._config.sasl_mechanism,
            sasl_plain_username=self._config.sasl_username,
            sasl_plain_password=self._config.sasl_password,
            acks=self._config.acks,
        )
        await self._producer.start()

    async def stop(self) -> None:
        """Kafka Producer 연결을 종료한다. FastAPI lifespan에서 호출."""
        if self._producer:
            await self._producer.stop()

    async def send(
        self,
        event: BaseEvent,
        *,
        key: str | None = None,
        acks: str | None = None,
        max_retries: int | None = None,
        retry_backoff_ms: int | None = None,
    ) -> None:
        """
        이벤트를 Kafka로 전송한다.

        Args:
            event: BaseEvent를 상속한 이벤트 객체
            key: 파티션 키 (None이면 라운드로빈)
            acks: 이 메시지에만 적용할 acks 오버라이드
            max_retries: 이 메시지에만 적용할 재시도 횟수
            retry_backoff_ms: 이 메시지에만 적용할 재시도 간격

        Raises:
            NonRetryableError: 직렬화 실패 등 재시도 불가능한 에러
            DLQSendError: 재시도 전부 실패 후 DLQ 전송도 실패한 경우
        """
        ...

    async def _send_to_dlq(self, topic: str, data: bytes, error: Exception) -> None:
        """원본 토픽 + dlq_suffix로 DLQ 전송한다."""
        ...
```

**FastAPI 연동 예시:**
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from medisolveai_kafka import AsyncProducer

producer = AsyncProducer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    yield
    await producer.stop()

app = FastAPI(lifespan=lifespan)
```

---

### consumer.py — AsyncConsumer 인터페이스

```python
from typing import Callable, Type

from aiokafka import AIOKafkaConsumer

from .base import BaseEvent
from .config import KafkaConfig


class AsyncConsumer:
    def __init__(self, config: KafkaConfig | None = None):
        """
        config가 None이면 환경변수에서 자동 로드한다.
        """
        self._config = config or KafkaConfig()
        self._consumer: AIOKafkaConsumer | None = None
        self._handlers: dict[str, _HandlerEntry] = {}  # topic → handler 매핑

    def on(
        self,
        event_cls: Type[BaseEvent],
        *,
        max_retries: int | None = None,
        retry_backoff_ms: int | None = None,
        dlq_enabled: bool | None = None,
        commit_strategy: str = "after_process",  # "after_process" | "immediate"
    ) -> Callable:
        """
        이벤트 핸들러를 등록하는 데코레이터.

        Args:
            event_cls: 처리할 이벤트 클래스 (TOPIC을 자동으로 읽음)
            max_retries: 이 핸들러에만 적용할 재시도 횟수
            retry_backoff_ms: 이 핸들러에만 적용할 재시도 간격
            dlq_enabled: 이 핸들러에만 적용할 DLQ 활성화 여부
            commit_strategy: "after_process" (기본값, 처리 후 커밋) | "immediate" (즉시 커밋)
        """
        def decorator(func: Callable) -> Callable:
            self._handlers[event_cls.TOPIC] = _HandlerEntry(
                event_cls=event_cls,
                func=func,
                max_retries=max_retries or self._config.max_retries,
                retry_backoff_ms=retry_backoff_ms or self._config.retry_backoff_ms,
                dlq_enabled=dlq_enabled if dlq_enabled is not None else self._config.dlq_enabled,
            )
            return func
        return decorator

    async def start(self) -> None:
        """Kafka Consumer를 시작하고 메시지 루프를 실행한다."""
        topics = list(self._handlers.keys())
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._config.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._config.auto_offset_reset,
        )
        await self._consumer.start()
        # 메시지 루프 시작 (내부에서 _process_message 호출)

    async def stop(self) -> None:
        """Kafka Consumer를 종료한다."""
        if self._consumer:
            await self._consumer.stop()

    async def _process_message(self, message) -> None:
        """
        메시지 처리 흐름:

        [after_process 모드 — 기본값]
        1. topic으로 handler 조회
        2. event_cls.deserialize(message.value)로 역직렬화
        3. handler.func(event) 실행
        4. 성공 → offset commit
        5. 실패 → 재시도 (exponential backoff)
        6. 재시도 전부 실패 → DLQ 전송 → offset commit

        [immediate 모드]
        1. topic으로 handler 조회
        2. offset commit (즉시)
        3. event_cls.deserialize(message.value)로 역직렬화
        4. handler.func(event) 실행
        5. 실패해도 재시도/DLQ 없음 (이미 commit됨)
        """
        ...

    async def _send_to_dlq(self, topic: str, data: bytes, error: Exception) -> None:
        """DLQ 토픽으로 실패 메시지를 전송한다."""
        ...


class _HandlerEntry:
    """핸들러 등록 정보를 담는 내부 데이터 클래스."""
    def __init__(
        self,
        event_cls: Type[BaseEvent],
        func: Callable,
        max_retries: int,
        retry_backoff_ms: int,
        dlq_enabled: bool,
    ):
        self.event_cls = event_cls
        self.func = func
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.dlq_enabled = dlq_enabled
        self.commit_strategy = commit_strategy
```

**FastAPI 연동 예시:**
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from medisolveai_kafka import AsyncConsumer
from medisolveai_kafka.events.bay.v1 import 소모발송

consumer = AsyncConsumer()

@consumer.on(소모발송)
async def handle_consume(event: 소모발송):
    await some_service.process(event)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await consumer.start()
    yield
    await consumer.stop()

app = FastAPI(lifespan=lifespan)
```

---

### 직렬화/역직렬화 규칙

| 방향 | 방법 | 담당 |
|------|------|------|
| 이벤트 → Kafka | `event.serialize()` → `model_dump_json().encode("utf-8")` | BaseEvent |
| Kafka → 이벤트 | `EventClass.deserialize(bytes)` → `model_validate_json(bytes)` | BaseEvent |

- 포맷: JSON (UTF-8)
- 직렬화 전에 `validate_business_rules()` 자동 호출
- 역직렬화 시 Pydantic이 스키마 검증 자동 수행
- `extra = "forbid"` 이므로 알 수 없는 필드가 있으면 역직렬화 실패 → NonRetryableError

### 이벤트 정의 — 중앙 관리

이벤트 스키마는 라이브러리 담당자만 `events/` 폴더를 수정할 수 있다. 서비스 담당자는 읽기만 한다.

```python
# events/bay/v1/__init__.py
class 소모발송(BaseEvent):
    TOPIC: ClassVar[str] = "inventory.consume.v1"
    VERSION: ClassVar[str] = "1.0"

    type: Literal["product", "material"]  # 허용값 강제
    id: int
    quantity: int

    def validate_business_rules(self) -> None:
        if self.quantity <= 0:
            raise ValueError("수량은 0보다 커야 합니다.")
```

### 서비스 담당자 사용법

담당자가 알아야 할 것은 딱 2가지다. 어떤 이벤트를 보낼지, 어떤 이벤트를 받을지.

```python
from medisolveai_kafka.events.bay.v1 import 소모발송
from medisolveai_kafka import AsyncProducer, AsyncConsumer

# 보내기
await producer.send(소모발송(type="product", id=2, quantity=3))

# 받기
@consumer.on(소모발송)
async def handle(event: 소모발송):
    await some_service.process(event)
```

---

## 기술 스택

| 역할 | 선택 |
|---|---|
| 기본 방식 | 비동기 (aiokafka) |
| 프레임워크 | FastAPI |
| 스키마 유효성 검사 | Pydantic |
| 설정 관리 | pydantic-settings (환경변수) |

동기 방식은 필요한 팀만 opt-in으로 사용 가능하도록 추후 `sync/` 하위에 별도 제공한다.

---

## Producer 설계

- 기본 동작: `send()` 호출 시 재시도 후 전부 실패하면 DLQ로 전송
- 재시도/DLQ 로직은 라이브러리 내부에서 처리하며 담당자는 신경 쓰지 않아도 됨
- 고수 담당자는 매개변수로 커스텀 가능

```python
# 일반 담당자
await producer.send(소모발송(type="product", id=2, quantity=3))

# 고수 담당자 (커스텀 설정)
await producer.send(
    소모발송(type="product", id=2, quantity=3),
    acks="all",
    max_retries=5,
)
```

재시도 흐름:

```
send() 호출
    ↓ 실패
재시도 1 (300ms 후)
재시도 2 (600ms 후)
재시도 3 (1200ms 후)
    ↓ 전부 실패
DLQ 토픽으로 전송 (inventory.consume.v1.dlq)
```

---

## Consumer 설계

- `enable_auto_commit = False` 로 설정
- 핸들러 성공 시에만 offset commit 전송
- 핸들러 실패 시 commit 안 함 → 카프카가 같은 메시지 재전송
- 재시도 횟수 초과 시 DLQ로 이동 후 commit
- commit/재시도/DLQ는 전부 라이브러리가 책임지며 담당자는 신경 쓰지 않아도 됨

```python
# 일반 담당자
@consumer.on(소모발송)
async def handle(event: 소모발송):
    await some_service.process(event)
    # commit은 라이브러리가 알아서 함

# 고수 담당자 (커스텀 설정)
@consumer.on(
    소모발송,
    max_retries=5,
    retry_backoff_ms=1000,
    dlq_enabled=False,
)
async def handle(event: 소모발송):
    await some_service.process(event)

# 즉시 커밋 (처리량 우선, 유실 허용)
@consumer.on(소모발송, commit_strategy="immediate")
async def handle(event: 소모발송):
    await some_service.process(event)
```

메시지 처리 흐름:

```
[after_process — 기본값]
메시지 수신 → 핸들러 실행 → 성공 → offset commit
                         → 실패 → 재시도 (최대 N회)
                                    → 전부 실패 → DLQ 전송 → offset commit

[immediate]
메시지 수신 → offset commit → 핸들러 실행 (실패해도 재처리 없음)
```

---

## 설정 관리

모든 설정은 환경변수로 관리하며 각 서비스가 `.env`에 직접 정의한다. 코드에 민감 정보 없음.

```env
# 각 서비스 .env
KAFKA_BOOTSTRAP_SERVERS=our-kafka.internal:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_USERNAME=myservice
KAFKA_SASL_PASSWORD=secret
KAFKA_ACKS=1
KAFKA_RETRIES=3
KAFKA_RETRY_BACKOFF_MS=300
KAFKA_GROUP_ID=my-service-group
KAFKA_DLQ_ENABLED=true
```

---

## 이벤트 버전 관리 전략

### base.py
카프카가 없어질 때까지 절대 변경하지 않는다.

### 필드 추가 (하위 호환 O)
Optional 필드로 추가한다. 기존 컨슈머는 새 필드를 무시하므로 안전하다. 라이브러리 버전은 minor 업 한다.

```python
# v1 유지, Optional 필드 추가
class 소모발송(BaseEvent):
    type: Literal["product", "material"]
    id: int
    quantity: int
    memo: str | None = None  # 추가. 기존 컨슈머 영향 없음
```

### 필드 삭제/타입 변경 (하위 호환 X)
새 버전 디렉토리와 새 토픽을 만든다. 라이브러리 버전은 major 업 한다.

```python
# events/bay/v2/__init__.py
class 소모발송(BaseEvent):
    TOPIC: ClassVar[str] = "inventory.consume.v2"  # 토픽도 분리
    ...
```

v1은 deprecated 공지 후 일정 기간 유지한 뒤 제거한다.

### 버전 규칙 요약

```
1.0.0  → 최초 배포
1.0.1  → 버그픽스
1.1.0  → 필드 추가 (하위 호환 O)
2.0.0  → 필드 삭제/타입 변경 (하위 호환 X)
```

---

## 토픽 네이밍 규칙

```
{도메인}.{행위}.{버전}

예시:
inventory.consume.v1
inventory.receive.v1
inventory.low-stock.v1
treatment.created.v1
treatment.completed.v1

DLQ:
inventory.consume.v1.dlq
```

---

## 배포

- 저장소: GitHub public repo (코드 자체는 공개해도 무방, 민감 정보는 환경변수)
- 패키지 배포: 공개 PyPI
- 자동 배포: GitHub Actions (public repo 무료)

```yaml
# .github/workflows/publish.yml
on:
  push:
    tags:
      - "v*"   # v1.2.0 태그 푸시 시 자동 배포

jobs:
  publish:
    steps:
      - run: python -m build
      - run: twine upload dist/*
        env:
          TWINE_PASSWORD: ${{ secrets.PYPI_TOKEN }}
```

서비스 담당자 설치:

```bash
pip install medisolveai-kafka
```

---

## 브랜치 전략

```
main         → 배포 브랜치 (태그 달면 PyPI 자동 배포)
develop      → 개발 브랜치
feature/xxx  → 기능 개발
hotfix/xxx   → 긴급 버그픽스
```

---

## MCP 서버 설계

### 목적

담당자가 Claude Code에 이 MCP 서버를 연결하면, AI가 라이브러리 규격을 이해하고 규격에 맞는 코드를 생성해준다.

### 담당자 서비스 프로젝트의 `.mcp.json`

```json
{
  "mcpServers": {
    "medisolveai-kafka": {
      "command": "uvx",
      "args": ["medisolveai-kafka-mcp"]
    }
  }
}
```

### MCP 서버가 제공하는 도구 (Tools)

| Tool | 설명 | 예시 |
|------|------|------|
| `list_events` | 사용 가능한 이벤트 목록 조회 | bay/v1의 모든 이벤트 |
| `get_event_schema` | 특정 이벤트의 필드/타입/규칙 조회 | 소모발송의 스키마 |
| `get_topic_naming` | 토픽 네이밍 규칙 반환 | `{도메인}.{행위}.{버전}` |
| `get_producer_usage` | Producer 사용법 코드 예시 반환 | send() 호출 예시 |
| `get_consumer_usage` | Consumer 사용법 코드 예시 반환 | @on() 데코레이터 예시 |
| `validate_event_code` | 담당자가 작성한 코드가 규격에 맞는지 검증 | BaseEvent 상속 여부, 필수 필드 확인 |

### MCP 서버 구현 (`mcp_server/server.py`)

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("medisolveai-kafka")

@mcp.tool()
def list_events(domain: str | None = None, version: str | None = None) -> str:
    """사용 가능한 이벤트 목록을 반환한다."""
    ...

@mcp.tool()
def get_event_schema(domain: str, event_name: str, version: str = "v1") -> str:
    """특정 이벤트의 스키마(필드, 타입, 비즈니스 규칙)를 반환한다."""
    ...

@mcp.tool()
def get_topic_naming() -> str:
    """토픽 네이밍 규칙과 예시를 반환한다."""
    ...

@mcp.tool()
def get_producer_usage() -> str:
    """AsyncProducer 사용법과 코드 예시를 반환한다."""
    ...

@mcp.tool()
def get_consumer_usage() -> str:
    """AsyncConsumer 사용법과 코드 예시를 반환한다."""
    ...

@mcp.tool()
def validate_event_code(code: str) -> str:
    """담당자가 작성한 코드가 라이브러리 규격에 맞는지 검증한다."""
    ...
```

### pyproject.toml MCP 엔트리포인트

```toml
[project.scripts]
medisolveai-kafka-mcp = "medisolveai_kafka.mcp.server:mcp.run"
```

### 담당자 개발 워크플로우

```
1. pip install medisolveai-kafka             ← 라이브러리 설치
2. .mcp.json에 medisolveai-kafka MCP 등록    ← Claude Code 연결
3. Claude Code에서 "bay 소모발송 이벤트 보내는 코드 짜줘" 요청
4. Claude가 MCP를 통해:
   - list_events로 사용 가능한 이벤트 확인
   - get_event_schema로 소모발송 스키마 조회
   - get_producer_usage로 사용법 확인
   - 규격에 맞는 코드 생성
```

---

## 미정 항목

```
⬜ 로컬 개발환경 (docker-compose Kafka 세팅 문서화)
⬜ 담당자용 README / 사용 가이드
⬜ 동기 방식 (sync/) 지원 시점
```