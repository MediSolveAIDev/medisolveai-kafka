# medisolveai-kafka

사내 서비스 Kafka 공통 라이브러리 + MCP 서버

서비스 담당자는 카프카 내부 동작을 몰라도 **이벤트 import + send/on 두 가지**만으로 Kafka를 사용할 수 있습니다.

## 구조

```
medisolveai_kafka/
├── core/           # BaseEvent, KafkaConfig, Exceptions
├── broker/         # AsyncProducer, AsyncConsumer
├── events/         # 이벤트 스키마 (도메인/버전별)
│   └── bay/v1/     # SupplyDispatch, StockReceive, LowStockAlert
├── mcp/            # MCP 서버 (Claude Code 연동)
└── example/        # 예제 앱 (Docker + FastAPI UI)
```

## 설치

```bash
pip install medisolveai-kafka
```

## 환경변수 (.env)

```env
KAFKA_BOOTSTRAP_SERVERS=your-kafka:9092   # 필수
KAFKA_GROUP_ID=my-service-group           # Consumer 사용 시 필수
```

## 사용법

### 이벤트 발행 (Producer)

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from medisolveai_kafka import AsyncProducer
from medisolveai_kafka.events.bay.v1.dispatch import SupplyDispatch

producer = AsyncProducer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    yield
    await producer.stop()

app = FastAPI(lifespan=lifespan)

@app.post("/dispatch")
async def dispatch():
    await producer.send(SupplyDispatch(type="product", id=1, quantity=10))
```

### 이벤트 소비 (Consumer)

```python
from medisolveai_kafka import AsyncConsumer
from medisolveai_kafka.events.bay.v1.dispatch import SupplyDispatch

consumer = AsyncConsumer()

@consumer.on(SupplyDispatch)
async def handle(event: SupplyDispatch):
    print(f"소모발송: {event.type} id={event.id} qty={event.quantity}")
```

### 커밋 전략

```python
# 기본: 처리 성공 후 commit (안전, 유실 없음)
@consumer.on(SupplyDispatch)

# 즉시 커밋: 수신 즉시 commit (처리량 우선, 유실 허용)
@consumer.on(StockReceive, commit_strategy="immediate")
```

### 핸들러 옵션

```python
@consumer.on(
    SupplyDispatch,
    max_retries=5,              # 재시도 횟수 (기본값: 3)
    retry_backoff_ms=1000,      # 재시도 간격 (기본값: 300ms, exponential)
    dlq_enabled=False,          # DLQ 비활성화
    commit_strategy="after_process",
)
async def handle(event: SupplyDispatch):
    ...
```

## 내부 동작

### Producer

```
send() -> serialize() -> Kafka 전송
                          ↓ 실패
                     재시도 (exponential backoff)
                          ↓ 전부 실패
                     DLQ 토픽 전송
```

### Consumer (after_process)

```
메시지 수신 -> 핸들러 실행 -> 성공 -> offset commit
                            -> 실패 -> 재시도
                                        -> 전부 실패 -> DLQ -> commit
```

### Consumer (immediate)

```
메시지 수신 -> offset commit -> 핸들러 실행 (실패해도 재처리 없음)
```

## MCP 서버 (Claude Code 연동)

담당자 서비스 프로젝트의 `.mcp.json`:

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

Claude Code가 사용할 수 있는 도구:

| Tool | 설명 |
|------|------|
| `list_events` | 사용 가능한 이벤트 목록 |
| `get_event_schema` | 이벤트 스키마 (필드, 타입, 비즈니스 규칙) |
| `get_topic_naming` | 토픽 네이밍 규칙 |
| `get_producer_usage` | Producer 사용법 + 코드 예시 |
| `get_consumer_usage` | Consumer 사용법 + 커밋전략 + 코드 예시 |
| `validate_event_code` | 코드 규격 검증 |

## 예제 앱 실행

```bash
docker compose -f medisolveai_kafka/example/docker-compose.example.yml up -d
# http://localhost:8000  - 예제 UI (Producer/Consumer 시나리오 테스트)
# http://localhost:8080  - Redpanda Console (토픽/메시지 확인)
```

5개 시나리오:
1. **기본** - after_process, 처리 후 commit
2. **즉시커밋** - immediate, 수신 즉시 commit
3. **재시도** - 2번 실패 후 3번째 성공 (exponential backoff)
4. **DLQ** - 항상 실패 -> 재시도 소진 -> DLQ 토픽 이동
5. **검증실패** - quantity=0 -> EventValidationError (Kafka 전송 안 됨)

## 이벤트 추가

이벤트 스키마는 라이브러리 담당자만 수정합니다. 새 이벤트가 필요하면 요청해주세요.

```python
# medisolveai_kafka/events/{도메인}/v1/{행위}.py
from typing import ClassVar, Literal
from medisolveai_kafka.core.base import BaseEvent

class NewEvent(BaseEvent):
    TOPIC: ClassVar[str] = "domain.action.v1"
    VERSION: ClassVar[str] = "1.0"

    # 비즈니스 필드
    field: str

    def validate_business_rules(self) -> None:
        if not self.field:
            raise ValueError("field is required")
```

## 개발

```bash
uv sync --all-extras       # 의존성 설치
uv run pytest tests/ -v    # 테스트 실행 (57 tests)
```
