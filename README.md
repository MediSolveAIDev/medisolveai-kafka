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

Claude Code에서 이 라이브러리 규격에 맞는 코드를 자동 생성할 수 있습니다.

### 1. 설정

담당자 서비스 프로젝트 루트에 `.mcp.json` 생성:

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

### 2. 연동 확인

Claude Code를 실행하면 MCP 서버가 자동 연결됩니다.

```bash
cd my-service-project
claude
# Claude Code가 medisolveai-kafka MCP 서버를 인식
```

### 3. 사용 예시

Claude Code에서 자연어로 요청하면 MCP를 통해 규격에 맞는 코드를 생성합니다:

```
"bay 소모발송 이벤트 보내는 코드 짜줘"
→ Claude가 list_events → get_event_schema → get_producer_usage 호출
→ 규격에 맞는 코드 생성

"어떤 이벤트 사용할 수 있어?"
→ Claude가 list_events 호출
→ [bay/v1] SupplyDispatch, StockReceive, LowStockAlert 응답

"StockReceive 스키마 알려줘"
→ Claude가 get_event_schema 호출
→ 필드, 타입, 비즈니스 규칙, import 경로 응답

"Consumer 즉시커밋으로 구현해줘"
→ Claude가 get_consumer_usage 호출
→ commit_strategy="immediate" 포함한 코드 생성
```

### 4. MCP 도구 목록

| Tool | 설명 | 응답 예시 |
|------|------|----------|
| `list_events` | 사용 가능한 이벤트 목록 | `[bay/v1] SupplyDispatch -> topic: bay.dispatch.v1` |
| `get_event_schema` | 이벤트 스키마 상세 | 필드, 타입, 필수/선택, 비즈니스 규칙, import 경로 |
| `get_topic_naming` | 토픽 네이밍 규칙 | `{도메인}.{행위}.{버전}` + DLQ 규칙 |
| `get_producer_usage` | Producer 사용법 | 환경변수, FastAPI 연동, send() 예시, 에러 처리 |
| `get_consumer_usage` | Consumer 사용법 | 커밋 전략 비교, 핸들러 옵션, 재시도/DLQ 흐름 |
| `validate_event_code` | 코드 규격 검증 | 라이브러리 규격 위반 사항 체크 |

### 5. 워크플로우

```
담당자 서비스에서 Claude Code 실행
    ↓
MCP 서버가 medisolveai-kafka 라이브러리의 events/ 자동 스캔
    ↓
담당자가 자연어로 요청
    ↓
Claude가 MCP 도구로 이벤트/스키마/사용법 조회
    ↓
규격에 맞는 코드 생성 (import 경로, 필수 필드, 비즈니스 룰 포함)
```

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
