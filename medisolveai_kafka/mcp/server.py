import importlib
import inspect
import pkgutil

from mcp.server.fastmcp import FastMCP

from medisolveai_kafka.core.base import BaseEvent

mcp = FastMCP("medisolveai-kafka")


def _scan_events() -> dict[str, dict[str, list[dict]]]:
    """events/ 하위를 동적으로 스캔하여 도메인/버전/이벤트 목록을 반환한다."""
    import medisolveai_kafka.events as events_pkg

    result = {}
    for domain_info in pkgutil.iter_modules(events_pkg.__path__):
        domain_name = domain_info.name
        domain_mod = importlib.import_module(f"medisolveai_kafka.events.{domain_name}")
        result[domain_name] = {}

        for version_info in pkgutil.iter_modules(domain_mod.__path__):
            version_name = version_info.name
            version_pkg = importlib.import_module(
                f"medisolveai_kafka.events.{domain_name}.{version_name}"
            )
            events = []
            # 각 이벤트 파일을 개별 스캔 (__init__.py에 re-export 없음)
            for file_info in pkgutil.iter_modules(version_pkg.__path__):
                file_mod = importlib.import_module(
                    f"medisolveai_kafka.events.{domain_name}.{version_name}.{file_info.name}"
                )
                for name, cls in inspect.getmembers(file_mod, inspect.isclass):
                    if issubclass(cls, BaseEvent) and cls is not BaseEvent:
                        fields = {
                            k: str(v.annotation)
                            for k, v in cls.model_fields.items()
                            if k not in ("event_id", "event_type", "timestamp", "version")
                        }
                        events.append({
                            "class_name": name,
                            "topic": cls.TOPIC,
                            "version": cls.VERSION,
                            "fields": fields,
                        })
            result[domain_name][version_name] = events

    return result


@mcp.tool()
def list_events(domain: str | None = None, version: str | None = None) -> str:
    """사용 가능한 이벤트 목록을 반환한다.

    Args:
        domain: 도메인 필터 (예: "bay"). None이면 전체 조회.
        version: 버전 필터 (예: "v1"). None이면 전체 버전.

    Returns:
        이벤트 목록 (도메인/버전/클래스명/토픽)
    """
    all_events = _scan_events()
    lines = []

    for d, versions in all_events.items():
        if domain and d != domain:
            continue
        for v, events in versions.items():
            if version and v != version:
                continue
            for e in events:
                lines.append(
                    f"[{d}/{v}] {e['class_name']} -> topic: {e['topic']}"
                )

    if not lines:
        return "등록된 이벤트가 없습니다."
    return "\n".join(lines)


@mcp.tool()
def get_event_schema(domain: str, event_name: str, version: str = "v1") -> str:
    """특정 이벤트의 스키마(필드, 타입, 비즈니스 규칙)를 반환한다.

    Args:
        domain: 도메인 (예: "bay")
        event_name: 이벤트 클래스명 (예: "소모발송")
        version: 버전 (기본값: "v1")

    Returns:
        이벤트 스키마 상세 정보
    """
    try:
        version_pkg = importlib.import_module(
            f"medisolveai_kafka.events.{domain}.{version}"
        )
    except ModuleNotFoundError:
        return f"도메인 '{domain}/{version}'을 찾을 수 없습니다."

    # 각 파일에서 클래스 검색
    cls = None
    for file_info in pkgutil.iter_modules(version_pkg.__path__):
        file_mod = importlib.import_module(
            f"medisolveai_kafka.events.{domain}.{version}.{file_info.name}"
        )
        found = getattr(file_mod, event_name, None)
        if found is not None and issubclass(found, BaseEvent):
            cls = found
            break

    if cls is None:
        return f"이벤트 '{event_name}'을 찾을 수 없습니다."

    fields_info = []
    for name, field in cls.model_fields.items():
        if name in ("event_id", "event_type", "timestamp", "version"):
            continue
        required = "필수" if field.is_required() else f"선택 (기본값: {field.default})"
        fields_info.append(f"  - {name}: {field.annotation} [{required}]")

    source = inspect.getsource(cls.validate_business_rules)
    fields_str = "\n".join(fields_info)

    return (
        f"이벤트: {event_name}\n"
        f"토픽: {cls.TOPIC}\n"
        f"버전: {cls.VERSION}\n"
        f"\n"
        f"필드:\n"
        f"{fields_str}\n"
        f"\n"
        f"비즈니스 규칙:\n"
        f"{source}\n"
        f"\n"
        f"import 경로:\n"
        f"  from medisolveai_kafka.events.{domain}.{version} import {event_name}\n"
    )


@mcp.tool()
def get_topic_naming() -> str:
    """토픽 네이밍 규칙과 예시를 반환한다."""
    return (
        "토픽 네이밍 규칙:\n"
        "  {도메인}.{행위}.{버전}\n"
        "\n"
        "예시:\n"
        "  bay.consume.v1        -- 소모발송\n"
        "  bay.receive.v1        -- 입고등록\n"
        "  bay.low-stock.v1      -- 재고부족알림\n"
        "\n"
        "DLQ 토픽:\n"
        "  원본 토픽 + '.dlq' 접미사\n"
        "  예: bay.consume.v1.dlq\n"
    )


@mcp.tool()
def get_producer_usage() -> str:
    """AsyncProducer 사용법과 코드 예시를 반환한다."""
    return (
        "AsyncProducer 사용법\n"
        "====================\n"
        "\n"
        "1. 설치\n"
        "  pip install medisolveai-kafka\n"
        "\n"
        "2. 환경변수 (.env)\n"
        "  KAFKA_BOOTSTRAP_SERVERS=your-kafka:9092  # 필수\n"
        "  KAFKA_ACKS=1                             # 선택 (기본값: '1')\n"
        "  KAFKA_MAX_RETRIES=3                      # 선택 (기본값: 3)\n"
        "  KAFKA_RETRY_BACKOFF_MS=300               # 선택 (기본값: 300)\n"
        "  KAFKA_DLQ_ENABLED=true                   # 선택 (기본값: true)\n"
        "\n"
        "3. FastAPI 연동\n"
        "  from contextlib import asynccontextmanager\n"
        "  from fastapi import FastAPI\n"
        "  from medisolveai_kafka import AsyncProducer\n"
        "  from medisolveai_kafka.events.bay.v1 import 소모발송\n"
        "\n"
        "  producer = AsyncProducer()\n"
        "\n"
        "  @asynccontextmanager\n"
        "  async def lifespan(app: FastAPI):\n"
        "      await producer.start()\n"
        "      yield\n"
        "      await producer.stop()\n"
        "\n"
        "  app = FastAPI(lifespan=lifespan)\n"
        "\n"
        "4. 이벤트 발행\n"
        "  # 기본 사용\n"
        "  await producer.send(소모발송(type='product', id=2, quantity=3))\n"
        "\n"
        "  # 파티션 키 지정 (같은 키는 같은 파티션으로)\n"
        "  await producer.send(소모발송(type='product', id=2, quantity=3), key='product-2')\n"
        "\n"
        "  # 재시도 횟수 오버라이드\n"
        "  await producer.send(소모발송(type='product', id=2, quantity=3), max_retries=5)\n"
        "\n"
        "5. 내부 동작 (담당자가 신경 쓸 필요 없음)\n"
        "  send() 호출\n"
        "      | 실패\n"
        "  재시도 1 (300ms 후)\n"
        "  재시도 2 (600ms 후)\n"
        "  재시도 3 (1200ms 후)\n"
        "      | 전부 실패\n"
        "  DLQ 토픽으로 전송 (bay.consume.v1.dlq)\n"
        "\n"
        "6. 에러 처리\n"
        "  - 직렬화 실패, 비즈니스 룰 위반: 즉시 에러 (재시도 안 함)\n"
        "  - 네트워크/브로커 장애: 자동 재시도\n"
        "  - 재시도 전부 실패: DLQ 전송\n"
        "  - DLQ 전송도 실패: DLQSendError raise\n"
    )


@mcp.tool()
def get_consumer_usage() -> str:
    """AsyncConsumer 사용법과 코드 예시를 반환한다."""
    return (
        "AsyncConsumer 사용법\n"
        "====================\n"
        "\n"
        "1. 환경변수 (.env)\n"
        "  KAFKA_BOOTSTRAP_SERVERS=your-kafka:9092  # 필수\n"
        "  KAFKA_GROUP_ID=my-service-group          # Consumer는 필수\n"
        "  KAFKA_AUTO_OFFSET_RESET=earliest         # 선택 (기본값: 'earliest')\n"
        "  KAFKA_MAX_RETRIES=3                      # 선택 (기본값: 3)\n"
        "  KAFKA_RETRY_BACKOFF_MS=300               # 선택 (기본값: 300)\n"
        "  KAFKA_DLQ_ENABLED=true                   # 선택 (기본값: true)\n"
        "\n"
        "2. FastAPI 연동\n"
        "  from contextlib import asynccontextmanager\n"
        "  from fastapi import FastAPI\n"
        "  from medisolveai_kafka import AsyncConsumer\n"
        "  from medisolveai_kafka.events.bay.v1 import 소모발송\n"
        "\n"
        "  consumer = AsyncConsumer()\n"
        "\n"
        "  @consumer.on(소모발송)\n"
        "  async def handle_consume(event: 소모발송):\n"
        "      await some_service.process(event)\n"
        "\n"
        "  @asynccontextmanager\n"
        "  async def lifespan(app: FastAPI):\n"
        "      await consumer.start()\n"
        "      yield\n"
        "      await consumer.stop()\n"
        "\n"
        "  app = FastAPI(lifespan=lifespan)\n"
        "\n"
        "3. 커밋 전략\n"
        "\n"
        "  [after_process -- 기본값]\n"
        "  처리 성공 후에만 offset commit. 실패 시 재시도 후 DLQ.\n"
        "  데이터 유실이 없어야 할 때 사용.\n"
        "\n"
        "  @consumer.on(소모발송)\n"
        "  async def handle(event: 소모발송):\n"
        "      await some_service.process(event)\n"
        "\n"
        "  [immediate -- 즉시 커밋]\n"
        "  메시지 수신 즉시 offset commit 후 핸들러 실행.\n"
        "  실패해도 재처리 없음. 처리량 우선, 유실 허용 시 사용.\n"
        "\n"
        "  @consumer.on(소모발송, commit_strategy='immediate')\n"
        "  async def handle(event: 소모발송):\n"
        "      await some_service.process(event)\n"
        "\n"
        "4. 핸들러 옵션 커스텀\n"
        "\n"
        "  @consumer.on(\n"
        "      소모발송,\n"
        "      max_retries=5,             # 재시도 횟수 (기본값: 3)\n"
        "      retry_backoff_ms=1000,     # 재시도 간격 (기본값: 300ms)\n"
        "      dlq_enabled=False,         # DLQ 비활성화\n"
        "      commit_strategy='after_process',  # 커밋 전략\n"
        "  )\n"
        "  async def handle(event: 소모발송):\n"
        "      await some_service.process(event)\n"
        "\n"
        "5. 내부 동작 (담당자가 신경 쓸 필요 없음)\n"
        "\n"
        "  [after_process]\n"
        "  메시지 수신 -> 핸들러 실행 -> 성공 -> offset commit\n"
        "                             -> 실패 -> 재시도 (exponential backoff)\n"
        "                                        -> 전부 실패 -> DLQ 전송 -> offset commit\n"
        "\n"
        "  [immediate]\n"
        "  메시지 수신 -> offset commit -> 핸들러 실행 (실패해도 재처리 없음)\n"
        "\n"
        "6. 에러 처리\n"
        "  - NonRetryableError (스키마 불일치 등): 재시도 없이 즉시 DLQ\n"
        "  - 그 외 에러: exponential backoff 재시도 후 전부 실패 시 DLQ\n"
        "  - DLQ 비활성화 시: 로깅 후 메시지 폐기\n"
    )


@mcp.tool()
def validate_event_code(code: str) -> str:
    """담당자가 작성한 코드가 라이브러리 규격에 맞는지 검증한다.

    Args:
        code: 검증할 Python 코드 문자열

    Returns:
        검증 결과 (통과 또는 위반 사항 목록)
    """
    issues = []

    if "BaseEvent" not in code:
        issues.append(
            "이벤트를 직접 정의하지 마세요. "
            "medisolveai_kafka.events.*에서 import하세요."
        )

    if "from medisolveai_kafka" not in code:
        issues.append("medisolveai_kafka에서 import해야 합니다.")

    if "AsyncProducer" in code or "AsyncConsumer" in code:
        if "lifespan" not in code and "start" not in code:
            issues.append(
                "Producer/Consumer는 start()/stop()을 호출해야 합니다. "
                "FastAPI lifespan 패턴을 사용하세요."
            )

    if not issues:
        return "검증 통과: 라이브러리 규격에 맞습니다."

    return "검증 결과:\n" + "\n".join(f"  - {issue}" for issue in issues)
