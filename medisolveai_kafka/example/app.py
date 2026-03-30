"""
예제 UI 서버
실행: docker compose -f medisolveai_kafka/example/docker-compose.example.yml up -d
UI: http://localhost:8000
"""

import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), ".env.example")
if os.path.exists(env_path):
    load_dotenv(env_path)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from medisolveai_kafka import AsyncConsumer, AsyncProducer
from medisolveai_kafka.events.bay.v1.dispatch import SupplyDispatch
from medisolveai_kafka.events.bay.v1.low_stock import LowStockAlert
from medisolveai_kafka.events.bay.v1.receive import StockReceive

producer = AsyncProducer()
consumer = AsyncConsumer()

ws_clients: list[WebSocket] = []

# 재시도 시나리오용 카운터
retry_counters: dict[str, int] = {}
# 재시도 시나리오 대상 event_id 목록
retry_target_ids: set[str] = set()


async def broadcast(data: dict):
    for ws in ws_clients[:]:
        try:
            await ws.send_json(data)
        except Exception:
            ws_clients.remove(ws)


# === SupplyDispatch: 기본 (after_process) + 재시도 시나리오 ===
@consumer.on(SupplyDispatch, max_retries=3, retry_backoff_ms=500)
async def handle_dispatch(event: SupplyDispatch):
    # 재시도 시나리오 대상이면 2번 실패 후 성공
    if event.event_id in retry_target_ids:
        key = event.event_id
        retry_counters[key] = retry_counters.get(key, 0) + 1
        attempt = retry_counters[key]

        if attempt <= 2:
            await broadcast({
                "scenario": "retry",
                "event_type": "SupplyDispatch",
                "status": "retrying",
                "topic": event.TOPIC,
                "event_id": event.event_id,
                "timestamp": event.timestamp.isoformat(),
                "data": {"attempt": attempt, "max_retries": 3},
                "message": f"시도 {attempt}/4 실패 - {500 * (2 ** (attempt-1))}ms 후 재시도...",
            })
            raise RuntimeError(f"일시적 장애 (시도 {attempt})")

        await broadcast({
            "scenario": "retry",
            "event_type": "SupplyDispatch",
            "status": "success",
            "topic": event.TOPIC,
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "data": {"attempt": attempt, "max_retries": 3},
            "message": f"시도 {attempt}/4 성공! 재시도 후 복구 -> offset commit",
        })
        retry_counters.pop(key, None)
        retry_target_ids.discard(key)
        return

    # 기본 시나리오: 정상 처리
    await broadcast({
        "scenario": "after_process",
        "event_type": "SupplyDispatch",
        "status": "success",
        "topic": event.TOPIC,
        "event_id": event.event_id,
        "timestamp": event.timestamp.isoformat(),
        "data": {"type": event.type, "id": event.id, "quantity": event.quantity},
        "message": "핸들러 처리 완료 -> offset commit",
    })


# === StockReceive: 즉시 커밋 (immediate) ===
@consumer.on(StockReceive, commit_strategy="immediate")
async def handle_receive(event: StockReceive):
    await broadcast({
        "scenario": "immediate",
        "event_type": "StockReceive",
        "status": "success",
        "topic": event.TOPIC,
        "event_id": event.event_id,
        "timestamp": event.timestamp.isoformat(),
        "data": {"type": event.type, "id": event.id, "quantity": event.quantity, "supplier": event.supplier},
        "message": "offset commit 즉시 완료 -> 핸들러 처리 (실패해도 재처리 없음)",
    })


# === LowStockAlert: DLQ (항상 실패) ===
@consumer.on(LowStockAlert, max_retries=2, retry_backoff_ms=300)
async def handle_low_stock(event: LowStockAlert):
    key = event.event_id
    retry_counters[key] = retry_counters.get(key, 0) + 1
    attempt = retry_counters[key]

    await broadcast({
        "scenario": "dlq",
        "event_type": "LowStockAlert",
        "status": "retrying",
        "topic": event.TOPIC,
        "event_id": event.event_id,
        "timestamp": event.timestamp.isoformat(),
        "data": {"attempt": attempt, "max_retries": 2},
        "message": f"시도 {attempt}/3 실패" + (" -> DLQ 전송 예정" if attempt > 2 else " - 재시도 대기..."),
    })
    raise RuntimeError("처리 불가능한 메시지")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    await consumer.start()
    yield
    await consumer.stop()
    await producer.stop()


app = FastAPI(lifespan=lifespan)

HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>medisolveai-kafka Example</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', sans-serif; background: #1a1a2e; color: #eee; height: 100vh; display: flex; flex-direction: column; }
  header { background: #16213e; padding: 14px 24px; text-align: center; border-bottom: 2px solid #0f3460; }
  header h1 { font-size: 18px; color: #e94560; }
  .container { display: flex; flex: 1; overflow: hidden; }
  .panel { flex: 1; padding: 16px; display: flex; flex-direction: column; overflow-y: auto; }
  .panel-left { border-right: 2px solid #0f3460; max-width: 480px; }
  .panel h2 { font-size: 15px; color: #e94560; margin-bottom: 12px; padding-bottom: 6px; border-bottom: 1px solid #333; }
  .tabs { display: flex; gap: 4px; margin-bottom: 12px; }
  .tab { padding: 6px 12px; font-size: 11px; border: 1px solid #333; border-radius: 4px; background: #1a1a2e; color: #888; cursor: pointer; }
  .tab.active { background: #e94560; color: white; border-color: #e94560; }
  .scenario { display: none; }
  .scenario.active { display: block; }
  .card { background: #16213e; border-radius: 8px; padding: 14px; margin-bottom: 10px; }
  .card h3 { font-size: 13px; color: #e94560; margin-bottom: 4px; }
  .card .desc { font-size: 11px; color: #666; margin-bottom: 10px; line-height: 1.5; }
  .card .config { font-size: 11px; color: #4ecca3; background: #0d1520; border-radius: 4px; padding: 8px; margin-bottom: 10px; font-family: monospace; line-height: 1.6; }
  .fields { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 10px; }
  .fields label { font-size: 11px; color: #999; }
  .fields input, .fields select { width: 100%; padding: 5px 7px; border: 1px solid #333; border-radius: 4px; background: #1a1a2e; color: #eee; font-size: 12px; }
  .btn { width: 100%; padding: 8px; border: none; border-radius: 4px; color: white; cursor: pointer; font-size: 12px; font-weight: bold; }
  .btn-red { background: #e94560; } .btn-red:hover { background: #c73652; }
  .btn-orange { background: #f9a826; color: #1a1a2e; } .btn-orange:hover { background: #e09520; }
  .btn-green { background: #4ecca3; color: #1a1a2e; } .btn-green:hover { background: #3db890; }
  .btn-purple { background: #9b59b6; } .btn-purple:hover { background: #8e44ad; }
  .flow { font-size: 10px; color: #555; background: #0d1520; border-radius: 4px; padding: 8px; margin-bottom: 10px; font-family: monospace; line-height: 1.6; }
  .flow .highlight { color: #4ecca3; }

  .log { flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; padding-top: 4px; }
  .log-item { background: #16213e; border-radius: 6px; padding: 10px; font-size: 11px; border-left: 3px solid #0f3460; animation: fadeIn 0.3s; }
  .log-item .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; }
  .log-item .event-name { font-weight: bold; }
  .log-item .badge { font-size: 9px; padding: 2px 6px; border-radius: 3px; font-weight: bold; }
  .badge-success { background: #4ecca3; color: #1a1a2e; }
  .badge-retrying { background: #f9a826; color: #1a1a2e; }
  .badge-dlq { background: #e94560; color: white; }
  .badge-error { background: #e74c3c; color: white; }
  .badge-immediate { background: #9b59b6; color: white; }
  .log-item .time { color: #555; font-size: 10px; }
  .log-item .msg { color: #aaa; margin-top: 3px; }
  .log-item .data { color: #666; margin-top: 2px; font-family: monospace; font-size: 10px; }
  .log-item.success { border-left-color: #4ecca3; }
  .log-item.retrying { border-left-color: #f9a826; }
  .log-item.dlq { border-left-color: #e94560; }
  .log-item.error { border-left-color: #e74c3c; }
  .log-item.immediate { border-left-color: #9b59b6; }
  .empty { color: #555; text-align: center; margin-top: 40px; font-size: 12px; }
  .status { font-size: 11px; color: #4ecca3; margin-bottom: 8px; }
  .filter-bar { display: flex; gap: 4px; margin-bottom: 8px; flex-wrap: wrap; }
  .filter { font-size: 10px; padding: 3px 8px; border-radius: 3px; border: 1px solid #333; background: #1a1a2e; color: #888; cursor: pointer; }
  .filter.active { border-color: #e94560; color: #e94560; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }
</style>
</head>
<body>
<header><h1>medisolveai-kafka Scenario Test</h1></header>
<div class="container">
  <div class="panel panel-left">
    <h2>Producer</h2>
    <div class="tabs">
      <div class="tab active" onclick="switchTab(this,'basic')">1.기본</div>
      <div class="tab" onclick="switchTab(this,'immediate')">2.즉시커밋</div>
      <div class="tab" onclick="switchTab(this,'retry')">3.재시도</div>
      <div class="tab" onclick="switchTab(this,'dlq')">4.DLQ</div>
      <div class="tab" onclick="switchTab(this,'validation')">5.검증실패</div>
    </div>

    <div class="scenario active" id="tab-basic">
      <div class="card">
        <h3>1. after_process (기본 모드)</h3>
        <div class="desc">핸들러 처리 성공 후에만 offset commit. 실패 시 재처리.</div>
        <div class="config">@consumer.on(SupplyDispatch)<br># commit_strategy="after_process" (기본값)</div>
        <div class="flow">send() -> Kafka -> 핸들러 실행 -> <span class="highlight">성공 -> offset commit</span></div>
        <div class="fields">
          <div><label>type</label><select id="b-type"><option value="product">product</option><option value="material">material</option></select></div>
          <div><label>id</label><input id="b-id" type="number" value="1"></div>
          <div><label>quantity</label><input id="b-qty" type="number" value="10"></div>
        </div>
        <button class="btn btn-green" onclick="pub('basic')">SupplyDispatch 발행</button>
      </div>
    </div>

    <div class="scenario" id="tab-immediate">
      <div class="card">
        <h3>2. immediate (즉시 커밋)</h3>
        <div class="desc">메시지 수신 즉시 offset commit. 핸들러가 실패해도 재처리 없음.<br>처리량 우선, 유실 허용 시 사용.</div>
        <div class="config">@consumer.on(StockReceive, commit_strategy="immediate")</div>
        <div class="flow">send() -> Kafka -> <span class="highlight">즉시 offset commit</span> -> 핸들러 실행 (실패해도 끝)</div>
        <div class="fields">
          <div><label>type</label><select id="i-type"><option value="product">product</option><option value="material">material</option></select></div>
          <div><label>id</label><input id="i-id" type="number" value="2"></div>
          <div><label>quantity</label><input id="i-qty" type="number" value="50"></div>
          <div><label>supplier</label><input id="i-sup" value="ABC Corp"></div>
        </div>
        <button class="btn btn-purple" onclick="pub('immediate')">StockReceive 발행</button>
      </div>
    </div>

    <div class="scenario" id="tab-retry">
      <div class="card">
        <h3>3. 재시도 후 성공</h3>
        <div class="desc">핸들러가 2번 실패 후 3번째에 성공. exponential backoff 적용.</div>
        <div class="config">@consumer.on(SupplyDispatch, max_retries=3, retry_backoff_ms=500)</div>
        <div class="flow">핸들러 실패 -> <span class="highlight">500ms 대기 -> 재시도</span> -> 1000ms 대기 -> 재시도 -> 성공 -> commit</div>
        <div class="fields">
          <div><label>type</label><select id="r-type"><option value="product">product</option><option value="material">material</option></select></div>
          <div><label>id</label><input id="r-id" type="number" value="1"></div>
          <div><label>quantity</label><input id="r-qty" type="number" value="30"></div>
        </div>
        <button class="btn btn-orange" onclick="pub('retry')">SupplyDispatch 발행 (2번 실패 후 성공)</button>
      </div>
    </div>

    <div class="scenario" id="tab-dlq">
      <div class="card">
        <h3>4. DLQ 전송</h3>
        <div class="desc">핸들러가 항상 실패. 재시도 소진 후 DLQ 토픽으로 이동.</div>
        <div class="config">@consumer.on(LowStockAlert, max_retries=2, retry_backoff_ms=300)<br># 3회 전부 실패 -> bay.low-stock.v1.dlq</div>
        <div class="flow">실패 -> 300ms -> 실패 -> 600ms -> 실패 -> <span class="highlight">DLQ 전송</span> -> offset commit</div>
        <div class="fields">
          <div><label>type</label><select id="d-type"><option value="product">product</option><option value="material">material</option></select></div>
          <div><label>id</label><input id="d-id" type="number" value="5"></div>
          <div><label>current_quantity</label><input id="d-cur" type="number" value="1"></div>
          <div><label>threshold</label><input id="d-thr" type="number" value="10"></div>
        </div>
        <button class="btn btn-red" onclick="pub('dlq')">LowStockAlert 발행 (항상 실패)</button>
      </div>
    </div>

    <div class="scenario" id="tab-validation">
      <div class="card">
        <h3>5. 비즈니스 룰 검증 실패</h3>
        <div class="desc">serialize() 시 validate_business_rules() 호출.<br>quantity=0이면 발행 자체가 거부됨. Kafka까지 가지 않음.</div>
        <div class="config">def validate_business_rules(self):<br>    if self.quantity <= 0:<br>        raise ValueError("수량은 0보다 커야 합니다.")<br># -> EventValidationError (재시도 안 함)</div>
        <div class="flow">send() -> serialize() -> <span class="highlight">validate 실패 -> EventValidationError</span> (Kafka 전송 안 됨)</div>
        <div class="fields">
          <div><label>type</label><select id="v-type"><option value="product">product</option><option value="material">material</option></select></div>
          <div><label>id</label><input id="v-id" type="number" value="99"></div>
          <div><label>quantity (0으로 테스트)</label><input id="v-qty" type="number" value="0"></div>
        </div>
        <button class="btn btn-red" onclick="pub('validation')">SupplyDispatch 발행 (검증 실패)</button>
      </div>
    </div>
  </div>

  <div class="panel">
    <h2>Consumer</h2>
    <div class="status" id="ws-status">연결 중...</div>
    <div class="filter-bar">
      <div class="filter active" data-filter="all" onclick="setFilter(this,'all')">전체</div>
      <div class="filter" data-filter="success" onclick="setFilter(this,'success')">성공</div>
      <div class="filter" data-filter="retrying" onclick="setFilter(this,'retrying')">재시도</div>
      <div class="filter" data-filter="dlq" onclick="setFilter(this,'dlq')">DLQ</div>
      <div class="filter" data-filter="immediate" onclick="setFilter(this,'immediate')">즉시커밋</div>
      <div class="filter" data-filter="error" onclick="setFilter(this,'error')">에러</div>
    </div>
    <div class="log" id="log">
      <div class="empty">시나리오 탭을 선택하고 발행 버튼을 누르세요</div>
    </div>
  </div>
</div>
<script>
const log = document.getElementById('log');
const statusEl = document.getElementById('ws-status');
let ws, currentFilter = 'all';

function switchTab(el, name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.scenario').forEach(s => s.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('tab-' + name).classList.add('active');
}
function setFilter(el, f) {
  currentFilter = f;
  document.querySelectorAll('.filter').forEach(e => e.classList.toggle('active', e.dataset.filter === f));
  document.querySelectorAll('.log-item').forEach(e => { e.style.display = (f === 'all' || e.dataset.status === f) ? '' : 'none'; });
}
function connect() {
  ws = new WebSocket('ws://' + location.host + '/ws');
  ws.onopen = () => { statusEl.textContent = 'Connected'; statusEl.style.color = '#4ecca3'; };
  ws.onclose = () => { statusEl.textContent = 'Disconnected'; statusEl.style.color = '#e94560'; setTimeout(connect, 2000); };
  ws.onmessage = (e) => {
    const d = JSON.parse(e.data);
    const empty = log.querySelector('.empty'); if (empty) empty.remove();
    const st = d.scenario === 'immediate' ? 'immediate' : (d.status || 'success');
    const badges = {success:'성공', retrying:'재시도', dlq:'DLQ', error:'에러', immediate:'즉시커밋'};
    const item = document.createElement('div');
    item.className = 'log-item ' + st;
    item.dataset.status = st;
    item.innerHTML =
      '<div class="header"><span class="event-name">' + d.event_type + '</span><span class="badge badge-' + st + '">' + (badges[st]||st) + '</span></div>' +
      '<div class="time">' + (d.timestamp||'') + ' | ' + (d.topic||'') + '</div>' +
      '<div class="msg">' + (d.message||'') + '</div>' +
      '<div class="data">' + JSON.stringify(d.data||{}) + '</div>';
    if (currentFilter !== 'all' && st !== currentFilter) item.style.display = 'none';
    log.prepend(item);
  };
}
connect();

async function pub(scenario) {
  let body = { scenario };
  if (scenario === 'basic') Object.assign(body, { event_type:'dispatch', type:val('b-type'), id:num('b-id'), quantity:num('b-qty') });
  else if (scenario === 'immediate') Object.assign(body, { event_type:'receive', type:val('i-type'), id:num('i-id'), quantity:num('i-qty'), supplier:val('i-sup') });
  else if (scenario === 'retry') Object.assign(body, { event_type:'dispatch', type:val('r-type'), id:num('r-id'), quantity:num('r-qty') });
  else if (scenario === 'dlq') Object.assign(body, { event_type:'low_stock', type:val('d-type'), id:num('d-id'), current_quantity:num('d-cur'), threshold:num('d-thr') });
  else if (scenario === 'validation') Object.assign(body, { event_type:'dispatch', type:val('v-type'), id:num('v-id'), quantity:num('v-qty') });
  const res = await fetch('/publish', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
  const result = await res.json();
  if (result.error) {
    const empty = log.querySelector('.empty'); if (empty) empty.remove();
    const item = document.createElement('div');
    item.className = 'log-item error'; item.dataset.status = 'error';
    item.innerHTML = '<div class="header"><span class="event-name">Producer Error</span><span class="badge badge-error">에러</span></div><div class="msg">' + result.error + '</div>';
    if (currentFilter !== 'all' && currentFilter !== 'error') item.style.display = 'none';
    log.prepend(item);
  }
}
function val(id) { return document.getElementById(id).value; }
function num(id) { return +document.getElementById(id).value; }
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_clients.remove(websocket)


@app.post("/publish")
async def publish(body: dict):
    scenario = body.pop("scenario", "basic")
    event_type = body.pop("event_type")

    try:
        if event_type == "dispatch":
            event = SupplyDispatch(**body)
        elif event_type == "receive":
            event = StockReceive(**body)
        elif event_type == "low_stock":
            event = LowStockAlert(**body)
        else:
            return {"error": f"unknown event_type: {event_type}"}

        # 재시도 시나리오: 이 event_id를 재시도 대상으로 등록
        if scenario == "retry":
            retry_target_ids.add(event.event_id)

        await producer.send(event)
        return {"status": "ok", "event_id": event.event_id, "scenario": scenario}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
