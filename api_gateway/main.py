from textwrap import dedent
from typing import Any, Dict, List, Optional

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import uvicorn
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api_gateway.aetherbus_extreme import AetherBusExtreme
from tools.contracts.contract_checker import ContractChecker

try:
    import tachyon_core

    tachyon_engine = tachyon_core.TachyonEngine()
    HAS_BRAIN = True
    print("🧠 Tachyon Core: CONNECTED")
except ImportError:
    tachyon_engine = None
    HAS_BRAIN = False
    print("⚠️ Tachyon Core: NOT FOUND (Running in Gateway Demo Mode)")

app = FastAPI(title="AetherBus Tachyon Control Surface")
bus = AetherBusExtreme()
immune_system = ContractChecker()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ContractLoadRequest(BaseModel):
    contract_id: str = Field(..., min_length=1, max_length=128)
    schema_payload: Dict[str, Any] = Field(..., alias="schema")

    model_config = {"populate_by_name": True}


class SystemAlert(BaseModel):
    level: str = Field(default="info")
    message: str = Field(..., min_length=1, max_length=500)


class AdminMutationMetadata(BaseModel):
    actor: str = Field(default="api_gateway")
    requested_reason: Optional[str] = None


class DLQReplayRequest(AdminMutationMetadata):
    message_ids: List[str] = Field(..., min_length=1)
    target_consumer_id: str = Field(..., min_length=1)
    target_topic: str = Field(..., min_length=1)
    confirm: str = Field(..., min_length=1)


class DLQPurgeRequest(AdminMutationMetadata):
    message_ids: List[str] = Field(default_factory=list)
    consumer_id: Optional[str] = None
    topic: Optional[str] = None
    reason: Optional[str] = None
    confirm: str = Field(..., min_length=1)


class ManualDeadLetterRequest(AdminMutationMetadata):
    message_id: str = Field(..., min_length=1)
    consumer_id: Optional[str] = None
    session_id: Optional[str] = None
    topic: str = Field(..., min_length=1)
    payload: List[int] = Field(default_factory=list)
    priority: Optional[str] = None
    enqueue_sequence: int = 0
    attempt: int = 0
    reason: str = Field(default="manual_admin_action", min_length=1)


DLQ_PATH = Path(os.getenv("DLQ_STORE_PATH", os.getenv("WAL_PATH", "./data/direct_delivery.wal") + ".dlq"))
SCHEDULED_PATH = Path(os.getenv("SCHEDULED_STORE_PATH", os.getenv("WAL_PATH", "./data/direct_delivery.wal") + ".scheduled"))
AUDIT_PATH = Path(os.getenv("AUDIT_STORE_PATH", os.getenv("WAL_PATH", "./data/direct_delivery.wal") + ".audit"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


def _admin_guard(admin_token: Optional[str]) -> None:
    if ADMIN_TOKEN and admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Admin token required.")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text() or "{}")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_audit() -> List[Dict[str, Any]]:
    if not AUDIT_PATH.exists():
        return []
    lines = [line.strip() for line in AUDIT_PATH.read_text().splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _append_audit_event(operation: str, actor: str, target_message_ids: List[str], requested_reason: Optional[str], prior_state: List[Dict[str, Any]], resulting_state: List[Dict[str, Any]], timestamp: Optional[str] = None) -> Dict[str, Any]:
    timestamp = timestamp or _utcnow()
    target_message_ids = sorted({message_id for message_id in target_message_ids if message_id})
    existing = _load_audit()
    prev_hash = existing[-1]["hash"] if existing else ""
    payload = {
        "actor": actor or "unknown",
        "timestamp": timestamp,
        "operation": operation,
        "target_message_ids": target_message_ids,
        "requested_reason": requested_reason or "",
        "prior_state": prior_state,
        "resulting_state": resulting_state,
        "prev_hash": prev_hash,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    event = {
        "event_id": f"audit_{digest[:16]}",
        **payload,
        "hash": digest,
    }
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")
    return event


def _query_audit(message_id: Optional[str] = None, actor: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
    events = _load_audit()
    def _matches_time(value: str, lower: Optional[str], upper: Optional[str]) -> bool:
        if lower and value < lower:
            return False
        if upper and value > upper:
            return False
        return True
    filtered = []
    for event in events:
        if actor and event.get("actor") != actor:
            continue
        if not _matches_time(event.get("timestamp", ""), start, end):
            continue
        if message_id:
            targets = set(event.get("target_message_ids", []))
            targets.update(item.get("message_id") for item in event.get("prior_state", []))
            targets.update(item.get("message_id") for item in event.get("resulting_state", []))
            if message_id not in targets:
                continue
        filtered.append(event)
    filtered.sort(key=lambda item: (item.get("timestamp", ""), item.get("event_id", "")))
    return filtered


def _list_dlq(consumer_id: Optional[str] = None, topic: Optional[str] = None, reason: Optional[str] = None) -> List[Dict[str, Any]]:
    records = []
    for message_id, record in _load_json(DLQ_PATH).items():
        record = dict(record)
        record["message_id"] = message_id
        if consumer_id and record.get("consumer_id") != consumer_id:
            continue
        if topic and record.get("topic") != topic:
            continue
        if reason and record.get("reason") != reason:
            continue
        if "payload" in record:
            try:
                record["payload_b64"] = base64.b64encode(bytes(record["payload"])).decode()
            except Exception:
                pass
        records.append(record)
    records.sort(key=lambda item: (item.get("dead_lettered_at", ""), item["message_id"]))
    return records


HOME_PAGE = dedent(
    """
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>AetherBus Tachyon</title>
        <style>
          :root {
            color-scheme: dark;
            --bg: #07111f;
            --panel: rgba(10, 22, 39, 0.86);
            --border: rgba(110, 168, 255, 0.18);
            --accent: #67e8f9;
            --accent2: #8b5cf6;
            --text: #e5eefc;
            --muted: #97a8c2;
            --ok: #34d399;
            --warn: #f59e0b;
          }
          * { box-sizing: border-box; }
          body {
            margin: 0;
            font-family: Inter, system-ui, sans-serif;
            background: radial-gradient(circle at top, #102647 0%, var(--bg) 55%);
            color: var(--text);
          }
          .shell { max-width: 1200px; margin: 0 auto; padding: 32px 20px 56px; }
          .hero, .grid, .timeline { display: grid; gap: 20px; }
          .hero { grid-template-columns: 1.4fr 1fr; align-items: stretch; }
          .card {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 24px;
            box-shadow: 0 18px 60px rgba(0, 0, 0, 0.26);
            backdrop-filter: blur(14px);
          }
          .hero-card { padding: 30px; }
          h1 { margin: 0 0 12px; font-size: clamp(2.2rem, 4vw, 4rem); }
          h2 { margin: 0 0 16px; font-size: 1.2rem; }
          p { color: var(--muted); line-height: 1.6; }
          .badge { display:inline-flex; gap:8px; align-items:center; padding:8px 12px; border-radius:999px; background:rgba(103,232,249,.12); color:var(--accent); font-size:.9rem; }
          .stats, .grid { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
          .stat { padding: 20px; }
          .metric { font-size: 2rem; font-weight: 700; margin-top: 10px; }
          .label { color: var(--muted); font-size: .92rem; }
          .action-list, .feed { list-style: none; margin: 0; padding: 0; }
          .action-list li, .feed li { padding: 14px 0; border-top: 1px solid rgba(255,255,255,.06); }
          .action-list li:first-child, .feed li:first-child { border-top: 0; padding-top: 0; }
          .pill { display:inline-block; padding:4px 10px; border-radius:999px; font-size:.78rem; background:rgba(139,92,246,.18); color:#d8c7ff; }
          .layout { display:grid; grid-template-columns: 1.1fr .9fr; gap:20px; margin-top:20px; }
          .panel { padding: 24px; }
          code { color: var(--accent); }
          .footer { margin-top: 18px; color: var(--muted); font-size: .9rem; }
          @media (max-width: 900px) { .hero, .layout { grid-template-columns: 1fr; } }
        </style>
      </head>
      <body>
        <main class="shell">
          <section class="hero">
            <article class="card hero-card">
              <span class="badge">⚡ AetherBus Tachyon Control Surface</span>
              <h1>Observe the broker, contracts, and recovery state from one launch page.</h1>
              <p>
                This redesigned landing page exposes runtime health, contract registry status,
                WebSocket activity, and the most important operating paths for the gateway.
              </p>
              <div class="footer">Primary endpoints: <code>/api/system/overview</code>, <code>/api/contracts/load</code>, <code>/ws/feed</code></div>
            </article>
            <article class="card panel">
              <h2>Live status</h2>
              <div class="stats">
                <div class="stat"><div class="label">Gateway</div><div class="metric" id="gateway-status">ONLINE</div></div>
                <div class="stat"><div class="label">Brain mode</div><div class="metric" id="brain-mode">Loading…</div></div>
                <div class="stat"><div class="label">Known contracts</div><div class="metric" id="contract-count">0</div></div>
              </div>
            </article>
          </section>

          <section class="layout">
            <article class="card panel">
              <h2>Operator playbook</h2>
              <ul class="action-list">
                <li><span class="pill">1</span> Load contracts with <code>POST /api/contracts/load</code> using an object schema.</li>
                <li><span class="pill">2</span> Mint demo identities with <code>POST /api/genesis/mint</code> when the Tachyon core is attached.</li>
                <li><span class="pill">3</span> Stream events from <code>/ws/feed</code> for dashboards and local tooling.</li>
                <li><span class="pill">4</span> Inspect the latest gateway activity via <code>/api/system/overview</code>.</li>
              </ul>
            </article>
            <article class="card panel">
              <h2>Recent bus events</h2>
              <ul class="feed" id="event-feed">
                <li>Waiting for gateway telemetry…</li>
              </ul>
            </article>
          </section>
        </main>
        <script>
          async function refreshOverview() {
            const response = await fetch('/api/system/overview');
            const data = await response.json();
            document.getElementById('brain-mode').textContent = data.mode;
            document.getElementById('contract-count').textContent = String(data.contract_count);
            const feed = document.getElementById('event-feed');
            feed.innerHTML = '';
            const items = data.recent_events.length ? data.recent_events : [{topic: 'SYSTEM', payload: {message: 'No activity yet'}}];
            for (const item of items) {
              const li = document.createElement('li');
              const payload = typeof item.payload === 'object' ? JSON.stringify(item.payload) : String(item.payload);
              li.textContent = `${item.topic} — ${payload}`;
              feed.appendChild(li);
            }
          }
          refreshOverview();
          setInterval(refreshOverview, 5000);
        </script>
      </body>
    </html>
    """
)


@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    return HTMLResponse(HOME_PAGE)


@app.get("/api/system/overview")
async def system_overview() -> Dict[str, Any]:
    return {
        "status": "ONLINE",
        "brain_connected": HAS_BRAIN,
        "mode": "Tachyon Connected" if HAS_BRAIN else "Gateway Demo Mode",
        "contract_count": len(immune_system.schemas),
        "contracts": sorted(immune_system.schemas.keys()),
        "recent_events": bus.recent_events(limit=8),
    }


@app.post("/api/contracts/load")
async def load_contract(request: ContractLoadRequest) -> Dict[str, Any]:
    schema_str = json.dumps(request.schema_payload)
    success = immune_system.load_dynamic_schema(request.contract_id, schema_str)
    if not success:
        raise HTTPException(status_code=400, detail="Invalid schema format.")

    await bus.emit(
        "CONTRACT_LOADED",
        {"contract_id": request.contract_id, "field_count": len(request.schema_payload.get("properties", {}))},
    )
    return {"status": "Contract loaded successfully", "contract_id": request.contract_id}


@app.post("/api/system/alerts")
async def publish_alert(alert: SystemAlert) -> Dict[str, str]:
    await bus.emit("SYSTEM_ALERT", alert.model_dump())
    return {"status": "Alert published"}


@app.post("/api/genesis/mint")
async def mint_agent(seed: int = 1000) -> Dict[str, Any]:
    if not HAS_BRAIN or tachyon_engine is None:
        raise HTTPException(status_code=503, detail="Brain (Tachyon Core) is not connected.")

    try:
        sentinel, catalyst, harmonizer = tachyon_engine.mint_starter_deck(seed)
    except Exception as exc:
        await bus.emit("SYSTEM_ALERT", {"level": "critical", "message": f"Tachyon Core Error during minting: {exc}"})
        raise HTTPException(status_code=500, detail="Error communicating with Tachyon Core.") from exc

    agents = {
        "sentinel": json.loads(tachyon_engine.inspect_identity_json(sentinel)),
        "catalyst": json.loads(tachyon_engine.inspect_identity_json(catalyst)),
        "harmonizer": json.loads(tachyon_engine.inspect_identity_json(harmonizer)),
    }

    is_valid_intent = immune_system.validate_intent(agents["sentinel"], expected_intent="guardian")
    if not is_valid_intent:
        await bus.emit("SYSTEM_ALERT", {"level": "warning", "message": "Minted agent failed intent validation!"})
        agents["validation_warning"] = "Sentinel intent mismatch"

    await bus.emit("AGENT_MINTED", agents)
    return agents


@app.websocket("/ws/feed")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()

    async def synapse_handler(event_data: Dict[str, Any]) -> None:
        try:
            await websocket.send_json(event_data)
        except Exception:
            pass

    async def mint_handler(payload: Any) -> None:
        await synapse_handler({"topic": "AGENT_MINTED", "payload": payload})

    async def alert_handler(payload: Any) -> None:
        await synapse_handler({"topic": "SYSTEM_ALERT", "payload": payload})

    async def contract_handler(payload: Any) -> None:
        await synapse_handler({"topic": "CONTRACT_LOADED", "payload": payload})

    await bus.subscribe("AGENT_MINTED", mint_handler)
    await bus.subscribe("SYSTEM_ALERT", alert_handler)
    await bus.subscribe("CONTRACT_LOADED", contract_handler)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print("🔌 Dashboard disconnected from /ws/feed")
    finally:
        await bus.unsubscribe("AGENT_MINTED", mint_handler)
        await bus.unsubscribe("SYSTEM_ALERT", alert_handler)
        await bus.unsubscribe("CONTRACT_LOADED", contract_handler)




@app.get("/api/admin/dlq/records")
async def list_dead_letters(consumer_id: Optional[str] = None, topic: Optional[str] = None, reason: Optional[str] = None, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    records = _list_dlq(consumer_id=consumer_id, topic=topic, reason=reason)
    return {"count": len(records), "records": records}


@app.get("/api/admin/dlq/records/{message_id}")
async def inspect_dead_letter(message_id: str, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    records = _load_json(DLQ_PATH)
    if message_id not in records:
        raise HTTPException(status_code=404, detail="Dead-letter record not found.")
    record = dict(records[message_id])
    record["message_id"] = message_id
    return record


@app.post("/api/admin/dlq/replay")
async def replay_dead_letters(request: DLQReplayRequest, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    if request.confirm != "REPLAY":
        raise HTTPException(status_code=400, detail="Replay requires confirm=REPLAY.")
    records = _load_json(DLQ_PATH)
    scheduled = json.loads(SCHEDULED_PATH.read_text()) if SCHEDULED_PATH.exists() and SCHEDULED_PATH.read_text().strip() else []
    result = {"requested": len(request.message_ids), "replayed": 0, "failed": 0, "failures": []}
    prior_state: List[Dict[str, Any]] = []
    resulting_state: List[Dict[str, Any]] = []
    next_seq = max([item.get("sequence", 0) for item in scheduled], default=0)
    for message_id in request.message_ids:
        record = records.get(message_id)
        if not record:
            result["failed"] += 1
            result["failures"].append({"message_id": message_id, "error": "record not found"})
            continue
        if record.get("consumer_id") != request.target_consumer_id:
            result["failed"] += 1
            result["failures"].append({"message_id": message_id, "error": "target consumer does not match original consumer"})
            continue
        if record.get("topic") != request.target_topic:
            result["failed"] += 1
            result["failures"].append({"message_id": message_id, "error": "target topic does not match original topic"})
            continue
        prior_state.append({"message_id": message_id, "location": "dead_letter_store", **record})
        next_seq += 1
        scheduled.append({"sequence": next_seq, "message_id": message_id, "topic": record["topic"], "payload": record.get("payload", []), "priority": record.get("priority", ""), "enqueue_sequence": record.get("enqueue_sequence", 0), "delivery_attempt": 1, "deliver_at": _utcnow(), "reason": "dlq_replay"})
        resulting_state.append({"message_id": message_id, "location": "scheduled_replay", "consumer_id": record.get("consumer_id"), "session_id": record.get("session_id"), "topic": record.get("topic"), "priority": record.get("priority", ""), "enqueue_sequence": record.get("enqueue_sequence", 0), "attempt": 1, "reason": "dlq_replay"})
        records.pop(message_id, None)
        result["replayed"] += 1
    _write_json(DLQ_PATH, records)
    _write_json(SCHEDULED_PATH, scheduled)
    if prior_state:
        _append_audit_event("replay_dead_letter", request.actor, request.message_ids, request.requested_reason, prior_state, resulting_state)
    return result


@app.post("/api/admin/dlq/purge")
async def purge_dead_letters(request: DLQPurgeRequest, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    if request.confirm != "PURGE":
        raise HTTPException(status_code=400, detail="Purge requires confirm=PURGE.")
    records = _load_json(DLQ_PATH)
    result = {"requested": 0, "purged": 0, "failed": 0, "failures": []}
    prior_state: List[Dict[str, Any]] = []
    target_message_ids: List[str] = []
    if request.message_ids:
        for message_id in request.message_ids:
            result["requested"] += 1
            if message_id not in records:
                result["failed"] += 1
                result["failures"].append({"message_id": message_id, "error": "record not found"})
                continue
            prior_state.append({"message_id": message_id, "location": "dead_letter_store", **records[message_id]})
            target_message_ids.append(message_id)
            records.pop(message_id, None)
            result["purged"] += 1
    else:
        for message_id, record in list(records.items()):
            if request.consumer_id and record.get("consumer_id") != request.consumer_id:
                continue
            if request.topic and record.get("topic") != request.topic:
                continue
            if request.reason and record.get("reason") != request.reason:
                continue
            result["requested"] += 1
            prior_state.append({"message_id": message_id, "location": "dead_letter_store", **record})
            target_message_ids.append(message_id)
            records.pop(message_id, None)
            result["purged"] += 1
    _write_json(DLQ_PATH, records)
    if prior_state:
        _append_audit_event("purge_dead_letter", request.actor, target_message_ids, request.requested_reason, prior_state, [{"message_id": item["message_id"], "location": "purged"} for item in prior_state])
    return result


@app.post("/api/admin/dlq/dead-letter")
async def dead_letter_message(request: ManualDeadLetterRequest, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    records = _load_json(DLQ_PATH)
    record = {
        "consumer_id": request.consumer_id,
        "session_id": request.session_id,
        "topic": request.topic,
        "payload": request.payload,
        "priority": request.priority,
        "enqueue_sequence": request.enqueue_sequence,
        "attempt": request.attempt,
        "reason": request.reason,
        "dead_lettered_at": _utcnow(),
    }
    prior_state = []
    if request.message_id in records:
        prior_state.append({"message_id": request.message_id, "location": "dead_letter_store", **records[request.message_id]})
    records[request.message_id] = {k: v for k, v in record.items() if v not in (None, "") or k in {"topic", "payload", "reason", "dead_lettered_at"}}
    _write_json(DLQ_PATH, records)
    resulting_state = [{"message_id": request.message_id, "location": "dead_letter_store", **records[request.message_id]}]
    _append_audit_event("manual_dead_letter", request.actor, [request.message_id], request.requested_reason, prior_state, resulting_state, timestamp=records[request.message_id]["dead_lettered_at"])
    return {"message_id": request.message_id, **records[request.message_id]}


@app.get("/api/admin/audit/events")
async def list_audit_events(message_id: Optional[str] = None, actor: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    _admin_guard(x_admin_token)
    events = _query_audit(message_id=message_id, actor=actor, start=start, end=end)
    return {"count": len(events), "events": events}


if __name__ == "__main__":
    print("🚀 AetherBus Tachyon gateway starting on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
