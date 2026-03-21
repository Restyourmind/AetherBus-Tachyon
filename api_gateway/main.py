import asyncio
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


WAL_PATH = os.getenv("WAL_PATH", "./data/direct_delivery.wal")
ROUTE_CATALOG_PATH = Path(os.getenv("ROUTE_CATALOG_PATH", "./data/routes.catalog.json"))
SESSION_SNAPSHOT_PATH = Path(os.getenv("SESSION_SNAPSHOT_PATH", WAL_PATH + ".sessions"))
DLQ_PATH = Path(os.getenv("DLQ_STORE_PATH", WAL_PATH + ".dlq"))
SCHEDULED_PATH = Path(os.getenv("SCHEDULED_STORE_PATH", WAL_PATH + ".scheduled"))
AUDIT_PATH = Path(os.getenv("AUDIT_STORE_PATH", WAL_PATH + ".audit"))
WAL_SEGMENTS_PATH = Path(os.getenv("WAL_SEGMENTS_PATH", WAL_PATH + ".segments"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


def _admin_guard(admin_token: Optional[str]) -> None:
    if ADMIN_TOKEN and admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Admin token required.")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text() or "{}")


def _load_json_list(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_text().strip()
    if not raw:
        return []
    payload = json.loads(raw)
    return payload if isinstance(payload, list) else []


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


def _iso_to_epoch(value: Optional[str]) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _load_route_catalog() -> Dict[str, Any]:
    if not ROUTE_CATALOG_PATH.exists():
        return {"version": 1, "routes": []}
    raw = ROUTE_CATALOG_PATH.read_text().strip()
    if not raw:
        return {"version": 1, "routes": []}
    payload = json.loads(raw)
    routes = payload.get("routes", [])
    routes.sort(key=lambda item: (
        item.get("tenant", ""),
        item.get("pattern", ""),
        item.get("destination_id", ""),
        item.get("route_type", ""),
    ))
    return {"version": payload.get("version", 1), "routes": routes}


def _load_session_snapshots() -> List[Dict[str, Any]]:
    snapshots = _load_json(SESSION_SNAPSHOT_PATH)
    out: List[Dict[str, Any]] = []
    for consumer_id, snapshot in snapshots.items():
        item = dict(snapshot)
        item["consumer_id"] = item.get("consumer_id", consumer_id)
        last_heartbeat = item.get("last_heartbeat")
        age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - _iso_to_epoch(last_heartbeat)) if last_heartbeat else None
        if item.get("resumable_pending"):
            health = "resumable_pending"
        elif age_seconds is None:
            health = "unknown"
        elif age_seconds <= 30:
            health = "healthy"
        elif age_seconds <= 120:
            health = "degraded"
        else:
            health = "stale"
        item["heartbeat_age_seconds"] = age_seconds
        item["health"] = health
        out.append(item)
    out.sort(key=lambda item: item.get("consumer_id", ""))
    return out


def _load_scheduled() -> List[Dict[str, Any]]:
    entries = _load_json_list(SCHEDULED_PATH)
    for entry in entries:
        entry["deliver_at_epoch"] = _iso_to_epoch(entry.get("deliver_at"))
    entries.sort(key=lambda item: (item.get("deliver_at_epoch", 0), item.get("sequence", 0)))
    return entries


def _load_wal_records() -> List[Dict[str, Any]]:
    if not WAL_SEGMENTS_PATH.exists():
        return []
    records: List[Dict[str, Any]] = []
    segment_files = sorted(path for path in WAL_SEGMENTS_PATH.iterdir() if path.is_file() and not path.name.endswith(".closed"))
    for segment_path in segment_files:
        for line_no, line in enumerate(segment_path.read_text().splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            record["_segment"] = segment_path.name
            record["_line"] = line_no
            record["_observed_at"] = datetime.fromtimestamp(segment_path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            records.append(record)
    return records


def _wal_inflight_snapshot() -> Dict[str, Any]:
    pending: Dict[str, Dict[str, Any]] = {}
    for record in _load_wal_records():
        message_id = record.get("message_id")
        if not message_id:
            continue
        if record.get("type") == "dispatched":
            pending[message_id] = record
        elif record.get("type") in {"committed", "dead_lettered"}:
            pending.pop(message_id, None)
    by_consumer: Dict[str, int] = {}
    by_topic: Dict[str, int] = {}
    for record in pending.values():
        consumer_id = record.get("consumer_id", "")
        topic = record.get("topic", "")
        by_consumer[consumer_id] = by_consumer.get(consumer_id, 0) + 1
        by_topic[topic] = by_topic.get(topic, 0) + 1
    return {
        "count": len(pending),
        "messages": sorted(pending.values(), key=lambda item: (item.get("consumer_id", ""), item.get("topic", ""), item.get("message_id", ""))),
        "by_consumer": dict(sorted(by_consumer.items())),
        "by_topic": dict(sorted(by_topic.items())),
    }


def _backlog_snapshot() -> Dict[str, Any]:
    inflight = _wal_inflight_snapshot()
    scheduled = _load_scheduled()
    dead_letters = _list_dlq()
    sessions = _load_session_snapshots()
    oldest_scheduled_at = scheduled[0].get("deliver_at") if scheduled else None
    backlog_ratio = (
        (inflight["count"] + len(scheduled) + len(dead_letters)) / max(len(sessions), 1)
        if (inflight["count"] + len(scheduled) + len(dead_letters)) > 0
        else 0
    )
    return {
        "direct_inflight": inflight["count"],
        "deferred_total": len(scheduled),
        "dead_letter_total": len(dead_letters),
        "active_sessions": len(sessions),
        "oldest_deferred_at": oldest_scheduled_at,
        "backlog_per_session": round(backlog_ratio, 2),
        "pressure": "high" if len(dead_letters) > 0 or len(scheduled) >= max(10, len(sessions) * 2) else "elevated" if len(scheduled) > 0 or inflight["count"] > len(sessions) else "nominal",
        "inflight_breakdown": inflight,
    }


def _recent_delivery_transitions(limit: int = 12) -> List[Dict[str, Any]]:
    transitions: List[Dict[str, Any]] = []
    for record in _list_dlq():
        transitions.append({
            "timestamp": record.get("dead_lettered_at"),
            "kind": "dead_lettered",
            "message_id": record.get("message_id"),
            "consumer_id": record.get("consumer_id"),
            "topic": record.get("topic"),
            "reason": record.get("reason"),
            "detail": "Message moved into dead-letter inventory",
        })
    for entry in _load_scheduled():
        transitions.append({
            "timestamp": entry.get("deliver_at"),
            "kind": entry.get("reason", "scheduled"),
            "message_id": entry.get("message_id"),
            "consumer_id": entry.get("consumer_id"),
            "topic": entry.get("topic"),
            "reason": entry.get("reason"),
            "detail": "Message queued for deferred delivery or replay",
        })
    for event in _load_audit():
        transitions.append({
            "timestamp": event.get("timestamp"),
            "kind": event.get("operation"),
            "message_id": ",".join(event.get("target_message_ids", [])[:3]),
            "consumer_id": event.get("actor"),
            "topic": "",
            "reason": event.get("requested_reason"),
            "detail": f"Admin action by {event.get('actor', 'unknown')}",
        })
    transitions = [item for item in transitions if item.get("timestamp")]
    transitions.sort(key=lambda item: (_iso_to_epoch(item.get("timestamp")), item.get("message_id", "")), reverse=True)
    return transitions[:limit]


def _broker_admin_snapshot() -> Dict[str, Any]:
    route_catalog = _load_route_catalog()
    sessions = _load_session_snapshots()
    backlog = _backlog_snapshot()
    dead_letters = _list_dlq()
    deferred = _load_scheduled()
    topology: Dict[str, List[Dict[str, Any]]] = {}
    for route in route_catalog["routes"]:
        topology.setdefault(route.get("tenant", "default"), []).append(route)
    return {
        "generated_at": _utcnow(),
        "route_catalog": route_catalog,
        "route_topology": [{"tenant": tenant, "routes": routes} for tenant, routes in sorted(topology.items())],
        "consumer_sessions": sessions,
        "backlog": backlog,
        "dead_letters": {"count": len(dead_letters), "records": dead_letters},
        "deferred_queue": {"count": len(deferred), "entries": deferred},
        "recent_transitions": _recent_delivery_transitions(),
    }


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


BROKER_ADMIN_PAGE = dedent(
    """
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Tachyon Broker Admin</title>
        <style>
          :root {
            color-scheme: dark;
            --bg: #071018;
            --panel: #0d1824;
            --panel-2: #132233;
            --border: rgba(111, 163, 255, .18);
            --text: #e8eef9;
            --muted: #91a4be;
            --accent: #7dd3fc;
            --ok: #34d399;
            --warn: #f59e0b;
            --bad: #f87171;
          }
          * { box-sizing: border-box; }
          body { margin: 0; font-family: Inter, system-ui, sans-serif; background: linear-gradient(180deg, #08131e, #050b12); color: var(--text); }
          main { max-width: 1360px; margin: 0 auto; padding: 24px; display: grid; gap: 18px; }
          .banner, .card { border: 1px solid var(--border); border-radius: 20px; background: rgba(13, 24, 36, .95); box-shadow: 0 18px 40px rgba(0,0,0,.24); }
          .banner { padding: 24px; display: grid; gap: 8px; }
          .banner h1 { margin: 0; font-size: clamp(2rem, 5vw, 3rem); }
          .subtle { color: var(--muted); }
          .mono { font-family: ui-monospace, SFMono-Regular, monospace; }
          .stats, .two-up, .triple { display: grid; gap: 16px; }
          .stats { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
          .two-up { grid-template-columns: 1.3fr .7fr; }
          .triple { grid-template-columns: 1fr 1fr 1fr; }
          .card { padding: 18px; overflow: hidden; }
          h2, h3 { margin: 0 0 12px; }
          .metric { font-size: 2rem; font-weight: 700; }
          .table { width: 100%; border-collapse: collapse; font-size: .92rem; }
          .table th, .table td { padding: 10px 8px; border-top: 1px solid rgba(255,255,255,.06); text-align: left; vertical-align: top; }
          .table th { color: var(--muted); font-weight: 600; }
          .pill { display: inline-flex; align-items: center; gap: 6px; border-radius: 999px; padding: 4px 10px; font-size: .8rem; background: rgba(125, 211, 252, .12); color: var(--accent); }
          .health-healthy { color: var(--ok); }
          .health-degraded, .pressure-elevated { color: var(--warn); }
          .health-stale, .health-resumable_pending, .pressure-high { color: var(--bad); }
          .route-list, .transition-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 10px; }
          .route-list li, .transition-list li { background: var(--panel-2); border-radius: 14px; padding: 12px; border: 1px solid rgba(255,255,255,.05); }
          form { display: grid; gap: 10px; }
          input, textarea { width: 100%; border-radius: 10px; border: 1px solid rgba(255,255,255,.12); background: #09111a; color: var(--text); padding: 10px 12px; }
          button { border: 0; border-radius: 12px; padding: 10px 14px; font-weight: 700; cursor: pointer; background: linear-gradient(90deg, #0891b2, #2563eb); color: white; }
          button.warn { background: linear-gradient(90deg, #d97706, #dc2626); }
          .result { min-height: 22px; color: var(--muted); }
          .split { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
          @media (max-width: 960px) { .two-up, .triple, .split { grid-template-columns: 1fr; } }
        </style>
      </head>
      <body>
        <main>
          <section class="banner">
            <span class="pill">Broker-only operational/admin surface</span>
            <h1>Tachyon broker state</h1>
            <div class="subtle">Separate from gateway message traffic. Data comes from the route catalog, WAL snapshots, deferred queues, dead-letter inventory, and audit trail.</div>
          </section>

          <section class="stats" id="stats"></section>

          <section class="two-up">
            <article class="card">
              <h2>Route topology</h2>
              <div id="route-topology"></div>
            </article>
            <article class="card">
              <h2>Direct-session health</h2>
              <table class="table">
                <thead><tr><th>Consumer</th><th>Session</th><th>Health</th><th>Heartbeat</th><th>Subscriptions</th></tr></thead>
                <tbody id="session-rows"></tbody>
              </table>
            </article>
          </section>

          <section class="triple">
            <article class="card">
              <h3>Inflight counts</h3>
              <div id="inflight-breakdown" class="subtle">Loading…</div>
            </article>
            <article class="card">
              <h3>Deferred queue</h3>
              <table class="table"><thead><tr><th>Message</th><th>Topic</th><th>Deliver at</th></tr></thead><tbody id="deferred-rows"></tbody></table>
            </article>
            <article class="card">
              <h3>Dead-letter inventory</h3>
              <table class="table"><thead><tr><th>Message</th><th>Consumer</th><th>Reason</th></tr></thead><tbody id="dlq-rows"></tbody></table>
            </article>
          </section>

          <section class="two-up">
            <article class="card">
              <h2>Recent delivery transitions</h2>
              <ul class="transition-list" id="transition-list"></ul>
            </article>
            <article class="card">
              <h2>Guarded controls</h2>
              <div class="subtle">Replay and purge actions stay on the broker-admin API and require the admin token header. Audit entries are appended for every successful mutation.</div>
              <div class="split" style="margin-top:14px;">
                <form id="replay-form">
                  <h3>Replay dead letters</h3>
                  <input name="token" placeholder="Admin token" />
                  <input name="actor" placeholder="Actor" value="broker-admin-ui" />
                  <input name="message_ids" placeholder="Message IDs (comma-separated)" />
                  <input name="target_consumer_id" placeholder="Target consumer ID" />
                  <input name="target_topic" placeholder="Target topic" />
                  <textarea name="requested_reason" placeholder="Reason"></textarea>
                  <input name="confirm" value="REPLAY" />
                  <button type="submit">Replay</button>
                  <div class="result" id="replay-result"></div>
                </form>
                <form id="purge-form">
                  <h3>Purge dead letters</h3>
                  <input name="token" placeholder="Admin token" />
                  <input name="actor" placeholder="Actor" value="broker-admin-ui" />
                  <input name="message_ids" placeholder="Message IDs (comma-separated)" />
                  <textarea name="requested_reason" placeholder="Reason"></textarea>
                  <input name="confirm" value="PURGE" />
                  <button type="submit" class="warn">Purge</button>
                  <div class="result" id="purge-result"></div>
                </form>
              </div>
            </article>
          </section>
        </main>
        <script>
          let brokerSocket;

          function setResult(id, payload) {
            document.getElementById(id).textContent = typeof payload === 'string' ? payload : JSON.stringify(payload);
          }

          async function postAction(path, form, resultId) {
            const data = new FormData(form);
            const token = data.get('token') || '';
            const messageIDs = String(data.get('message_ids') || '').split(',').map(v => v.trim()).filter(Boolean);
            const body = {
              actor: data.get('actor'),
              requested_reason: data.get('requested_reason'),
              confirm: data.get('confirm'),
              message_ids: messageIDs,
            };
            if (path.includes('replay')) {
              body.target_consumer_id = data.get('target_consumer_id');
              body.target_topic = data.get('target_topic');
            }
            const response = await fetch(path, {
              method: 'POST',
              headers: {'Content-Type': 'application/json', 'X-Admin-Token': token},
              body: JSON.stringify(body),
            });
            const payload = await response.json();
            setResult(resultId, payload.detail || payload);
            if (response.ok) {
              refreshDashboard();
            }
          }

          function renderStats(snapshot) {
            const backlog = snapshot.backlog;
            const stats = [
              ['Routes', snapshot.route_catalog.routes.length],
              ['Sessions', snapshot.consumer_sessions.length],
              ['Inflight', backlog.direct_inflight],
              ['Deferred', backlog.deferred_total],
              ['Dead letters', backlog.dead_letter_total],
              ['Pressure', backlog.pressure],
            ];
            document.getElementById('stats').innerHTML = stats.map(([label, value]) => `<article class="card"><div class="subtle">${label}</div><div class="metric pressure-${String(value).toLowerCase()}">${value}</div></article>`).join('');
          }

          function renderTopology(snapshot) {
            const host = document.getElementById('route-topology');
            const groups = snapshot.route_topology.length ? snapshot.route_topology : [{tenant: 'default', routes: []}];
            host.innerHTML = groups.map(group => `
              <div style="margin-bottom:14px;">
                <div class="pill">${group.tenant}</div>
                <ul class="route-list">
                  ${group.routes.map(route => `<li><strong>${route.pattern}</strong><br/><span class="subtle">→ ${route.destination_id} · ${route.route_type} · enabled=${route.enabled}</span></li>`).join('') || '<li class="subtle">No routes loaded.</li>'}
                </ul>
              </div>`).join('');
          }

          function renderSessions(snapshot) {
            const rows = snapshot.consumer_sessions.map(item => `
              <tr>
                <td>${item.consumer_id}</td>
                <td class="mono">${item.session_id || '—'}</td>
                <td class="health-${item.health}">${item.health}</td>
                <td>${item.last_heartbeat || '—'}</td>
                <td>${(item.subscriptions || []).join(', ') || '—'}</td>
              </tr>`).join('');
            document.getElementById('session-rows').innerHTML = rows || '<tr><td colspan="5" class="subtle">No resumable session snapshots present.</td></tr>';
          }

          function renderBreakdown(snapshot) {
            const inflight = snapshot.backlog.inflight_breakdown;
            const byConsumer = Object.entries(inflight.by_consumer || {}).map(([key, value]) => `${key || 'unassigned'}=${value}`).join(', ');
            const byTopic = Object.entries(inflight.by_topic || {}).map(([key, value]) => `${key}=${value}`).join(', ');
            document.getElementById('inflight-breakdown').innerHTML = `
              <div><strong>Total:</strong> ${inflight.count}</div>
              <div><strong>By consumer:</strong> ${byConsumer || 'none'}</div>
              <div><strong>By topic:</strong> ${byTopic || 'none'}</div>
            `;

            document.getElementById('deferred-rows').innerHTML = snapshot.deferred_queue.entries.slice(0, 8).map(item => `
              <tr><td class="mono">${item.message_id}</td><td>${item.topic}</td><td>${item.deliver_at || '—'}</td></tr>`).join('') || '<tr><td colspan="3" class="subtle">No deferred messages.</td></tr>';

            document.getElementById('dlq-rows').innerHTML = snapshot.dead_letters.records.slice(0, 8).map(item => `
              <tr><td class="mono">${item.message_id}</td><td>${item.consumer_id || '—'}</td><td>${item.reason || '—'}</td></tr>`).join('') || '<tr><td colspan="3" class="subtle">No dead-letter inventory.</td></tr>';
          }

          function renderTransitions(snapshot) {
            document.getElementById('transition-list').innerHTML = snapshot.recent_transitions.map(item => `
              <li><strong>${item.kind}</strong> <span class="mono">${item.message_id || '—'}</span><br/><span class="subtle">${item.timestamp} · ${item.detail}${item.reason ? ` · ${item.reason}` : ''}</span></li>`).join('') || '<li class="subtle">No recent transitions.</li>';
          }

          async function refreshDashboard() {
            const response = await fetch('/api/broker-admin/overview');
            const snapshot = await response.json();
            renderStats(snapshot);
            renderTopology(snapshot);
            renderSessions(snapshot);
            renderBreakdown(snapshot);
            renderTransitions(snapshot);
          }

          function connectSocket() {
            brokerSocket = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/broker-admin`);
            brokerSocket.onmessage = () => refreshDashboard();
            brokerSocket.onclose = () => setTimeout(connectSocket, 3000);
          }

          document.getElementById('replay-form').addEventListener('submit', async (event) => {
            event.preventDefault();
            await postAction('/api/broker-admin/actions/replay', event.target, 'replay-result');
          });
          document.getElementById('purge-form').addEventListener('submit', async (event) => {
            event.preventDefault();
            await postAction('/api/broker-admin/actions/purge', event.target, 'purge-result');
          });

          refreshDashboard();
          connectSocket();
          setInterval(refreshDashboard, 10000);
        </script>
      </body>
    </html>
    """
)


@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    return HTMLResponse(HOME_PAGE)


@app.get("/broker-admin", response_class=HTMLResponse)
async def broker_admin_home() -> HTMLResponse:
    return HTMLResponse(BROKER_ADMIN_PAGE)


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


@app.get("/api/broker-admin/overview")
async def broker_admin_overview() -> Dict[str, Any]:
    return _broker_admin_snapshot()


@app.get("/api/broker-admin/routes")
async def broker_admin_routes() -> Dict[str, Any]:
    return _load_route_catalog()


@app.get("/api/broker-admin/sessions")
async def broker_admin_sessions() -> Dict[str, Any]:
    sessions = _load_session_snapshots()
    return {"count": len(sessions), "sessions": sessions}


@app.get("/api/broker-admin/backlog")
async def broker_admin_backlog() -> Dict[str, Any]:
    return _backlog_snapshot()


@app.get("/api/broker-admin/transitions")
async def broker_admin_transitions(limit: int = 12) -> Dict[str, Any]:
    items = _recent_delivery_transitions(limit=max(1, min(limit, 100)))
    return {"count": len(items), "transitions": items}


@app.get("/api/broker-admin/dead-letters")
async def broker_admin_dead_letters(consumer_id: Optional[str] = None, topic: Optional[str] = None, reason: Optional[str] = None) -> Dict[str, Any]:
    records = _list_dlq(consumer_id=consumer_id, topic=topic, reason=reason)
    return {"count": len(records), "records": records}


@app.get("/api/broker-admin/deferred")
async def broker_admin_deferred() -> Dict[str, Any]:
    entries = _load_scheduled()
    return {"count": len(entries), "entries": entries}


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


@app.websocket("/ws/broker-admin")
async def broker_admin_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            await websocket.send_json({"type": "broker_admin_snapshot", "snapshot": _broker_admin_snapshot()})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        print("🔌 Broker admin dashboard disconnected from /ws/broker-admin")
    except Exception:
        await websocket.close()




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


@app.post("/api/broker-admin/actions/replay")
async def broker_admin_replay_dead_letters(request: DLQReplayRequest, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    return await replay_dead_letters(request, x_admin_token)


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


@app.post("/api/broker-admin/actions/purge")
async def broker_admin_purge_dead_letters(request: DLQPurgeRequest, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    return await purge_dead_letters(request, x_admin_token)


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


@app.get("/api/broker-admin/audit/events")
async def broker_admin_audit_events(message_id: Optional[str] = None, actor: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None, x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    return await list_audit_events(message_id=message_id, actor=actor, start=start, end=end, x_admin_token=x_admin_token)


if __name__ == "__main__":
    print("🚀 AetherBus Tachyon gateway starting on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
