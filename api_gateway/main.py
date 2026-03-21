from textwrap import dedent
from typing import Any, Dict

import json
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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


if __name__ == "__main__":
    print("🚀 AetherBus Tachyon gateway starting on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
