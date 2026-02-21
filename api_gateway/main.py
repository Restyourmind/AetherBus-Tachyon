# api_gateway/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import json
import time

# Import ระบบประสาทและภูมิคุ้มกัน
from api_gateway.aetherbus_extreme import AetherBusExtreme
from tools.contracts.contract_checker import ContractChecker

# พยายาม Import สมอง (Tachyon Core)
try:
    import tachyon_core
    tachyon_engine = tachyon_core.TachyonEngine()
    HAS_BRAIN = True
    print("🧠 Tachyon Core: CONNECTED")
except ImportError:
    HAS_BRAIN = False
    print("⚠️ Tachyon Core: NOT FOUND (Running in Spinal Reflex Mode)")

app = FastAPI(title="Aetherium Syndicate Interface")
bus = AetherBusExtreme()
immune_system = ContractChecker()

# เปิด CORS ให้ Dashboard เรียกใช้ได้
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. Reflex Arcs (API Endpoints) ---

@app.get("/")
async def root():
    return {"status": "ONLINE", "brain_connected": HAS_BRAIN}

@app.post("/api/genesis/mint")
async def mint_agent(seed: int = 1000):
    """สั่งสมองให้สร้าง Agent ใหม่ (Identity Crystallization)"""
    if not HAS_BRAIN:
        return {"error": "Brain missing"}
    
    # 1. เรียก Tachyon Core (Rust)
    deck_bytes = tachyon_engine.mint_starter_deck(seed)
    sentinel, catalyst, harmonizer = deck_bytes
    
    # 2. แปลงเป็น JSON เพื่อส่งกลับ
    agents = {
        "sentinel": json.loads(tachyon_engine.inspect_identity_json(sentinel)),
        "catalyst": json.loads(tachyon_engine.inspect_identity_json(catalyst)),
        "harmonizer": json.loads(tachyon_engine.inspect_identity_json(harmonizer))
    }
    
    # 3. ส่งสัญญาณเข้าระบบประสาท (เพื่อให้ Dashboard อัปเดต Real-time)
    await bus.emit("AGENT_MINTED", agents)
    
    return agents

# --- 2. Neural Pathway (WebSocket) ---

@app.websocket("/ws/feed")
async def websocket_endpoint(websocket: WebSocket):
    """ท่อส่งข้อมูล Real-time ไปยัง Dashboard"""
    await websocket.accept()
    
    # ฟังก์ชันดักฟังเสียงในหัว (Callback)
    async def synapse_handler(payload):
        try:
            await websocket.send_json(payload)
        except Exception:
            pass # Connection closed

    # เชื่อมต่อ Synapse เข้ากับ Bus
    await bus.subscribe("AGENT_MINTED", synapse_handler)
    await bus.subscribe("SYSTEM_ALERT", synapse_handler)

    try:
        while True:
            # รับข้อมูลจาก Dashboard (ถ้ามี)
            data = await websocket.receive_text()
            # สามารถส่งต่อเข้า Bus ได้ถ้าต้องการ
            # await bus.emit("USER_INPUT", data)
    except WebSocketDisconnect:
        print("🔌 Dashboard Disconnected")

# --- 3. Start System ---

if __name__ == "__main__":
    print("🚀 Aetherium Manifest: Awakening Sequence Initiated...")
    uvicorn.run(app, host="0.0.0.0", port=8000)