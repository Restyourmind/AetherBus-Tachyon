# api_gateway/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import json
import time

# Import a more advanced immune system and the nervous system
from tools.contracts.contract_checker import ContractChecker
from api_gateway.aetherbus_extreme import AetherBusExtreme

# Attempt to import the Tachyon Core (Brain)
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
immune_system = ContractChecker() # Now an AdvancedContractChecker

# Allow CORS for dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. Reflex Arcs (API Endpoints) ---

@app.get("/")
async def root():
    return {"status": "ONLINE", "brain_connected": HAS_BRAIN, "mode": "Tachyon Connected" if HAS_BRAIN else "Spinal Reflex"}

@app.post("/api/contracts/load")
async def load_contract(contract_id: str, schema: dict):
    """Dynamically loads a new data contract (DNA) into the immune system."""
    schema_str = json.dumps(schema)
    success = immune_system.load_dynamic_schema(contract_id, schema_str)
    if not success:
        raise HTTPException(status_code=400, detail="Invalid schema format.")
    return {"status": "Contract loaded successfully", "contract_id": contract_id}


@app.post("/api/genesis/mint")
async def mint_agent(seed: int = 1000):
    """Commands the brain to create a new Agent (Identity Crystallization)."""
    if not HAS_BRAIN:
        raise HTTPException(status_code=503, detail="Brain (Tachyon Core) is not connected.")
    
    # 1. Call the Tachyon Core (Rust)
    try:
        deck_bytes = tachyon_engine.mint_starter_deck(seed)
        sentinel, catalyst, harmonizer = deck_bytes
    except Exception as e:
        await bus.emit("SYSTEM_ALERT", {"level": "critical", "message": f"Tachyon Core Error during minting: {e}"})
        raise HTTPException(status_code=500, detail="Error communicating with Tachyon Core.")

    # 2. Convert to JSON for response
    agents = {
        "sentinel": json.loads(tachyon_engine.inspect_identity_json(sentinel)),
        "catalyst": json.loads(tachyon_engine.inspect_identity_json(catalyst)),
        "harmonizer": json.loads(tachyon_engine.inspect_identity_json(harmonizer))
    }
    
    # 3. Use the Immune System to validate the intent of the newly created agent data
    # Example: We expect the sentinel to have the intent 'guardian'
    is_valid_intent = immune_system.validate_intent(agents['sentinel'], expected_intent="guardian")
    if not is_valid_intent:
        await bus.emit("SYSTEM_ALERT", {"level": "warning", "message": "Minted agent failed intent validation!"})
        # Decide whether to reject the mint or just warn
        # For now, we will add a flag to the response
        agents["validation_warning"] = "Sentinel intent mismatch"

    # 4. Signal the nervous system (for real-time dashboard updates)
    await bus.emit("AGENT_MINTED", agents)
    
    return agents

# --- 2. Neural Pathway (WebSocket) ---

@app.websocket("/ws/feed")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time data feed to the dashboard."""
    await websocket.accept()
    
    # A handler function to listen to the bus (like a synapse)
    async def synapse_handler(event_data: dict):
        try:
            # Add topic to the payload for the client to filter
            await websocket.send_json(event_data)
        except Exception:
            pass # Connection closed

    # Connect synapses to the bus
    # We create a wrapper to pass the topic along with the payload
    async def mint_handler(payload): await synapse_handler({"topic": "AGENT_MINTED", "payload": payload})
    async def alert_handler(payload): await synapse_handler({"topic": "SYSTEM_ALERT", "payload": payload})

    await bus.subscribe("AGENT_MINTED", mint_handler)
    await bus.subscribe("SYSTEM_ALERT", alert_handler)

    try:
        while True:
            # Keep the connection alive, optionally receive data from the dashboard
            data = await websocket.receive_text()
            # We can forward this to the bus if needed
            # await bus.emit("USER_INPUT", {"source": "dashboard", "data": data})
    except WebSocketDisconnect:
        print("🔌 Dashboard Disconnected from Neural Pathway")

# --- 3. System Awakening Sequence ---

if __name__ == "__main__":
    print("🚀 Aetherium Manifest: Awakening Sequence Initiated...")
    uvicorn.run(app, host="0.0.0.0", port=8000)