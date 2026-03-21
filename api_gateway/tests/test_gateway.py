from fastapi.testclient import TestClient

from api_gateway.main import app, bus, immune_system

client = TestClient(app)


def test_root_serves_homepage():
    response = client.get("/")
    assert response.status_code == 200
    assert "AetherBus Tachyon Control Surface" in response.text
    assert "/api/system/overview" in response.text


def test_system_overview_reports_contracts_and_recent_events():
    response = client.get("/api/system/overview")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ONLINE"
    assert "ipw_v1" in body["contracts"]
    assert isinstance(body["recent_events"], list)


def test_load_contract_requires_object_schema_and_emits_event():
    response = client.post(
        "/api/contracts/load",
        json={
            "contract_id": "demo_contract",
            "schema": {
                "type": "object",
                "required": ["message"],
                "properties": {"message": {"type": "string"}},
            },
        },
    )
    assert response.status_code == 200
    assert response.json()["contract_id"] == "demo_contract"
    assert "demo_contract" in immune_system.schemas
    assert bus.recent_events(1)[0]["topic"] == "CONTRACT_LOADED"


def test_load_contract_rejects_non_object_schema():
    response = client.post(
        "/api/contracts/load",
        json={"contract_id": "bad_contract", "schema": {"type": "array"}},
    )
    assert response.status_code == 400


def test_publish_alert_appends_bus_history():
    response = client.post("/api/system/alerts", json={"level": "warning", "message": "queue pressure rising"})
    assert response.status_code == 200
    latest = bus.recent_events(1)[0]
    assert latest["topic"] == "SYSTEM_ALERT"
    assert latest["payload"]["message"] == "queue pressure rising"
