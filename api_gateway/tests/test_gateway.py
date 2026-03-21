import json
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


def test_dlq_browse_replay_and_purge(tmp_path, monkeypatch):
    dlq_path = tmp_path / "delivery.wal.dlq"
    scheduled_path = tmp_path / "delivery.wal.scheduled"
    monkeypatch.setattr("api_gateway.main.DLQ_PATH", dlq_path)
    monkeypatch.setattr("api_gateway.main.SCHEDULED_PATH", scheduled_path)
    monkeypatch.setattr("api_gateway.main.ADMIN_TOKEN", "secret")
    dlq_path.write_text(json.dumps({
        "msg-1": {"consumer_id": "worker-1", "topic": "orders.created", "payload": [112, 49], "reason": "retry_exhausted", "dead_lettered_at": "2026-03-21T00:00:00Z"},
        "msg-2": {"consumer_id": "worker-2", "topic": "orders.failed", "payload": [112, 50], "reason": "terminal_nack", "dead_lettered_at": "2026-03-21T00:00:01Z"},
    }))

    denied = client.get("/api/admin/dlq/records")
    assert denied.status_code == 403

    listed = client.get("/api/admin/dlq/records?consumer_id=worker-1", headers={"X-Admin-Token": "secret"})
    assert listed.status_code == 200
    assert listed.json()["count"] == 1

    replay = client.post(
        "/api/admin/dlq/replay",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-1", "missing"], "target_consumer_id": "worker-1", "target_topic": "orders.created", "confirm": "REPLAY"},
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] == 1
    assert replay.json()["failed"] == 1
    scheduled = json.loads(scheduled_path.read_text())
    assert scheduled[0]["message_id"] == "msg-1"

    bad_confirm = client.post(
        "/api/admin/dlq/purge",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-2"], "confirm": "NOPE"},
    )
    assert bad_confirm.status_code == 400

    purge = client.post(
        "/api/admin/dlq/purge",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-2"], "confirm": "PURGE"},
    )
    assert purge.status_code == 200
    assert purge.json()["purged"] == 1
    assert json.loads(dlq_path.read_text()) == {}
