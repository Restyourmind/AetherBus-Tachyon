import json
from fastapi.testclient import TestClient

from api_gateway.main import app, bus, immune_system

client = TestClient(app)


def test_root_serves_homepage():
    response = client.get("/")
    assert response.status_code == 200
    assert "AetherBus Tachyon Control Surface" in response.text
    assert "/api/system/overview" in response.text


def test_broker_admin_homepage_is_separate_operational_ui():
    response = client.get("/broker-admin")
    assert response.status_code == 200
    assert "Tachyon broker state" in response.text
    assert "Broker-only operational/admin surface" in response.text


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


def test_dlq_browse_replay_purge_and_audit(tmp_path, monkeypatch):
    route_catalog_path = tmp_path / "routes.catalog.json"
    session_snapshot_path = tmp_path / "delivery.wal.sessions"
    dlq_path = tmp_path / "delivery.wal.dlq"
    scheduled_path = tmp_path / "delivery.wal.scheduled"
    audit_path = tmp_path / "delivery.wal.audit"
    wal_segments_path = tmp_path / "delivery.wal.segments"
    wal_segments_path.mkdir()
    monkeypatch.setattr("api_gateway.main.DLQ_PATH", dlq_path)
    monkeypatch.setattr("api_gateway.main.SCHEDULED_PATH", scheduled_path)
    monkeypatch.setattr("api_gateway.main.AUDIT_PATH", audit_path)
    monkeypatch.setattr("api_gateway.main.ROUTE_CATALOG_PATH", route_catalog_path)
    monkeypatch.setattr("api_gateway.main.SESSION_SNAPSHOT_PATH", session_snapshot_path)
    monkeypatch.setattr("api_gateway.main.WAL_SEGMENTS_PATH", wal_segments_path)
    monkeypatch.setattr("api_gateway.main.ADMIN_TOKEN", "secret")
    route_catalog_path.write_text(json.dumps({
        "version": 1,
        "routes": [
            {"pattern": "orders.created", "destination_id": "node-a", "route_type": "direct", "enabled": True, "tenant": "tenant-a"},
            {"pattern": "orders.failed", "destination_id": "node-b", "route_type": "direct", "enabled": True, "tenant": "tenant-a"},
        ],
    }))
    session_snapshot_path.write_text(json.dumps({
        "worker-1": {
            "session_id": "sess_1",
            "consumer_id": "worker-1",
            "subscriptions": ["orders.created"],
            "connected_at": "2026-03-21T00:00:00Z",
            "last_heartbeat": "2099-03-21T00:00:00Z",
            "max_inflight": 8,
            "supports_ack": True,
            "resumable": True,
            "live": False,
            "resumable_pending": True,
        }
    }))
    dlq_path.write_text(json.dumps({
        "msg-1": {"consumer_id": "worker-1", "topic": "orders.created", "payload": [112, 49], "reason": "retry_exhausted", "dead_lettered_at": "2026-03-21T00:00:00Z"},
        "msg-2": {"consumer_id": "worker-2", "topic": "orders.failed", "payload": [112, 50], "reason": "terminal_nack", "dead_lettered_at": "2026-03-21T00:00:01Z"},
    }))
    scheduled_path.write_text(json.dumps([
        {"sequence": 1, "message_id": "msg-s1", "topic": "orders.created", "payload": [115], "deliver_at": "2026-03-21T00:00:03Z", "reason": "nack_retry"}
    ]))
    (wal_segments_path / "000000000001.log").write_text(
        "\n".join([
            json.dumps({"type": "dispatched", "message_id": "msg-live", "consumer_id": "worker-1", "topic": "orders.created", "payload": "cDE="}),
            json.dumps({"type": "dispatched", "message_id": "msg-done", "consumer_id": "worker-2", "topic": "orders.failed", "payload": "cDI="}),
            json.dumps({"type": "committed", "message_id": "msg-done"}),
        ]) + "\n"
    )

    overview = client.get("/api/broker-admin/overview")
    assert overview.status_code == 200
    snapshot = overview.json()
    assert snapshot["route_catalog"]["version"] == 1
    assert snapshot["backlog"]["direct_inflight"] == 1
    assert snapshot["dead_letters"]["count"] == 2
    assert snapshot["consumer_sessions"][0]["health"] == "resumable_pending"

    routes = client.get("/api/broker-admin/routes")
    assert routes.status_code == 200
    assert routes.json()["routes"][0]["pattern"] == "orders.created"

    sessions = client.get("/api/broker-admin/sessions")
    assert sessions.status_code == 200
    assert sessions.json()["count"] == 1

    backlog = client.get("/api/broker-admin/backlog")
    assert backlog.status_code == 200
    assert backlog.json()["deferred_total"] == 1

    transitions = client.get("/api/broker-admin/transitions?limit=2")
    assert transitions.status_code == 200
    assert transitions.json()["count"] == 2

    denied = client.get("/api/admin/dlq/records")
    assert denied.status_code == 403

    listed = client.get("/api/admin/dlq/records?consumer_id=worker-1", headers={"X-Admin-Token": "secret"})
    assert listed.status_code == 200
    assert listed.json()["count"] == 1

    replay = client.post(
        "/api/broker-admin/actions/replay",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-1", "missing"], "target_consumer_id": "worker-1", "target_topic": "orders.created", "confirm": "REPLAY", "actor": "alice", "requested_reason": "customer remediation"},
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] == 1
    assert replay.json()["failed"] == 1
    scheduled = json.loads(scheduled_path.read_text())
    assert any(item["message_id"] == "msg-1" for item in scheduled)

    dead_letter = client.post(
        "/api/admin/dlq/dead-letter",
        headers={"X-Admin-Token": "secret"},
        json={"message_id": "msg-3", "consumer_id": "worker-3", "topic": "orders.retry", "payload": [112, 51], "actor": "alice", "requested_reason": "manual quarantine", "reason": "manual_admin_action"},
    )
    assert dead_letter.status_code == 200
    assert dead_letter.json()["message_id"] == "msg-3"

    bad_confirm = client.post(
        "/api/admin/dlq/purge",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-2"], "confirm": "NOPE"},
    )
    assert bad_confirm.status_code == 400

    purge = client.post(
        "/api/broker-admin/actions/purge",
        headers={"X-Admin-Token": "secret"},
        json={"message_ids": ["msg-2"], "confirm": "PURGE", "actor": "alice", "requested_reason": "retention cleanup"},
    )
    assert purge.status_code == 200
    assert purge.json()["purged"] == 1

    audit = client.get(
        "/api/broker-admin/audit/events?message_id=msg-3&actor=alice",
        headers={"X-Admin-Token": "secret"},
    )
    assert audit.status_code == 200
    body = audit.json()
    assert body["count"] == 1
    assert body["events"][0]["operation"] == "manual_dead_letter"
    all_events = [json.loads(line) for line in audit_path.read_text().splitlines() if line.strip()]
    assert len(all_events) == 3
    assert all_events[1]["prev_hash"] == all_events[0]["hash"]
    assert all_events[2]["prev_hash"] == all_events[1]["hash"]
