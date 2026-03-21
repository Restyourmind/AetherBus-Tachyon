import json
from typing import Any, Dict


class ContractChecker:
    """Simple schema registry and validator for gateway payload checks."""

    def __init__(self, schema_path: str = "docs/schemas/"):
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.schema_path = schema_path
        self._load_initial_schemas()

    def _load_initial_schemas(self) -> None:
        self.schemas["ipw_v1"] = {
            "type": "object",
            "required": ["intent", "vector", "timestamp"],
            "properties": {
                "intent": {"type": "string"},
                "vector": {"type": "array", "items": {"type": "number"}},
                "timestamp": {"type": "number"},
            },
        }
        print(f"🛡️  Immune System Loaded: {len(self.schemas)} contracts active.")

    def load_dynamic_schema(self, contract_id: str, schema_data: str) -> bool:
        """Load a JSON schema-like contract into the registry."""
        try:
            parsed = json.loads(schema_data)
        except json.JSONDecodeError:
            print(f"❌ Failed to decode dynamic schema for {contract_id}")
            return False

        if not isinstance(parsed, dict) or parsed.get("type") != "object":
            print(f"❌ Invalid schema for {contract_id}: top-level object schema required")
            return False

        self.schemas[contract_id] = parsed
        print(f"🧬 New Contract DNA loaded: {contract_id}")
        return True

    def validate_schema(self, payload: Dict[str, Any], contract_type: str) -> bool:
        schema = self.schemas.get(contract_type)
        if not schema:
            print(f"⚠️  Unknown Contract Type: {contract_type}")
            return False

        for field in schema.get("required", []):
            if field not in payload:
                print(f"❌ Contract Violation: Missing required field '{field}' in {contract_type}")
                return False

        for field, props in schema.get("properties", {}).items():
            if field not in payload:
                continue

            field_type = props.get("type")
            py_type = None
            if field_type == "string":
                py_type = str
            elif field_type == "number":
                py_type = (int, float)
            elif field_type == "array":
                py_type = list
            elif field_type == "object":
                py_type = dict

            if py_type and not isinstance(payload[field], py_type):
                print(
                    f"❌ Contract Violation: Field '{field}' has wrong type "
                    f"(expected {field_type}) in {contract_type}"
                )
                return False

        return True

    def validate_intent(self, payload: Dict[str, Any], expected_intent: str) -> bool:
        if "intent" not in payload:
            print("❌ Intent Validation Failed: 'intent' field is missing.")
            return False

        actual_intent = payload.get("intent")
        if actual_intent != expected_intent:
            print(f"❌ Intent Mismatch: Expected '{expected_intent}', but got '{actual_intent}'.")
            return False

        return True
