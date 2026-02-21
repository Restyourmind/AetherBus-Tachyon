# tools/contracts/contract_checker.py
import json
import os
from typing import Dict, Any, Optional, Literal

class ContractChecker:
    """
    The Advanced Immune System.
    Performs dynamic, schema-based data integrity checks before data enters the core system.
    """
    def __init__(self, schema_path: str = "docs/schemas/"):
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.schema_path = schema_path
        self._load_initial_schemas()

    def _load_initial_schemas(self):
        """Loads the initial "genetic blueprint" of valid data."""
        # Hardcoded schema for demonstration purposes. In a real system, this would load from schema_path.
        self.schemas['ipw_v1'] = {
            "type": "object",
            "required": ["intent", "vector", "timestamp"],
            "properties": {
                "intent": {"type": "string"},
                "vector": {"type": "array", "items": {"type": "number"}},
                "timestamp": {"type": "number"}
            }
        }
        print(f"🛡️  Immune System Loaded: {len(self.schemas)} contracts active.")

    def load_dynamic_schema(self, contract_id: str, schema_data: str) -> bool:
        """Loads a new DNA blueprint into the immune system dynamically."""
        try:
            self.schemas[contract_id] = json.loads(schema_data)
            print(f"🧬 New Contract DNA loaded: {contract_id}")
            return True
        except json.JSONDecodeError:
            print(f"❌ Failed to decode dynamic schema for {contract_id}")
            return False

    def validate_schema(self, payload: Dict[str, Any], contract_type: str) -> bool:
        """
        Validates a payload against a registered contract schema.
        True = Pure Data (Pass)
        False = Contaminated Data (Reject)
        """
        schema = self.schemas.get(contract_type)
        if not schema:
            print(f"⚠️  Unknown Contract Type: {contract_type}")
            return False

        # Basic Validation Logic (can be replaced with 'jsonschema' library for production)
        for field in schema.get("required", []):
            if field not in payload:
                print(f"❌ Contract Violation: Missing required field '{field}' in {contract_type}")
                return False
        
        for field, props in schema.get("properties", {}).items():
            if field in payload:
                # A very basic type check
                field_type = props.get("type")
                py_type = None
                if field_type == "string": py_type = str
                elif field_type == "number": py_type = (int, float)
                elif field_type == "array": py_type = list
                elif field_type == "object": py_type = dict
                
                if py_type and not isinstance(payload[field], py_type):
                    print(f"❌ Contract Violation: Field '{field}' has wrong type (expected {field_type}) in {contract_type}")
                    return False
        
        return True
        
    def validate_intent(self, payload: Dict[str, Any], expected_intent: str) -> bool:
        """Verifies if the payload's intent matches the expected reality."""
        if "intent" not in payload:
            print(f"❌ Intent Validation Failed: 'intent' field is missing.")
            return False
        
        actual_intent = payload.get("intent")
        if actual_intent != expected_intent:
            print(f"❌ Intent Mismatch: Expected '{expected_intent}', but got '{actual_intent}'.")
            return False
            
        return True
