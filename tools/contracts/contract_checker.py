# tools/contracts/contract_checker.py
import json
import os
from typing import Dict, Any, Optional

class ContractChecker:
    """
    The Immune System.
    ตรวจสอบความถูกต้องของข้อมูล (Data Integrity) ก่อนเข้าสู่ระบบหลัก.
    """
    def __init__(self, schema_path: str = "docs/schemas/"):
        self.schemas = {}
        self.schema_path = schema_path
        self._load_schemas()

    def _load_schemas(self):
        """โหลดพิมพ์เขียว (DNA) ของข้อมูลที่ถูกต้อง"""
        # ตัวอย่าง Schema อย่างง่าย (Hardcoded for Demo)
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

    def validate(self, payload: Dict[str, Any], contract_type: str) -> bool:
        """
        ตรวจจับเชื้อโรค (Invalid Data)
        True = ข้อมูลบริสุทธิ์ (Pass)
        False = ข้อมูลปนเปื้อน (Reject)
        """
        schema = self.schemas.get(contract_type)
        if not schema:
            print(f"⚠️  Unknown Contract Type: {contract_type}")
            return False

        # Basic Validation Logic (สามารถเปลี่ยนไปใช้ library 'jsonschema' ได้)
        for field in schema.get("required", []):
            if field not in payload:
                print(f"❌ Contract Violation: Missing field '{field}' in {contract_type}")
                return False
        
        return True