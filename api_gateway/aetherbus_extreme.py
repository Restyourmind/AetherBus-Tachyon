# api_gateway/aetherbus_extreme.py
import asyncio
import time
import json
import logging
from typing import Dict, List, Callable, Any, Awaitable

# ตั้งค่า Logging ให้ดูเป็นระบบประสาท
logging.basicConfig(level=logging.INFO, format='%(asctime)s | AETHERBUS | %(levelname)s | %(message)s')
logger = logging.getLogger("AetherBus")

class AetherBusExtreme:
    """
    The High-Speed Nervous System of Aetherium.
    รองรับการสื่อสารระดับ Microsecond สำหรับ Agent Swarm.
    """
    def __init__(self):
        self._subscribers: Dict[str, List[Callable[[Any], Awaitable[None]]]] = {}
        self._history: List[Dict[str, Any]] = [] # ความจำระยะสั้น (Short-term Memory)
        self._is_active = True
        logger.info("⚡ AetherBus Extreme: Nervous System Online")

    async def subscribe(self, topic: str, handler: Callable[[Any], Awaitable[None]]):
        """เชื่อมต่ออวัยวะ (Agent) เข้ากับระบบประสาท"""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(handler)
        logger.debug(f"🔌 Synapse connected to topic: {topic}")

    async def emit(self, topic: str, payload: Any):
        """ส่งกระแสประสาท (Signal) ไปยังทุกอวัยวะที่เกี่ยวข้อง"""
        if not self._is_active: return

        # 1. บันทึก Log ลงความจำระยะสั้น
        timestamp = time.time()
        event = {"ts": timestamp, "topic": topic, "payload": payload}
        self._history.append(event)
        
        # Keep only last 1000 events to prevent memory overflow
        if len(self._history) > 1000:
            self._history.pop(0)

        # 2. กระจายสัญญาณ (Broadcast)
        if topic in self._subscribers:
            tasks = []
            for handler in self._subscribers[topic]:
                # Fire and forget pattern for speed
                tasks.append(asyncio.create_task(handler(payload)))
            
            # รอให้ทุกอวัยวะรับทราบ (Optional: ถ้าต้องการความเร็วสุดๆ อาจไม่ต้องรอ)
            await asyncio.gather(*tasks, return_exceptions=True)
            
        logger.info(f"⚡ [SIGNAL] {topic}: {str(payload)[:50]}...")

    async def shutdown(self):
        self._is_active = False
        logger.info("💤 AetherBus Extreme: System Shutdown")