import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | AETHERBUS | %(levelname)s | %(message)s',
)
logger = logging.getLogger("AetherBus")

Subscriber = Callable[[Any], Awaitable[None]]


class AetherBusExtreme:
    """Lightweight in-process event bus used by the API gateway."""

    def __init__(self, history_limit: int = 1000):
        self._subscribers: Dict[str, List[Subscriber]] = {}
        self._history: List[Dict[str, Any]] = []
        self._history_limit = max(1, history_limit)
        self._is_active = True
        logger.info("⚡ AetherBus Extreme: Nervous System Online")

    async def subscribe(self, topic: str, handler: Subscriber) -> None:
        """Register an async handler for a topic."""
        self._subscribers.setdefault(topic, []).append(handler)
        logger.debug("🔌 Synapse connected to topic: %s", topic)

    async def unsubscribe(self, topic: str, handler: Subscriber) -> None:
        """Detach an async handler to avoid websocket/session leaks."""
        handlers = self._subscribers.get(topic)
        if not handlers:
            return
        self._subscribers[topic] = [registered for registered in handlers if registered is not handler]
        if not self._subscribers[topic]:
            self._subscribers.pop(topic, None)
        logger.debug("🔌 Synapse disconnected from topic: %s", topic)

    async def emit(self, topic: str, payload: Any) -> None:
        """Broadcast an event and store a bounded history buffer."""
        if not self._is_active:
            return

        event = {"ts": time.time(), "topic": topic, "payload": payload}
        self._history.append(event)
        if len(self._history) > self._history_limit:
            self._history = self._history[-self._history_limit :]

        handlers = list(self._subscribers.get(topic, []))
        if handlers:
            tasks = [asyncio.create_task(handler(payload)) for handler in handlers]
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("⚡ [SIGNAL] %s: %s...", topic, str(payload)[:80])

    def recent_events(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Return most recent events first for dashboards and diagnostics."""
        bounded_limit = max(1, min(limit, self._history_limit))
        return list(reversed(self._history[-bounded_limit:]))

    async def shutdown(self) -> None:
        self._is_active = False
        self._subscribers.clear()
        logger.info("💤 AetherBus Extreme: System Shutdown")
