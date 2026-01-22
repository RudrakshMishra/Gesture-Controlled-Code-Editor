# backend/websocket_handler.py
"""
Advanced WebSocket connection management with heartbeat, backpressure handling,
and connection lifecycle events. Production patterns for high-reliability real-time systems.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from enum import Enum
from dataclasses import dataclass, field

from fastapi import WebSocket, WebSocketState
import msgpack  # Optional: faster serialization

logger = logging.getLogger(__name__)

class ConnectionState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTING = "disconnecting"
    DISCONNECTED = "disconnected"
    FAILED = "failed"

@dataclass
class ConnectionMetadata:
    """Rich connection metadata for analytics and debugging."""
    connected_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: Optional[datetime] = None
    heartbeat_interval: float = 30.0
    message_count: int = 0
    gesture_rate: float = 0.0  # gestures/sec
    avg_latency: Optional[float] = None
    session_features: Dict[str, Any] = field(default_factory=dict)

class BackpressureMonitor:
    """Monitors and handles WebSocket backpressure."""
    
    def __init__(self, warning_threshold: float = 0.8, max_queue_size: int = 100):
        self.warning_threshold = warning_threshold
        self.max_queue_size = max_queue_size
        self.queue_sizes: List[int] = []
        
    def should_throttle(self, current_size: int) -> bool:
        self.queue_sizes.append(current_size)
        if len(self.queue_sizes) > 100:
            self.queue_sizes.pop(0)
        avg_size = sum(self.queue_sizes) / len(self.queue_sizes)
        return avg_size > self.max_queue_size * self.warning_threshold

class AdvancedConnectionManager:
    """Production-grade WebSocket manager with heartbeats, backpressure, and metrics."""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.metadata: Dict[str, ConnectionMetadata] = {}
        self.backpressure_monitor = BackpressureMonitor()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept connection with initial handshake."""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.metadata[client_id] = ConnectionMetadata()
        
        logger.info(f"Client {client_id} connected. Total: {len(self.active_connections)}")
        
        # Start client-specific heartbeat
        asyncio.create_task(self._client_heartbeat(client_id))
    
    def disconnect(self, websocket: WebSocket, client_id: str):
        """Clean disconnection."""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self.metadata:
            del self.metadata[client_id]
        logger.info(f"Client {client_id} disconnected. Total: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: Any, websocket: WebSocket):
        """Send message to specific connection with backpressure handling."""
        if websocket.client_state != WebSocketState.CONNECTED:
            return
            
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
    
    async def broadcast_gesture_event(self, event: Dict[str, Any], exclude_client: str):
        """Broadcast to all except sender with rate limiting."""
        if self.backpressure_monitor.should_throttle(len(self.active_connections)):
            logger.warning("Backpressure detected, skipping broadcast")
            return
            
        broadcast_msg = {
            "type": "gesture_broadcast",
            "event": event,
            "timestamp": asyncio.get_event_loop().time()
        }
        
        tasks = []
        for client_id, ws in self.active_connections.items():
            if client_id != exclude_client:
                tasks.append(self.send_personal_message(broadcast_msg, ws))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _client_heartbeat(self, client_id: str):
        """Per-client heartbeat with timeout detection."""
        metadata = self.metadata[client_id]
        while client_id in self.active_connections:
            try:
                await asyncio.sleep(metadata.heartbeat_interval)
                await self.active_connections[client_id].send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat()
                })
                metadata.last_heartbeat = datetime.now()
            except Exception as e:
                logger.warning(f"Heartbeat failed for {client_id}: {e}")
                break
    
    async def close_all(self):
        """Graceful shutdown of all connections."""
        logger.info("Closing all connections")
        for client_id in list(self.active_connections.keys()):
            await self.disconnect_websocket(client_id)
    
    async def disconnect_websocket(self, client_id: str):
        """Force disconnect with cleanup."""
        ws = self.active_connections.get(client_id)
        if ws:
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.close(code=1000, reason="Server shutdown")
            except Exception:
                pass
            self.disconnect(ws, client_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Connection statistics for monitoring."""
        return {
            "active_connections": len(self.active_connections),
            "backpressure_status": self.backpressure_monitor.should_throttle(0),
            "avg_queue_size": getattr(self.backpressure_monitor, 'queue_sizes', [0])
        }
