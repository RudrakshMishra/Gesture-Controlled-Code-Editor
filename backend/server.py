# backend/server.py
"""
Production-grade FastAPI backend server for Gesture-Controlled Code Editor.
Implements advanced async processing pipeline with dependency injection,
connection state management, and pluggable gesture processing chains.
Design follows hexagonal architecture principles with domain-driven boundaries.
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional, Callable, Awaitable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel, validator

from websocket_handler import AdvancedConnectionManager, ConnectionState
from api_routes import router as api_router
from gesture_engine.gesture_mapper import GestureMapper
from gesture_engine.config_loader import ConfigLoader
from gesture_engine.command_registry import CommandRegistry
from gesture_engine.state_manager import GestureStateManager

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('gesture_server.log')
    ]
)
logger = logging.getLogger(__name__)

class GestureEvent(BaseModel):
    """Pydantic model for incoming gesture events from vision pipeline."""
    timestamp: float
    session_id: str
    hand_id: int
    gesture_type: str
    confidence: float
    landmarks: List[Dict[str, float]]  # Normalized MediaPipe landmarks
    velocity: Optional[Dict[str, float]] = None
    direction: Optional[str] = None
    
    @validator('confidence')
    def confidence_must_be_valid(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('confidence must be between 0.0 and 1.0')
        return v

class ProcessingChain:
    """Composable async processing chain for gesture events using functional composition."""
    
    def __init__(self):
        self._processors: List[Callable[[GestureEvent], Awaitable[GestureEvent]]] = []
    
    def add_processor(self, processor: Callable[[GestureEvent], Awaitable[GestureEvent]]):
        """Add a processor to the chain."""
        self._processors.append(processor)
        return self
    
    async def process(self, event: GestureEvent) -> GestureEvent:
        """Execute the full processing chain."""
        processed = event
        for processor in self._processors:
            processed = await processor(processed)
        return processed

class GestureProcessingOrchestrator:
    """Orchestrates gesture processing with pluggable chains and stateful context."""
    
    def __init__(
        self,
        mapper: GestureMapper,
        registry: CommandRegistry,
        state_manager: GestureStateManager,
        config_loader: ConfigLoader
    ):
        self.mapper = mapper
        self.registry = registry
        self.state_manager = state_manager
        self.config_loader = config_loader
        self.chains: Dict[str, ProcessingChain] = {}
        self._init_chains()
    
    def _init_chains(self):
        """Initialize default processing chains based on config."""
        config = self.config_loader.load_gestures()
        
        for gesture_group, processors in config.get('processing_chains', {}).items():
            chain = ProcessingChain()
            for proc_config in processors:
                # Dynamic processor instantiation (extensible)
                proc_type = proc_config.get('type')
                if proc_type == 'debounce':
                    chain.add_processor(self._debounce_factory(proc_config))
                elif proc_type == 'velocity_filter':
                    chain.add_processor(self._velocity_filter_factory(proc_config))
                # Add more processor types here
            self.chains[gesture_group] = chain
    
    def _debounce_factory(self, config: Dict[str, Any]) -> Callable[[GestureEvent], Awaitable[GestureEvent]]:
        """Factory for debounce processor."""
        debounce_ms = config.get('debounce_ms', 100)
        last_time = {'timestamp': 0}
        
        async def debounce(event: GestureEvent) -> GestureEvent:
            if event.timestamp - last_time['timestamp'] < debounce_ms / 1000.0:
                raise asyncio.CancelledError("Debounced")
            last_time['timestamp'] = event.timestamp
            return event
        return debounce
    
    def _velocity_filter_factory(self, config: Dict[str, Any]) -> Callable[[GestureEvent], Awaitable[GestureEvent]]:
        """Factory for velocity-based filtering."""
        min_velocity = config.get('min_velocity', 0.1)
        
        async def velocity_filter(event: GestureEvent) -> GestureEvent:
            if event.velocity and event.velocity.get('magnitude', 0) < min_velocity:
                raise asyncio.CancelledError("Velocity too low")
            return event
        return velocity_filter
    
    async def process_gesture(self, event: GestureEvent, connection_id: str) -> Optional[Dict[str, Any]]:
        """Main gesture processing entrypoint."""
        try:
            # Stateful context injection
            state = self.state_manager.get_state(connection_id)
            event = event.copy(update={'context': state.context})
            
            # Route to appropriate chain
            chain_key = self._get_chain_key(event)
            chain = self.chains.get(chain_key)
            if not chain:
                logger.warning(f"No chain for {chain_key}")
                return None
            
            processed = await chain.process(event)
            
            # Map to command
            command = await self.mapper.map_gesture(processed, state)
            if command:
                cmd_info = self.registry.register_command(command)
                self.state_manager.update_state(connection_id, processed)
                return {
                    'command_id': cmd_info.id,
                    'action': command.action,
                    'params': command.params,
                    'timestamp': processed.timestamp
                }
            return None
        except asyncio.CancelledError:
            logger.debug("Gesture cancelled by processor")
            return None
        except Exception as e:
            logger.error(f"Gesture processing error: {e}", exc_info=True)
            return None
    
    def _get_chain_key(self, event: GestureEvent) -> str:
        """Determine processing chain key."""
        return f"{event.gesture_type}_{len(event.landmarks)}"

# Dependency injection container (manual for production control)
class DIContainer:
    """Simple dependency injection container."""
    
    def __init__(self):
        self._services: Dict[str, Any] = {}
    
    def register(self, name: str, service: Any):
        self._services[name] = service
    
    def get(self, name: str) -> Any:
        return self._services.get(name)

# Global app state
app_state = {
    'connections': AdvancedConnectionManager(),
    'orchestrator': None,
    'container': DIContainer()
}

def get_orchestrator() -> GestureProcessingOrchestrator:
    """Dependency for orchestrator."""
    if not app_state['orchestrator']:
        raise HTTPException(status_code=503, detail="System not initialized")
    return app_state['orchestrator']

def get_connection_manager() -> AdvancedConnectionManager:
    """Dependency for connection manager."""
    return app_state['connections']

@asynccontextmanager
async def lifespan(app_: FastAPI):
    """App lifespan manager for startup/shutdown."""
    # Startup
    logger.info("Initializing Gesture-Controlled Code Editor Backend")
    
    # Initialize dependencies
    container = app_state['container']
    config_loader = ConfigLoader()
    registry = CommandRegistry()
    state_manager = GestureStateManager()
    mapper = GestureMapper(config_loader, registry)
    
    container.register('config_loader', config_loader)
    container.register('registry', registry)
    container.register('state_manager', state_manager)
    container.register('mapper', mapper)
    
    orchestrator = GestureProcessingOrchestrator(
        mapper=mapper,
        registry=registry,
        state_manager=state_manager,
        config_loader=config_loader
    )
    app_state['orchestrator'] = orchestrator
    
    logger.info("Backend initialization complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down backend")
    await app_state['connections'].close_all()
    logger.info("Backend shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Gesture-Controlled Code Editor Backend",
    description="Production WebSocket server for gesture-based IDE control",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
app.state.app_state = app_state

@app.websocket("/ws/gestures/{client_id}")
async def websocket_gestures(
    websocket: WebSocket,
    client_id: str,
    orchestrator: GestureProcessingOrchestrator = Depends(get_orchestrator),
    manager: AdvancedConnectionManager = Depends(get_connection_manager)
):
    """Primary WebSocket endpoint for gesture events."""
    await manager.connect(websocket, client_id)
    try:
        while True:
            data = await websocket.receive_json()
            event = GestureEvent(**data)
            
            # Async processing with timeout
            processed_cmd = await asyncio.wait_for(
                orchestrator.process_gesture(event, client_id),
                timeout=0.05  # 50ms processing budget
            )
            
            if processed_cmd:
                await manager.send_personal_message(processed_cmd, websocket)
                await manager.broadcast_gesture_event(processed_cmd, client_id)
                
    except asyncio.TimeoutError:
        logger.warning(f"Gesture processing timeout for {client_id}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, client_id)
        logger.info(f"Client {client_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for {client_id}: {e}", exc_info=True)
        await manager.disconnect(websocket, client_id)

@app.get("/health")
async def health_check(manager: AdvancedConnectionManager = Depends(get_connection_manager)):
    """Health check endpoint."""
    return {
        "status": "healthy",
        "active_connections": len(manager.active_connections),
        "uptime": "production_ready"
    }

def handle_shutdown(signum, frame):
    """Graceful shutdown handler."""
    logger.info(f"Received signal {signum}, initiating shutdown")
    loop = asyncio.get_event_loop()
    loop.create_task(app_state['connections'].close_all())
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8001,
        log_level="info",
        workers=1,  # Single worker for WebSocket state
        reload=False,
        access_log=True
    )
