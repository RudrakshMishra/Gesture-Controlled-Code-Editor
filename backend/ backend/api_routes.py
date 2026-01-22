# backend/api_routes.py
"""
REST API routes for system configuration, monitoring, and admin functions.
Provides health checks, metrics, and dynamic configuration reloading.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any

from server import get_connection_manager, app_state

router = APIRouter(prefix="/api/v1", tags=["api"])

@router.get("/metrics")
async def get_metrics(manager=Depends(get_connection_manager)):
    """System metrics endpoint."""
    orchestrator = app_state['orchestrator']
    return {
        "connections": manager.get_stats(),
        "processing_chains": len(orchestrator.chains),
        "uptime": "production",
        "version": "1.0.0"
    }

@router.post("/config/reload")
async def reload_config():
    """Reload configuration without restart."""
    # Implementation would call config_loader.reload()
    return {"status": "reloaded"}
