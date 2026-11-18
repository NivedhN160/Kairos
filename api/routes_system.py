import time
from typing import Dict, Any, List
from fastapi import APIRouter
from core.tracing import metrics
from scheduler.registry import registry
from events.clusterer import event_clusterer

router = APIRouter(prefix="/api/system", tags=["system"])

@router.get("/metrics")
async def get_system_metrics() -> Dict[str, Any]:
    """Returns real-time pipeline telemetry, latency waterfall, and throughput."""
    stage_summary = metrics.get_summary()
    sources = registry.get_all()
    events = event_clusterer.get_all_events()

    healthy_count = sum(1 for s in sources if s.status == "healthy")
    degraded_count = sum(1 for s in sources if s.status == "degraded")
    circuit_open_count = sum(1 for s in sources if s.status == "circuit_open")

    return {
        "pipeline_version": "v1.0.0",
        "timestamp": time.time(),
        "stages": stage_summary,
        "sources_summary": {
            "total": len(sources),
            "healthy": healthy_count,
            "degraded": degraded_count,
            "circuit_open": circuit_open_count
        },
        "events_summary": {
            "total_active_events": len(events),
            "emerging": sum(1 for e in events if e.status == "emerging"),
            "active": sum(1 for e in events if e.status == "active"),
            "cooling": sum(1 for e in events if e.status == "cooling")
        }
    }

@router.get("/sources")
async def get_sources_grid() -> List[Dict[str, Any]]:
    """Returns the live status of all registered sources for the health dot grid."""
    return [
        {
            "id": s.id,
            "name": s.name,
            "category": s.category,
            "tier": s.tier,
            "status": s.status,
            "poll_interval_s": s.poll_interval_s,
            "last_fetch_at": s.last_fetch_at.isoformat() if s.last_fetch_at else None,
            "consecutive_failures": s.consecutive_failures
        }
        for s in registry.get_all()
    ]
