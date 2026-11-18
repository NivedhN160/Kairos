from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from core.models import Event
from events.clusterer import event_clusterer

router = APIRouter(prefix="/api/events", tags=["events"])

@router.get("", response_model=List[Event])
async def list_events(limit: int = 50, status: Optional[str] = None):
    """Returns real-time clustered events sorted by recency."""
    events = event_clusterer.get_all_events()
    if status:
        events = [e for e in events if e.status == status]
    return events[:limit]

@router.get("/{event_id}", response_model=Event)
async def get_event(event_id: str):
    """Returns detailed event information including timeline and sources."""
    event = event_clusterer.get_event(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event

@router.get("/search")
async def search_events(q: str = Query(..., min_length=2)):
    """Searches active events by title, keywords, or entities."""
    query_lower = q.lower()
    matches = []
    for ev in event_clusterer.get_all_events():
        if (query_lower in ev.title.lower() or 
            any(query_lower in ent.lower() for ent in ev.entities) or
            any(query_lower in src.lower() for src in ev.sources)):
            matches.append(ev)
    return matches
