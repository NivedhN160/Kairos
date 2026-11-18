import json
import asyncio
import logging
from typing import AsyncGenerator
from core.redis import stream_client

logger = logging.getLogger("kairos.sse")

async def event_generator() -> AsyncGenerator[str, None]:
    """Generates Server-Sent Events (SSE) for connected web clients with heartbeat pings."""
    queue = stream_client.mem_broker.subscribe()
    try:
        # Initial greeting event
        yield f"event: connected\ndata: {json.dumps({'status': 'connected', 'message': 'Kairos Live SSE Stream active'})}\n\n"

        while True:
            try:
                # Wait for next event or 15-second heartbeat ping
                item = await asyncio.wait_for(queue.get(), timeout=15.0)
                event_type = item.get("channel", "event.updated")
                payload = item.get("data", "{}")
                yield f"event: {event_type}\ndata: {payload}\n\n"
            except asyncio.TimeoutError:
                # Keep-alive ping to prevent Render proxy timeout
                yield ": heartbeat ping\n\n"
    except asyncio.CancelledError:
        logger.debug("SSE client disconnected.")
    finally:
        stream_client.mem_broker.unsubscribe(queue)
