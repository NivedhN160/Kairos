import asyncio
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from core.config import settings
from core.db import init_db
from core.redis import stream_client
from fetcher.worker import pipeline_runner
from api.routes_events import router as events_router
from api.routes_system import router as system_router
from api.sse import event_generator

logger = logging.getLogger("kairos.api")
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
STATIC_DIR = BASE_DIR / "web" / "static"

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager: starts database, broker, and embedded pipeline workers."""
    logger.info("Initializing Kairos core infrastructure...")
    # Initialize database tables
    await init_db()
    # Connect stream client (Redis or in-memory)
    await stream_client.connect()

    pipeline_task = None
    if settings.RUN_EMBEDDED_WORKERS:
        logger.info("Starting embedded pipeline ingestion workers...")
        pipeline_task = asyncio.create_task(pipeline_runner.start())

    yield

    logger.info("Shutting down Kairos...")
    if pipeline_runner.is_running:
        pipeline_runner.stop()
    if pipeline_task:
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass

app = FastAPI(
    title="KAIROS: Live Event Intelligence Engine",
    description="The world, clustered, as it happens. Real-time news event intelligence platform.",
    version="1.0.0",
    lifespan=lifespan
)

# Include API sub-routers
app.include_router(events_router)
app.include_router(system_router)

# Mount static assets
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(STATIC_DIR / "favicon.svg", media_type="image/svg+xml")

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def live_feed_page(request: Request):
    """Renders the main live event stream feed."""
    return FileResponse(TEMPLATES_DIR / "index.html", media_type="text/html")

@app.api_route("/system", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def system_observability_page(request: Request):
    """Renders the system health and stage latency flex page."""
    return FileResponse(TEMPLATES_DIR / "system.html", media_type="text/html")

@app.get("/live")
async def sse_live_endpoint():
    """Server-Sent Events (SSE) live push stream for real-time frontend updates."""
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    """Health check endpoint for Render / load balancers."""
    return {
        "status": "healthy",
        "service": "kairos",
        "version": "1.0.0",
        "embedded_workers": settings.RUN_EMBEDDED_WORKERS
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host=settings.HOST, port=settings.PORT, reload=False)
