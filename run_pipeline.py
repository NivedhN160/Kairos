"""Native Python Multi-Process Supervisor for Kairos (Zero Docker Required).

Runs the web API and background ingestion workers with graceful shutdown.
Usage:
  python run_pipeline.py
"""

import sys
import os
import uvicorn
from core.config import settings

def main():
    print("=" * 60)
    print("⚡ Starting KAIROS: Live Event Intelligence Engine")
    print(f"   Host: {settings.HOST} | Port: {settings.PORT}")
    print(f"   Embedded Workers: {settings.RUN_EMBEDDED_WORKERS}")
    print(f"   Broker: {'Redis (' + settings.REDIS_URL + ')' if settings.REDIS_URL else 'In-Memory Stream Broker'}")
    print(f"   Database: {'PostgreSQL' if settings.DATABASE_URL else 'SQLite Dev'}")
    print("=" * 60)

    # Run Uvicorn directly
    uvicorn.run(
        "api.main:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False
    )

if __name__ == "__main__":
    main()
