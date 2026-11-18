import os
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Web & Hosting (Render dynamically binds $PORT)
    PORT: int = int(os.getenv("PORT", 8000))
    HOST: str = os.getenv("HOST", "0.0.0.0")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Embedded Pipeline Workers (Ideal for single Web Service hosting on Render free tier)
    RUN_EMBEDDED_WORKERS: bool = os.getenv("RUN_EMBEDDED_WORKERS", "true").lower() in ("true", "1", "yes")

    # Storage & Broker (PostgreSQL + Redis)
    # If left empty, Kairos runs using in-memory streams & SQLite for instant zero-dependency testing
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL")

    # Pipeline Parameters
    PIPELINE_VERSION: str = "v1.0.0"
    FETCH_CONCURRENCY: int = int(os.getenv("FETCH_CONCURRENCY", 5))
    DEFAULT_POLL_INTERVAL: int = int(os.getenv("DEFAULT_POLL_INTERVAL", 60))

    # Optional Free-Tier LLM Keys
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    GROQ_API_KEY: Optional[str] = os.getenv("GROQ_API_KEY")

    class Config:
        env_file = ".env"
        extra = "allow"

    @property
    def async_database_url(self) -> str:
        """Ensures the DATABASE_URL uses the asyncpg driver for PostgreSQL."""
        if not self.DATABASE_URL:
            return "sqlite+aiosqlite:///./kairos_dev.db"
        url = self.DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

settings = Settings()
