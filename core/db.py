import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from core.config import settings

logger = logging.getLogger("kairos.db")

# Create asynchronous engine
# Note: connect_args={"check_same_thread": False} only applies to sqlite
is_sqlite = settings.async_database_url.startswith("sqlite")
connect_args = {"check_same_thread": False} if is_sqlite else {}

engine = create_async_engine(
    settings.async_database_url,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# SQL statements to initialize PostgreSQL or SQLite tables
POSTGRES_SCHEMA = """
-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1. Sources table
CREATE TABLE IF NOT EXISTS sources (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL UNIQUE,
    category VARCHAR(64) NOT NULL,
    type VARCHAR(32) DEFAULT 'rss',
    tier INT DEFAULT 2,
    reliability FLOAT DEFAULT 1.0,
    lang VARCHAR(16) DEFAULT 'en',
    poll_interval_s INT DEFAULT 60,
    etag TEXT,
    last_modified TEXT,
    last_fetch_at TIMESTAMPTZ,
    avg_new_per_hour FLOAT DEFAULT 2.0,
    status VARCHAR(32) DEFAULT 'healthy',
    consecutive_failures INT DEFAULT 0
);

-- 2. Raw log (immutable)
CREATE TABLE IF NOT EXISTS raw_log (
    offset_id BIGSERIAL PRIMARY KEY,
    source_id VARCHAR(64) REFERENCES sources(id),
    guid TEXT NOT NULL,
    url TEXT NOT NULL,
    payload JSONB NOT NULL,
    raw_hash VARCHAR(64) NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    trace_id VARCHAR(64) NOT NULL
);

-- 3. Articles table
CREATE TABLE IF NOT EXISTS articles (
    id VARCHAR(64) PRIMARY KEY,
    source_id VARCHAR(64) REFERENCES sources(id),
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    lang VARCHAR(16) DEFAULT 'en',
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    simhash BIGINT,
    canonical_article_id VARCHAR(64),
    pipeline_version VARCHAR(32) DEFAULT 'v1.0.0',
    trace_id VARCHAR(64) NOT NULL
);

-- 4. Article duplicates
CREATE TABLE IF NOT EXISTS article_dupes (
    id BIGSERIAL PRIMARY KEY,
    article_id VARCHAR(64) REFERENCES articles(id),
    canonical_id VARCHAR(64) REFERENCES articles(id),
    level VARCHAR(32) NOT NULL,
    similarity FLOAT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5. Events table
CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(64) PRIMARY KEY,
    title TEXT NOT NULL,
    status VARCHAR(32) DEFAULT 'emerging',
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    article_count INT DEFAULT 1,
    source_count INT DEFAULT 1,
    sources JSONB DEFAULT '[]'::jsonb,
    summary TEXT,
    summary_hash VARCHAR(64),
    pipeline_version VARCHAR(32) DEFAULT 'v1.0.0'
);

-- 6. Event articles relationship
CREATE TABLE IF NOT EXISTS event_articles (
    event_id VARCHAR(64) REFERENCES events(id),
    article_id VARCHAR(64) REFERENCES articles(id),
    score FLOAT DEFAULT 1.0,
    role VARCHAR(32) DEFAULT 'followup',
    PRIMARY KEY (event_id, article_id)
);

-- 7. Timeline items
CREATE TABLE IF NOT EXISTS timeline_items (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) REFERENCES events(id),
    article_id VARCHAR(64) REFERENCES articles(id),
    at TIMESTAMPTZ NOT NULL,
    kind VARCHAR(32) NOT NULL,
    headline TEXT NOT NULL,
    source_name VARCHAR(255) NOT NULL
);
"""

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    type TEXT DEFAULT 'rss',
    tier INTEGER DEFAULT 2,
    reliability REAL DEFAULT 1.0,
    lang TEXT DEFAULT 'en',
    poll_interval_s INTEGER DEFAULT 60,
    etag TEXT,
    last_modified TEXT,
    last_fetch_at TIMESTAMP,
    avg_new_per_hour REAL DEFAULT 2.0,
    status TEXT DEFAULT 'healthy',
    consecutive_failures INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS raw_log (
    offset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    guid TEXT NOT NULL,
    url TEXT NOT NULL,
    payload TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    trace_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS articles (
    id TEXT PRIMARY KEY,
    source_id TEXT,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    lang TEXT DEFAULT 'en',
    published_at TIMESTAMP NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    content_hash TEXT NOT NULL,
    simhash INTEGER,
    canonical_article_id TEXT,
    pipeline_version TEXT DEFAULT 'v1.0.0',
    trace_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS article_dupes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id TEXT,
    canonical_id TEXT,
    level TEXT NOT NULL,
    similarity REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT DEFAULT 'emerging',
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    article_count INTEGER DEFAULT 1,
    source_count INTEGER DEFAULT 1,
    sources TEXT DEFAULT '[]',
    summary TEXT,
    summary_hash TEXT,
    pipeline_version TEXT DEFAULT 'v1.0.0'
);

CREATE TABLE IF NOT EXISTS event_articles (
    event_id TEXT,
    article_id TEXT,
    score REAL DEFAULT 1.0,
    role TEXT DEFAULT 'followup',
    PRIMARY KEY (event_id, article_id)
);

CREATE TABLE IF NOT EXISTS timeline_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    article_id TEXT,
    at TIMESTAMP NOT NULL,
    kind TEXT NOT NULL,
    headline TEXT NOT NULL,
    source_name TEXT NOT NULL
);
"""

async def init_db():
    """Initializes database tables."""
    try:
        async with engine.begin() as conn:
            schema = SQLITE_SCHEMA if is_sqlite else POSTGRES_SCHEMA
            # Split commands by semicolon to execute individually
            for statement in schema.split(";"):
                stmt = statement.strip()
                if stmt:
                    try:
                        await conn.execute(text(stmt))
                    except Exception as e:
                        # Don't fail if vector extension isn't available in standard postgres
                        if "vector" in stmt.lower():
                            logger.warning(f"Vector extension skipped (optional for basic ingestion): {e}")
                        else:
                            logger.warning(f"Error executing statement: {e}")
        logger.info(f"Database initialized successfully (engine: {'sqlite' if is_sqlite else 'postgresql'}).")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

async def get_db():
    """Dependency for obtaining an async session."""
    async with AsyncSessionLocal() as session:
        yield session
