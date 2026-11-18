from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class Source(BaseModel):
    id: str
    name: str
    url: str
    category: str
    type: str = "rss"                      # rss, atom, api
    tier: int = 2                          # 1 = breaking, 2 = normal, 3 = slow
    reliability: float = 1.0               # 0.0 to 1.0
    lang: str = "en"
    poll_interval_s: int = 60
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    last_fetch_at: Optional[datetime] = None
    avg_new_per_hour: float = 2.0
    status: str = "healthy"                # healthy, degraded, failed, circuit_open
    consecutive_failures: int = 0

class RawArticle(BaseModel):
    source_id: str
    guid: str
    url: str
    title: str
    summary: str
    published_at: Optional[datetime] = None
    fetched_at: datetime = Field(default_factory=utc_now)
    raw_hash: str
    trace_id: str

class NormalizedArticle(BaseModel):
    id: str
    source_id: str
    canonical_url: str
    title: str
    body: str
    lang: str = "en"
    published_at: datetime
    fetched_at: datetime
    content_hash: str
    simhash: Optional[int] = None
    canonical_article_id: Optional[str] = None
    pipeline_version: str = "v1.0.0"
    trace_id: str

class ArticleDup(BaseModel):
    article_id: str
    canonical_id: str
    level: str                             # L1_exact, L2_near_dup, L3_semantic
    similarity: float

class TimelineItem(BaseModel):
    event_id: str
    at: datetime = Field(default_factory=utc_now)
    kind: str = "report"                   # first_report, update, official_statement, escalation
    article_id: str
    headline: str
    source_name: str

class Event(BaseModel):
    id: str
    title: str
    status: str = "emerging"               # emerging, active, cooling, dead
    first_seen: datetime = Field(default_factory=utc_now)
    last_seen: datetime = Field(default_factory=utc_now)
    article_count: int = 1
    source_count: int = 1
    sources: List[str] = Field(default_factory=list)
    articles: List[Dict[str, Any]] = Field(default_factory=list)
    entities: List[str] = Field(default_factory=list)
    summary: Optional[str] = None
    summary_hash: Optional[str] = None
    velocity: float = 1.0                  # articles per hour
    tier_mix: Dict[str, int] = Field(default_factory=dict)
    timeline: List[TimelineItem] = Field(default_factory=list)
    personal_score: float = 0.0

class LiveDelta(BaseModel):
    event_id: str
    event_title: str
    delta_type: str                        # created, updated, merged
    article_count: int
    source_count: int
    latest_article_title: str
    timestamp: datetime = Field(default_factory=utc_now)
