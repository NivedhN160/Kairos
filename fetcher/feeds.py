import hashlib
import logging
from typing import List, Tuple, Optional
from datetime import datetime, timezone
import httpx
import feedparser
from core.models import Source, RawArticle, utc_now
from core.tracing import generate_trace_id
from fetcher.politeness import politeness

logger = logging.getLogger("kairos.fetcher.feeds")

USER_AGENT = "KairosNewsBot/1.0 (+https://github.com/NivedhN160/Kairos; personal-aggregator)"

async def fetch_feed_items(source: Source) -> Tuple[List[RawArticle], Optional[str], Optional[str], int]:
    """Asynchronously fetches and parses an RSS/Atom feed using conditional GET.
    Returns: (new_raw_articles, new_etag, new_last_modified, http_status_code)
    """
    await politeness.wait_for_host(source.url)

    headers = {"User-Agent": USER_AGENT}
    if source.etag:
        headers["If-None-Match"] = source.etag
    if source.last_modified:
        headers["If-Modified-Since"] = source.last_modified

    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        try:
            response = await client.get(source.url, headers=headers)
        except Exception as e:
            logger.warning(f"Network error fetching {source.name} ({source.url}): {e}")
            raise e

    # 304 Not Modified -> feed has no changes
    if response.status_code == 304:
        return [], source.etag, source.last_modified, 304

    if response.status_code >= 400:
        logger.warning(f"HTTP {response.status_code} error from {source.url}")
        return [], source.etag, source.last_modified, response.status_code

    new_etag = response.headers.get("ETag")
    new_last_modified = response.headers.get("Last-Modified")

    # Parse XML content
    parsed = feedparser.parse(response.content)
    raw_articles: List[RawArticle] = []

    for entry in parsed.entries:
        title = entry.get("title", "").strip()
        url = entry.get("link", "").strip()
        guid = entry.get("id", url) or url
        if not title or not url:
            continue

        summary = entry.get("summary", "") or entry.get("description", "") or ""

        # Parse published date
        published_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                published_at = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                published_at = utc_now()
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            try:
                published_at = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                published_at = utc_now()
        else:
            published_at = utc_now()

        # Compute deterministic raw hash
        hash_input = f"{source.id}:{guid}:{title}".encode("utf-8")
        raw_hash = hashlib.sha256(hash_input).hexdigest()

        raw_articles.append(
            RawArticle(
                source_id=source.id,
                guid=guid,
                url=url,
                title=title,
                summary=summary,
                published_at=published_at,
                fetched_at=utc_now(),
                raw_hash=raw_hash,
                trace_id=generate_trace_id()
            )
        )

    return raw_articles, new_etag, new_last_modified, response.status_code
