import asyncio
import logging
import hashlib
from typing import List
from datetime import datetime, timezone
from core.models import Source, RawArticle, NormalizedArticle, utc_now
from core.tracing import TraceContext, metrics
from core.redis import stream_client
from scheduler.registry import registry
from scheduler.adaptive import calculate_adaptive_interval
from fetcher.breaker import circuit_breaker
from fetcher.feeds import fetch_feed_items
from normalizer.canonical import canonicalize_url
from normalizer.extract import clean_html_text
from dedup.worker import dedup_engine
from events.clusterer import event_clusterer

logger = logging.getLogger("kairos.pipeline")

class PipelineRunner:
    """End-to-end continuous ingestion, normalization, deduplication, and clustering pipeline."""
    def __init__(self):
        self.is_running = False
        self._task: asyncio.Task = None

    async def start(self):
        self.is_running = True
        logger.info("Kairos real-time event intelligence pipeline worker started.")
        while self.is_running:
            try:
                await self.poll_cycle()
            except Exception as e:
                logger.error(f"Error in pipeline poll cycle: {e}", exc_info=True)
            # Short sleep between polling sweeps
            await asyncio.sleep(5)

    def stop(self):
        self.is_running = False

    async def poll_cycle(self):
        sources = registry.get_all()
        now = utc_now()

        for source in sources:
            # Check circuit breaker
            if not circuit_breaker.is_available(source.id):
                registry.update_status(source.id, "circuit_open")
                continue

            # Check if due for fetching
            if source.last_fetch_at:
                elapsed = (now - source.last_fetch_at).total_seconds()
                if elapsed < source.poll_interval_s:
                    continue

            # Fetch feed asynchronously
            await self._process_source(source)

    async def _process_source(self, source: Source):
        source.last_fetch_at = utc_now()
        with TraceContext("fetch_and_parse", f"src_{source.id}"):
            try:
                raw_articles, new_etag, new_last_mod, status_code = await fetch_feed_items(source)
                source.etag = new_etag
                source.last_modified = new_last_mod
                circuit_breaker.record_success(source.id)
                registry.update_status(source.id, "healthy")
            except Exception as e:
                circuit_breaker.record_failure(source.id)
                registry.update_status(source.id, "degraded", failure=True)
                return

        new_items_count = 0

        for raw_item in raw_articles:
            # S1: Seen Guard (Redis Bloom / Set)
            is_seen = await stream_client.check_seen(source.id, raw_item.guid)
            if is_seen:
                continue

            await stream_client.mark_seen(source.id, raw_item.guid)
            new_items_count += 1

            # Emit to Stream S1
            await stream_client.emit_stream("s1.raw", raw_item.model_dump(mode="json"))

            # S2: Normalizer
            with TraceContext("normalize", raw_item.trace_id):
                canonical_url = canonicalize_url(raw_item.url)
                clean_body = clean_html_text(f"{raw_item.title}. {raw_item.summary}")
                content_hash = hashlib.sha256(clean_body.encode("utf-8")).hexdigest()

                norm_article = NormalizedArticle(
                    id=f"art_{hashlib.md5(canonical_url.encode()).hexdigest()[:12]}",
                    source_id=source.id,
                    canonical_url=canonical_url,
                    title=raw_item.title,
                    body=clean_body,
                    lang="en",
                    published_at=raw_item.published_at or utc_now(),
                    fetched_at=raw_item.fetched_at,
                    content_hash=content_hash,
                    trace_id=raw_item.trace_id
                )

            await stream_client.emit_stream("s2.normalized", norm_article.model_dump(mode="json"))

            # S3: Deduplication (L1 Exact + L2 SimHash LSH)
            with TraceContext("dedup", raw_item.trace_id):
                is_dup, canonical_id, level, sim = await dedup_engine.check_duplicate(
                    article_id=norm_article.id,
                    canonical_url=canonical_url,
                    content_hash=content_hash,
                    body_text=clean_body
                )

            if is_dup:
                logger.debug(f"Duplicate story detected [{level}] ({sim:.2f}): {norm_article.title[:40]}")
                norm_article.canonical_article_id = canonical_id
                continue

            await stream_client.emit_stream("s3.unique", norm_article.model_dump(mode="json"))

            # S6: Online Event Clustering
            with TraceContext("cluster", raw_item.trace_id):
                event, is_new = await event_clusterer.process_article(
                    article_id=norm_article.id,
                    title=norm_article.title,
                    body=norm_article.body,
                    source_id=source.id,
                    source_name=source.name,
                    published_at=norm_article.published_at
                )

        # Adaptively update poll interval based on new items yield
        if new_items_count > 0:
            source.avg_new_per_hour = (source.avg_new_per_hour * 0.7) + (new_items_count * 0.3)
            source.poll_interval_s = calculate_adaptive_interval(source.avg_new_per_hour, tier=source.tier)

pipeline_runner = PipelineRunner()
