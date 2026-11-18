import re
import math
import uuid
import logging
from typing import Dict, List, Optional, Tuple, Set, Any
from datetime import datetime, timezone
from core.models import Event, TimelineItem, LiveDelta, utc_now
from core.redis import stream_client

logger = logging.getLogger("kairos.clusterer")

def extract_entities_and_keywords(text: str) -> Set[str]:
    """Lightweight extractor for capitalized proper nouns and distinctive terms."""
    # Find capitalized multi-word phrases and single proper nouns
    proper_nouns = set(re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text))
    # Add significant keywords (length >= 4, lowercased)
    words = {w.lower() for w in re.findall(r"\b[a-zA-Z]{4,}\b", text)}
    return proper_nouns.union(words)

def jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return float(intersection) / float(union) if union > 0 else 0.0

class OnlineEventClusterer:
    """Incremental online event clusterer with time decay and explainable decision logs."""
    def __init__(self, assign_threshold: float = 0.35):
        self.assign_threshold = assign_threshold
        self.active_events: Dict[str, Event] = {}
        self.event_features: Dict[str, Set[str]] = {}

    def get_all_events(self) -> List[Event]:
        # Return sorted by last_seen descending
        return sorted(self.active_events.values(), key=lambda e: e.last_seen, reverse=True)

    def get_event(self, event_id: str) -> Optional[Event]:
        return self.active_events.get(event_id)

    async def process_article(
        self,
        article_id: str,
        title: str,
        body: str,
        source_id: str,
        source_name: str,
        published_at: datetime
    ) -> Tuple[Event, bool]:
        """Clusters incoming unique article into an existing event or spawns a new event.
        Returns: (event, is_new_event)
        """
        article_features = extract_entities_and_keywords(f"{title} {body[:500]}")
        now = utc_now()

        best_event_id: Optional[str] = None
        best_score = 0.0

        # Compare against active events within the last 72 hours
        for ev_id, ev in list(self.active_events.items()):
            hours_diff = max(0.0, (now - ev.last_seen).total_seconds() / 3600.0)
            if hours_diff > 72.0:
                ev.status = "cooling" if hours_diff < 120.0 else "dead"
                continue

            # Feature overlap (entities & keywords)
            ev_feat = self.event_features.get(ev_id, set())
            overlap_score = jaccard_similarity(article_features, ev_feat)

            # Time proximity decay: exp(-0.05 * hours_diff)
            time_decay = math.exp(-0.05 * hours_diff)
            combined_score = overlap_score * time_decay

            if combined_score > best_score:
                best_score = combined_score
                best_event_id = ev_id

        # Decide: Attach or Create
        if best_event_id is not None and best_score >= self.assign_threshold:
            event = self.active_events[best_event_id]
            event.article_count += 1
            if source_name not in event.sources:
                event.sources.append(source_name)
                event.source_count = len(event.sources)
            event.last_seen = now
            event.status = "active"

            # Update feature set
            self.event_features[best_event_id].update(article_features)

            # Add timeline item
            timeline_item = TimelineItem(
                event_id=event.id,
                at=now,
                kind="update",
                article_id=article_id,
                headline=title,
                source_name=source_name
            )
            event.timeline.append(timeline_item)

            # Push live delta to clients
            delta = LiveDelta(
                event_id=event.id,
                event_title=event.title,
                delta_type="updated",
                article_count=event.article_count,
                source_count=event.source_count,
                latest_article_title=title,
                timestamp=now
            )
            await stream_client.publish_live_delta("event.updated", delta.model_dump())

            logger.info(f"Article '{title[:40]}...' attached to event '{event.title[:40]}' (score={best_score:.3f})")
            return event, False

        else:
            # Create brand new event
            new_id = f"ev_{uuid.uuid4().hex[:10]}"
            new_event = Event(
                id=new_id,
                title=title,
                status="emerging",
                first_seen=published_at or now,
                last_seen=now,
                article_count=1,
                source_count=1,
                sources=[source_name],
                entities=list(re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", title))[:5],
                timeline=[
                    TimelineItem(
                        event_id=new_id,
                        at=now,
                        kind="first_report",
                        article_id=article_id,
                        headline=title,
                        source_name=source_name
                    )
                ]
            )

            self.active_events[new_id] = new_event
            self.event_features[new_id] = article_features

            delta = LiveDelta(
                event_id=new_id,
                event_title=title,
                delta_type="created",
                article_count=1,
                source_count=1,
                latest_article_title=title,
                timestamp=now
            )
            await stream_client.publish_live_delta("event.updated", delta.model_dump())

            logger.info(f"Created new event '{title[:45]}...' (id={new_id})")
            return new_event, True

event_clusterer = OnlineEventClusterer()
