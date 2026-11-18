from typing import List, Dict, Any
from core.models import Source

DEFAULT_SOURCES: List[Dict[str, Any]] = [
    # --- Breaking / World (Tier 1) ---
    {
        "id": "bbc_world",
        "name": "BBC World News",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "category": "World",
        "tier": 1,
        "reliability": 0.95,
        "poll_interval_s": 45,
    },
    {
        "id": "aljazeera_world",
        "name": "Al Jazeera English",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "category": "World",
        "tier": 1,
        "reliability": 0.92,
        "poll_interval_s": 45,
    },
    {
        "id": "guardian_world",
        "name": "The Guardian World",
        "url": "https://www.theguardian.com/world/rss",
        "category": "World",
        "tier": 1,
        "reliability": 0.93,
        "poll_interval_s": 45,
    },
    {
        "id": "npr_news",
        "name": "NPR News",
        "url": "https://feeds.npr.org/1001/rss.xml",
        "category": "World",
        "tier": 2,
        "reliability": 0.94,
        "poll_interval_s": 60,
    },
    {
        "id": "reuters_world",
        "name": "Reuters World (Google News)",
        "url": "https://news.google.com/rss/headlines/section/topic/WORLD",
        "category": "World",
        "tier": 1,
        "reliability": 0.92,
        "poll_interval_s": 45,
    },

    # --- India (National & Regional) ---
    {
        "id": "the_hindu",
        "name": "The Hindu",
        "url": "https://www.thehindu.com/news/national/feeder/default.rss",
        "category": "India",
        "tier": 1,
        "reliability": 0.94,
        "poll_interval_s": 45,
    },
    {
        "id": "indian_express",
        "name": "The Indian Express",
        "url": "https://indianexpress.com/section/india/feed/",
        "category": "India",
        "tier": 1,
        "reliability": 0.93,
        "poll_interval_s": 45,
    },
    {
        "id": "ndtv_top",
        "name": "NDTV Top Stories",
        "url": "https://feeds.feedburner.com/ndtvnews-top-stories",
        "category": "India",
        "tier": 2,
        "reliability": 0.90,
        "poll_interval_s": 60,
    },
    {
        "id": "hindustan_times",
        "name": "Hindustan Times",
        "url": "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "category": "India",
        "tier": 2,
        "reliability": 0.90,
        "poll_interval_s": 60,
    },
    {
        "id": "times_of_india",
        "name": "Times of India",
        "url": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "category": "India",
        "tier": 2,
        "reliability": 0.88,
        "poll_interval_s": 60,
    },

    # --- Tech & Computing ---
    {
        "id": "hackernews_front",
        "name": "Hacker News",
        "url": "https://news.ycombinator.com/rss",
        "category": "Tech",
        "tier": 1,
        "reliability": 0.90,
        "poll_interval_s": 45,
    },
    {
        "id": "reddit_tech",
        "name": "Reddit Technology",
        "url": "https://www.reddit.com/r/technology/.rss",
        "category": "Tech",
        "tier": 2,
        "reliability": 0.85,
        "poll_interval_s": 60,
    },
    {
        "id": "techcrunch",
        "name": "TechCrunch",
        "url": "https://techcrunch.com/feed/",
        "category": "Tech",
        "tier": 2,
        "reliability": 0.91,
        "poll_interval_s": 60,
    },
    {
        "id": "arstechnica",
        "name": "Ars Technica",
        "url": "https://feeds.arstechnica.com/arstechnica/index",
        "category": "Tech",
        "tier": 2,
        "reliability": 0.93,
        "poll_interval_s": 60,
    },

    # --- Science & AI ---
    {
        "id": "arxiv_ai",
        "name": "arXiv cs.AI / cs.LG",
        "url": "https://rss.arxiv.org/rss/cs.AI",
        "category": "Science",
        "tier": 3,
        "reliability": 0.98,
        "poll_interval_s": 120,
    },
    {
        "id": "sciencedaily",
        "name": "ScienceDaily Top",
        "url": "https://www.sciencedaily.com/rss/all.xml",
        "category": "Science",
        "tier": 3,
        "reliability": 0.92,
        "poll_interval_s": 120,
    },

    # --- Community & Curated ---
    {
        "id": "compile_artisan",
        "name": "Compile Artisan",
        "url": "https://compileartisan.dev/rss.xml",
        "category": "Mentor's Corner",
        "tier": 1,
        "reliability": 1.0,
        "poll_interval_s": 45,
    },
    {
        "id": "reddit_worldnews",
        "name": "Reddit World News",
        "url": "https://www.reddit.com/r/worldnews/.rss",
        "category": "World",
        "tier": 2,
        "reliability": 0.82,
        "poll_interval_s": 60,
    },
    {
        "id": "wikipedia_current",
        "name": "Wikipedia Current Events",
        "url": "https://en.wikipedia.org/w/api.php?action=feedrecentchanges&feedformat=atom",
        "category": "World",
        "tier": 3,
        "reliability": 0.95,
        "poll_interval_s": 120,
    }
]

class SourceRegistry:
    def __init__(self):
        self._sources: Dict[str, Source] = {}
        for s_data in DEFAULT_SOURCES:
            self._sources[s_data["id"]] = Source(**s_data)

    def get_all(self) -> List[Source]:
        return list(self._sources.values())

    def get(self, source_id: str) -> Optional[Source]:
        return self._sources.get(source_id)

    def update_status(self, source_id: str, status: str, failure: bool = False):
        src = self._sources.get(source_id)
        if src:
            src.status = status
            if failure:
                src.consecutive_failures += 1
            else:
                src.consecutive_failures = 0

registry = SourceRegistry()
