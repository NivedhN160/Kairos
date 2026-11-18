# KAIROS: Live Event Intelligence Engine

> **Tagline:** The world, clustered, as it happens.  
> *Personal-use website. $0 cost. Python. Live to the second.*

---

## 1. WHAT IT DOES

Ingests 200+ RSS/API sources continuously $\rightarrow$ normalizes $\rightarrow$ dedupes near-identical stories $\rightarrow$ clusters them into **EVENTS** $\rightarrow$ builds per-event timelines + a global entity graph $\rightarrow$ tracks how coverage evolves $\rightarrow$ ranks everything against **MY** interests $\rightarrow$ serves a live website and a daily briefing.

### Success Metrics *(put these on the site, they are the flex)*:
- **End-to-end latency:** publish time $\rightarrow$ visible on site (p50 / p95)
- **Dedupe precision/recall** on a hand-labelled set (200 pairs)
- **Cluster purity / NMI** on a hand-labelled set (50 events)
- **Entity resolution accuracy** on a labelled set
- **Throughput & lag:** articles/min throughput, stage lag, DLQ size

---

## 2. DESIGN PRINCIPLES

- **Event-sourced:** The raw article log is the source of truth. Everything else (clusters, graph, rankings) is a derived view that can be rebuilt by replay.
- **Independent stream workers:** Every stage is an independent worker consuming a stream, so it can be scaled, restarted, or replayed alone.
- **Idempotent stages:** Processing the same message twice yields the exact same state.
- **Versioned pipeline:** Each derived row stores `pipeline_version`, so a new model can be backfilled and compared with the old one (A/B).
- **Backpressure + DLQ everywhere:** Nothing is silently dropped.
- **Free tier only:** Local models for embeddings/NER, LLM API used sparingly (summaries, briefing) behind a quota-aware router with caching.

---

## 3. SYSTEM ARCHITECTURE (High Level)

```text
  [200+ sources: RSS, Atom, GDELT, HN, Reddit RSS, arXiv, Wikipedia Current Events, APIs]
                                   |
                                   v
  +---------------------------------------------------------------+
  | SCHEDULER / FETCHER POOL                                       |
  |  adaptive poll intervals, ETag/If-Modified-Since, per-host     |
  |  rate limits, circuit breakers, robots.txt, conditional GET    |
  +---------------------------------------------------------------+
                                   |  raw_articles (Redis Stream S1)
                                   v
  +---------------------------------------------------------------+
  | NORMALIZER: canonical URL, trafilatura body extract,           |
  | lang detect, timestamp repair, source-reliability tag          |
  +---------------------------------------------------------------+
                                   |  S2: normalized
                                   v
  +---------------------------------------------------------------+
  | DEDUP ENGINE                                                   |
  |  L1 exact (url hash / content hash)                            |
  |  L2 near-dup (SimHash 64-bit + LSH band buckets)               |
  |  L3 semantic dup (embedding cosine > threshold, same window)   |
  +---------------------------------------------------------------+
        | duplicate -> attach to canonical article (syndication count++)
        | unique    v   S3: unique_articles
  +---------------------------------------------------------------+
  | ENRICHER (parallel consumers on S3)                            |
  |  a) Embedder (local bge-small / MiniLM)                        |
  |  b) NER (spaCy) -> mention candidates                          |
  |  c) Keyphrases (YAKE/KeyBERT), topic tag, sentiment, stance    |
  +---------------------------------------------------------------+
                                   |  S4: enriched
                                   v
  +----------------------------+      +-------------------------------+
  | ENTITY RESOLUTION          |----->| ENTITY GRAPH (Postgres edges)  |
  | alias table, blocking,     |      | entity<->entity co-occurrence, |
  | embedding+context scoring, |      | entity<->event, weighted,      |
  | merge/split, Wikidata IDs  |      | time-decayed                   |
  +----------------------------+      +-------------------------------+
                                   |  S5: resolved
                                   v
  +---------------------------------------------------------------+
  | EVENT CLUSTERER (online, incremental)                          |
  |  candidate events from ANN (pgvector) + entity overlap + time  |
  |  decay -> assign / create / merge / split                      |
  |  nightly HDBSCAN repair pass fixes drift                       |
  +---------------------------------------------------------------+
                                   |  S6: event_updates
                                   v
  +---------------------------------------------------------------+
  | EVENT INTELLIGENCE                                             |
  |  timeline builder, coverage tracker (velocity, source mix,     |
  |  framing drift, first-reporter), event summary (LLM, cached)   |
  +---------------------------------------------------------------+
                                   |
                                   v
  +---------------------------------------------------------------+
  | RANKER: interest profile vector + features + recency + novelty |
  |  -> personal score per event; online learning from my clicks   |
  +---------------------------------------------------------------+
                                   |
            +----------------------+----------------------+
            v                                             v
  +--------------------------+               +---------------------------+
  | LIVE PUSH (Redis pubsub  |               | BRIEFING GENERATOR (cron) |
  |  -> FastAPI SSE/WS)      |               |  daily + on-demand digest |
  +--------------------------+               +---------------------------+
            \                                             /
             +------------------+------------------------+
                                v
                    WEB APP (live feed, event pages,
                    entity graph view, coverage charts,
                    SYSTEM page with pipeline health)
```

> **CROSS-CUTTING:** Raw event log (replay), DLQ, metrics (Prometheus $\rightarrow$ Grafana or built-in System page), structured logs, tracing IDs per article.

---

## 4. TECH STACK ($0)

| Layer | Technologies & Tools |
| :--- | :--- |
| **Language** | Python 3.12, `asyncio` |
| **API / Web** | FastAPI + SSE (`EventSource`), HTMX or React + Vite |
| **Broker** | Redis Streams (consumer groups, ack, pending-entries reclaim) |
| **Primary DB** | PostgreSQL + `pgvector` (articles, events, entities, graph edges) |
| **Cache / Hot State** | Redis (SimHash buckets, event centroids, rate limiters) |
| **Embeddings** | `sentence-transformers` `bge-small-en-v1.5` (local, CPU ok) |
| **NER** | spaCy `en_core_web_sm` / `en_core_web_md` (+ custom alias rules) |
| **Extraction** | `trafilatura`, `feedparser`, `httpx` (HTTP/2, async) |
| **Clustering** | NumPy, scikit-learn, HDBSCAN (nightly repair) |
| **LLM (sparingly)** | Gemini / Groq free tier via a quota-aware router + cache |
| **Orchestration** | Native process runner (`run_pipeline.py`) or `Procfile` (`honcho`) — no Docker required |
| **Observability** | `prometheus-client`, Grafana (optional), own `/system` page |
| **Hosting** | Home machine / free VM, Cloudflare Tunnel for access |

---

## 5. STAGE-BY-STAGE WORKFLOW (The Complexity Lives Here)

### S0. SOURCE REGISTRY

```sql
sources(id, url, type, tier, reliability, lang, poll_interval_s,
        etag, last_modified, last_fetch_at, avg_new_per_hour, status)
```

- **Tiers:** Tier 1 = breaking (poll 30–60 s), Tier 2 = normal (5 min), Tier 3 = slow (30 min).
- **Adaptive polling:** `interval = clamp(k / avg_new_per_hour, min, max)`. A source that posts often is polled more, a quiet one less.
- **Circuit breaker per source:** 5 consecutive failures $\rightarrow$ open (backoff 1m..1h), half-open probe, auto-recover. Status visible on the System page.

---

### S1. FETCHER

- Priority queue (min-heap on `next_fetch_at`) feeding an async worker pool.
- Per-host token bucket (politeness) + global concurrency cap.
- Conditional GET (`ETag` / `If-Modified-Since`); `304 Not Modified` = cheap no-op.
- Parse feed $\rightarrow$ for each item emit `{source_id, guid, url, title, summary, published_at, fetched_at, raw_hash}` to stream `S1`.
- **Seen-guard:** Redis `SET` / Bloom filter on `(source_id, guid)` to skip repeats fast.
- Append every raw payload to the immutable raw log (for replay).

---

### S2. NORMALIZER

- Canonicalize URL (strip `utm_*`, `fbclid`, AMP, trailing slashes, resolve redirects).
- Extract main text (`trafilatura`); fall back to feed summary if blocked.
- Repair timestamps (future dates, missing tz $\rightarrow$ use `fetched_at`, flag it).
- Language detection; drop/route non-supported languages.
- Compute `content_hash`. Emit to `S3` candidate stream.

---

### S3. DEDUP (3 Levels, Cheapest First)

- **L1 Exact:** `content_hash` / `canonical_url` hit $\rightarrow$ mark duplicate, stop.
- **L2 Near-Dup:** 64-bit SimHash over word shingles; split into 4 bands of 16 bits; lookup candidates per band in Redis; Hamming distance $\le 3$ $\rightarrow$ near-duplicate (wire copies, reposts).
- **L3 Semantic:** After embedding, cosine similarity $> 0.92$ against last 48h of articles (`pgvector` ANN) $\rightarrow$ same-story-different-words.
- **Handling:** Duplicates are **NOT** discarded: they attach to the canonical article and increment `syndication_count` and `sources_seen` (this acts as a coverage signal).
- Track precision/recall against the labelled set; thresholds are configurable.

---

### S4. ENRICHMENT (Fan-out, Parallel Consumer Groups)

- **Embed:** Title + lead paragraph $\rightarrow$ 384-d vector.
- **NER:** `PERSON` / `ORG` / `GPE` / `EVENT` / `PRODUCT` mentions with character spans.
- **Keyphrases & Metadata:** YAKE/KeyBERT keyphrases, topic (zero-shot or classifier), sentiment, stance.
- **Join Barrier:** Each enricher writes its own columns; a join barrier emits to `S5` when all required enrichers have finished (or a timeout fires with partial data).

---

### S5. ENTITY RESOLUTION

**Goal:** `"Modi"`, `"PM Modi"`, `"Narendra Modi"`, `"नरेंद्र मोदी"` $\rightarrow$ unified into one entity.

1. **Normalize mention:** Case, titles, punctuation, transliteration.
2. **Blocking:** Candidate entities retrieved via alias table + trigram index.
3. **Scoring:** Name similarity + type match + context embedding similarity + co-occurring-entity overlap + recency prior.
4. **Decision:**
   - `score > hi` $\rightarrow$ link.
   - `lo < score < hi` $\rightarrow$ queue for LLM tie-break (cached).
   - `score < lo` $\rightarrow$ create new entity.
5. **Maintenance jobs:** Merge duplicates, split overloaded entities, Wikidata/Wikipedia ID linking for canonical identity.
6. **Graph edges with time decay:**
   $$\text{weight} = \sum \exp(-\lambda \cdot \text{age\_hours})$$

---

### S6. EVENT CLUSTERING (Online)

**Each event retains:** Centroid embedding, entity set, `first_seen`, `last_seen`, `article_count`, `source_count`, status (`emerging` / `active` / `cooling` / `dead`).

**For each new article:**
1. **Candidates:** Top-$k$ events by ANN on centroid within a 72-hour window.
2. **Score calculation:**
   $$\text{score} = a \cdot \text{cosine} + b \cdot \text{entity\_jaccard} + c \cdot \text{title\_keyphrase\_overlap} + d \cdot \text{time\_proximity\_decay}$$
3. **Assignment:**
   - `score >= T_assign` $\rightarrow$ attach; update centroid (running mean) + entity set.
   - `score < T_new` $\rightarrow$ create new event (`status = emerging`).
   - `in between` $\rightarrow$ hold in a short "pending pool" for $N$ minutes, re-evaluate as neighbours arrive.
4. **Merge check:** Two events whose centroids + entities converge $\rightarrow$ merge, keep a redirect (`event_merges` table) so old URLs still resolve.
5. **Split check:** Event whose internal cohesion drops (bimodal) $\rightarrow$ split.
6. **Nightly repair:** HDBSCAN over the last 7 days, compare with online assignments, fix mismatches, log drift metrics.
- *All decisions are logged to `cluster_decisions` (replayable + explainable: "why was this article put in this event?").*

---

### S7. EVENT INTELLIGENCE

- **Timeline:** Chronological milestones per event (first report, official statement, correction, escalation). Detected via novelty of new article content vs. the event so far (new entities / numbers / quotes).
- **Coverage Tracker (snapshotted every 5 min per active event):**
  - Article velocity, source count, source-tier mix, geographic spread.
  - Sentiment drift, framing terms drift.
  - First-reporter source, lag between first report and mainstream pickup.
- **Summary:** LLM generates and caches a short summary only when an event crosses a size/velocity threshold or materially changes (hash of timeline).
- **Lifecycle:** `emerging` $\rightarrow$ `active` $\rightarrow$ `cooling` $\rightarrow$ `dead` (based on velocity decay).

---

### S8. PERSONAL RANKER

**Scoring formula:**
```text
score(event, me) =
    w1 * interest_sim(event.centroid, profile_vector)
  + w2 * entity_affinity(event.entities, my_followed_entities)
  + w3 * importance(velocity, source_count, tier_mix)
  + w4 * recency_decay(last_update)
  + w5 * novelty(event vs. what I already saw)
  - w6 * repetition_penalty(same story shown before)
  + w7 * serendipity (small exploration bonus)
```

- **Profile vector:** Exponential moving average of embeddings of events I opened / saved / dismissed (negative signal for dismiss).
- **Online weight learning:** Logistic regression or contextual bandit trained from click / dwell / dismiss. Keep an offline evaluator (NDCG on logged feedback).
- **Site controls:** Follow/mute entity, topic sliders, "why am I seeing this" explanation card.

---

### S9. LIVE PUSH

- Every change to an event publishes a compact delta on Redis pubsub channels `event.updated` and `feed.top`.
- FastAPI SSE endpoint `/live` streams deltas; the client patches the DOM.
- **Reconnect-safe:** Client sends `Last-Event-ID`; server replays missed deltas from a Redis stream window.
- **Latency Budget (Target p95 < 5 s from publish to screen on Tier-1 sources):**
  - Fetch + Parse: `1.0s`
  - Normalize: `0.5s`
  - Dedup: `0.3s`
  - Enrich: `1.5s`
  - Resolve + Cluster: `0.7s`
  - Rank + Push: `0.3s`
  - **Total budget: < 4.3s – 5.0s**
- Every article carries a `trace_id` and per-stage timestamps, so the System page displays a live waterfall for any article.

---

### S10. BRIEFING GENERATOR (Cron)

- **Schedule:** 07:00 IST daily + on-demand.
- **Generation:** Take top-$N$ events by personal score, group by topic, pick per event a headline, 3-line summary, "what changed since yesterday", coverage note (e.g., *"only 2 outlets so far"*), key entities.
- **Delivery:** Serves as a web page; optional email / Telegram bot delivery.
- **Diversity pass:** Maximal Marginal Relevance (MMR) so the briefing isn't 10 versions of one story.

---

### S11. BACKFILL / REPLAY

- Raw log is append-only and offset-indexed.
- **CLI replay:** `replay --from <ts> --stage <name> --pipeline-version vN` re-runs a stage over history into shadow tables, then compares $v(N-1)$ vs $vN$ (cluster purity, dedup F1). Promote with an atomic pointer swap.
- **Historical backfill:** GDELT / archived feeds into the same pipeline at controlled speed, so cold start has history.
- **Crash recovery:** Consumers use consumer groups; `XAUTOCLAIM` reclaims pending messages of dead workers; all stages are idempotent.

---

## 6. DATA MODEL (PostgreSQL)

```sql
sources(id, url, type, tier, reliability, poll_interval_s, status, ...)

raw_log(offset, source_id, payload, fetched_at)          -- immutable

articles(id, source_id, canonical_url, title, body, lang, published_at,
         fetched_at, content_hash, simhash, embedding vector(384),
         canonical_article_id NULL, pipeline_version, trace_id)

article_dupes(article_id, canonical_id, level, similarity)

mentions(id, article_id, text, type, span, entity_id NULL, confidence)

entities(id, canonical_name, type, wikidata_id NULL, embedding, merged_into NULL)

entity_aliases(entity_id, alias, source)

entity_edges(a_id, b_id, weight, last_seen)               -- graph

events(id, title, centroid vector(384), status, first_seen, last_seen,
       article_count, source_count, summary, summary_hash, pipeline_version)

event_articles(event_id, article_id, score, role)         -- role: first/core/followup

event_entities(event_id, entity_id, weight)

event_merges(old_id, new_id, at)

cluster_decisions(article_id, chosen_event_id, candidates jsonb, features jsonb, at)

timeline_items(event_id, at, kind, article_id, text)

coverage_snapshots(event_id, at, velocity, sources, tier_mix, sentiment, framing jsonb)

user_profile(id, vector, followed_entities, muted, weights jsonb)

feedback(id, event_id, action, dwell_ms, at)              -- open/save/dismiss

briefings(id, date, content jsonb, generated_at)

pipeline_metrics(stage, ts, lag_ms, throughput, errors)

dlq(id, stage, payload, error, retries, at)
```

### Indexes
- **pgvector HNSW:** on `articles.embedding` and `events.centroid`
- **Trigram GIN:** on `entity_aliases.alias`
- **B-tree:** on `(published_at)`, `(last_seen)`

---

## 7. REDIS LAYOUT

### Streams
- `s1.raw`
- `s2.normalized`
- `s3.unique`
- `s4.enriched`
- `s5.resolved`
- `s6.event_updates`
- `s9.live_deltas`
- `dlq.*`

### Groups
- One consumer group per stage; $N$ workers per group.

### Keys
| Key Pattern | Type / Value | Purpose |
| :--- | :--- | :--- |
| `simhash:band:{0..3}:{bits}` | Set of article IDs | LSH bucket for fast near-dup candidates |
| `event:centroid:{id}` | Cached Vector | Hot centroid cache for low-latency lookups |
| `ratelimit:host:{host}` | Token bucket | Per-host crawl rate limiter |
| `seen:{source_id}` | Redis Set / Bloom filter | GUID deduplication filter |
| `quota:{provider}` | Counters | LLM free-tier usage & quota counters |

---

## 8. WEBSITE (Pages)

| Route | View & Capabilities |
| :--- | :--- |
| `/` | **Live feed:** Ranked events, auto-updating via SSE, pulse badge when an event grows, "why am I seeing this" card. |
| `/event/:id` | **Event detail:** Timeline, all articles grouped by source tier, coverage chart (velocity, source mix, framing drift), entity chips, summary. |
| `/entities` | **Entity Graph:** Interactive entity graph (force-directed layout), click node $\rightarrow$ linked events. |
| `/briefing` | **Briefing:** Today's briefing + historical archive. |
| `/explore` | **Search & Filters:** Search (keyword + semantic), filter by entity/topic/time. |
| `/system` | **THE FLEX PAGE:** Live pipeline diagram with per-stage lag, queue depth, throughput, DLQ count, source health grid (200 dots), per-article trace waterfall, dedup/cluster accuracy cards, replay/backfill controls. |
| `/settings` | **Preferences:** Followed/muted entities, topic weights, source weights. |

---

## 9. REPO STRUCTURE

```text
kairos/
  run_pipeline.py
  Procfile
  WORKFLOW.md
  core/        config.py  models.py  db.py  redis.py  tracing.py  versions.py
  scheduler/   registry.py  adaptive.py  heap.py
  fetcher/     worker.py  feeds.py  politeness.py  breaker.py
  normalizer/  worker.py  canonical.py  extract.py
  dedup/       worker.py  simhash.py  lsh.py  semantic.py
  enrich/      embed.py  ner.py  keyphrase.py  sentiment.py  barrier.py
  entities/    resolver.py  blocking.py  scoring.py  maintenance.py  graph.py
  events/      clusterer.py  pending_pool.py  merge_split.py  repair_job.py
  intel/       timeline.py  coverage.py  summarizer.py  lifecycle.py
  ranker/      profile.py  features.py  scorer.py  learner.py
  briefing/    builder.py  mmr.py  cron.py
  replay/      cli.py  shadow.py  compare.py  backfill_gdelt.py
  api/         main.py  sse.py  routes_*.py
  web/         (HTMX templates or React app)
  eval/        labelled_dupes.jsonl  labelled_events.jsonl  run_eval.py
  ops/         metrics.py  dlq_reprocess.py  chaos.py
```

---

## 10. BUILD ORDER (Each Phase Ends With a Demo)

- **`P1` Skeleton:** Local or cloud DB setup (PostgreSQL + pgvector, Redis), process runner (`run_pipeline.py`), source registry, fetcher for 20 feeds $\rightarrow$ `raw_log` $\rightarrow$ `articles` table.  
  *Demo: rows streaming in.*
- **`P2` Normalizer + Exact/SimHash Dedup:** Pipeline stages S2 and S3 (L1 & L2).  
  *Demo: duplicate counter climbing live.*
- **`P3` Embeddings + Semantic Dedup:** Stage S3 (L3) + labelled-set evaluation script.
- **`P4` NER + Entity Resolution + Entity Graph:** Stages S4 (NER) and S5.  
  *Demo: graph view of today's news.*
- **`P5` Online Event Clustering + Decision Log + Pending Pool:** Stage S6.  
  *Demo: watch articles land in events with live explanations.*
- **`P6` Live Push (SSE) + Live Feed UI:** Stage S9 + frontend `/` feed.  
  *Demo: event updates reflecting on screen in seconds.*
- **`P7` Timeline + Coverage Tracker + Event Page Charts:** Stage S7 + `/event/:id`.
- **`P8` Ranker + Feedback Loop + Profile:** Stage S8; "why am I seeing this" explanation cards.
- **`P9` Briefing Generator + Cron + MMR Diversity:** Stage S10.
- **`P10` System Observability:** `/system` page, Prometheus metrics, trace waterfall, DLQ + `XAUTOCLAIM` recovery.
- **`P11` Replay/Backfill + Versioned Pipeline:** A/B model compare + nightly HDBSCAN repair pass.
- **`P12` Scale & Resilience:** Scale to 200+ sources, adaptive polling, chaos tests (kill workers, block a source, flood a stream) with automated recovery demo.

---

## 11. FAILURE MODES TO HANDLE (and Demo)

- **Source down / slow / changes format:** Breaker opens, health grid dot turns red.
- **Worker crash mid-message:** Message reclaimed via `XAUTOCLAIM`, zero loss, no duplicate side-effects.
- **Redis / Postgres restart:** Consumers reconnect automatically; streams resume processing from last uncommitted offset.
- **Ingestion burst (breaking news floods 500 articles/min):** Stream backpressure kicks in, priority lane for Tier-1 sources, lag visibly reported on `/system`.
- **LLM quota exhausted:** Fallback provider triggered $\rightarrow$ gracefully falls back to local extractive summary.
- **Clusters drift or wrongly merge:** Corrected by nightly HDBSCAN repair pass + manual merge/split interface on UI.
- **Clock skew / bad timestamps:** Automatically repaired and flagged with `fetched_at` fallback.

---

## 12. FIRST SOURCES (All Free)

BBC World, Al Jazeera, The Hindu, Indian Express, NDTV, Hindustan Times, Times of India, Reuters-via-Google-News RSS, Guardian, NPR, HN front page, r/worldnews + r/technology RSS, arXiv cs.AI/cs.LG, Wikipedia Current Events portal, GDELT 2.0 (15-min updates + backfill), GitHub trending, company newsrooms, government PIB/RBI/SEBI feeds.

**Expansion plan:** Grow to 200+ sources categorized by:
- World
- India
- Tech
- Business
- Science
- Sports
- Policy
- Regional languages
