# KAIROS: Live Event Intelligence Engine
Tagline: the world, clustered, as it happens.
Personal-use website. $0 cost. Python. Live to the second.

======================================================================
1. WHAT IT DOES
======================================================================
Ingests 200+ RSS/API sources continuously -> normalizes -> dedupes near-identical
stories -> clusters them into EVENTS -> builds per-event timelines + a global
entity graph -> tracks how coverage evolves -> ranks everything against MY
interests -> serves a live website and a daily briefing.

Success metrics (put these on the site, they are the flex):
- end-to-end latency: publish time -> visible on site (p50 / p95)
- dedupe precision/recall on a hand-labelled set (200 pairs)
- cluster purity / NMI on a hand-labelled set (50 events)
- entity resolution accuracy on a labelled set
- articles/min throughput, stage lag, DLQ size

======================================================================
2. DESIGN PRINCIPLES
======================================================================
- Event-sourced: the raw article log is the source of truth. Everything else
  (clusters, graph, rankings) is a derived view that can be rebuilt by replay.
- Every stage is an independent worker consuming a stream, so it can be
  scaled, restarted or replayed alone.
- Idempotent stages: processing the same message twice yields the same state.
- Versioned pipeline: each derived row stores pipeline_version, so a new model
  can be backfilled and compared with the old one (A/B).
- Backpressure + DLQ everywhere. Nothing is silently dropped.
- Free tier only: local models for embeddings/NER, LLM API used sparingly
  (summaries, briefing) behind a quota-aware router with caching.

======================================================================
3. SYSTEM ARCHITECTURE (high level)
======================================================================

  [200+ sources: RSS, Atom, GDELT, HN, Reddit RSS, arXiv, Wikipedia Current Events, APIs]
                                   |
                                   v
  +---------------------------------------------------------------+
  | SCHEDULER / FETCHER POOL                                       |
  |  adaptive poll intervals, ETag/If-Modified-Since, per-host     |
  |  rate limits, circuit breakers, robots.txt, conditional GET    |
  +---------------------------------------------------------------+
                                   |  raw_articles  (Redis Stream S1)
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

  CROSS-CUTTING: raw event log (replay), DLQ, metrics (Prometheus ->
  Grafana or built-in System page), structured logs, tracing IDs per article.

======================================================================
4. TECH STACK ($0)
======================================================================
Language:        Python 3.12, asyncio
API/Web:         FastAPI + SSE (EventSource), HTMX or React + Vite
Broker:          Redis Streams (consumer groups, ack, pending-entries reclaim)
Primary DB:      PostgreSQL + pgvector (articles, events, entities, graph edges)
Cache/hot state: Redis (SimHash buckets, event centroids, rate limiters)
Embeddings:      sentence-transformers bge-small-en-v1.5 (local, CPU ok)
NER:             spaCy en_core_web_sm/md (+ custom alias rules)
Extraction:      trafilatura, feedparser, httpx (HTTP/2, async)
Clustering:      numpy, scikit-learn, hdbscan (nightly repair)
LLM (sparingly): Gemini / Groq free tier via a quota-aware router + cache
Orchestration:   Docker Compose (one command up)
Observability:   prometheus-client, Grafana (optional), own /system page
Hosting:         home machine / free VM, Cloudflare Tunnel for access

======================================================================
5. STAGE-BY-STAGE WORKFLOW (the complexity lives here)
======================================================================

--- S0. SOURCE REGISTRY ---------------------------------------------
sources(id, url, type, tier, reliability, lang, poll_interval_s,
        etag, last_modified, last_fetch_at, avg_new_per_hour, status)
- tier 1 = breaking (poll 30-60 s), tier 2 = normal (5 min), tier 3 = slow (30 min)
- Adaptive polling: interval = clamp(k / avg_new_per_hour, min, max).
  A source that posts often is polled more, a quiet one less.
- Circuit breaker per source: 5 consecutive failures -> open (backoff 1m..1h),
  half-open probe, auto-recover. Status visible on the System page.

--- S1. FETCHER ------------------------------------------------------
- Priority queue (min-heap on next_fetch_at) feeding an async worker pool.
- Per-host token bucket (politeness) + global concurrency cap.
- Conditional GET (ETag / If-Modified-Since); 304 = cheap no-op.
- Parse feed -> for each item emit {source_id, guid, url, title, summary,
  published_at, fetched_at, raw_hash} to stream S1.
- Seen-guard: Redis SET/Bloom filter on (source_id, guid) to skip repeats fast.
- Append every raw payload to the immutable raw log (for replay).

--- S2. NORMALIZER ---------------------------------------------------
- Canonicalize URL (strip utm_*, fbclid, AMP, trailing slash, resolve redirects).
- Extract main text (trafilatura); fall back to the feed summary if blocked.
- Repair timestamps (future dates, missing tz -> use fetched_at, flag it).
- Language detect; drop/route non-supported languages.
- Compute content_hash. Emit to S3 candidate stream.

--- S3. DEDUP (3 levels, cheapest first) -----------------------------
L1 exact:     content_hash / canonical_url hit -> mark duplicate, stop.
L2 near-dup:  64-bit SimHash over word shingles; split into 4 bands of 16 bits;
              lookup candidates per band in Redis; Hamming distance <= 3
              -> near-duplicate (wire copies, reposts).
L3 semantic:  after embedding, cosine > 0.92 against last 48h of articles
              (pgvector ANN) -> same-story-different-words.
- Duplicates are NOT discarded: they attach to the canonical article and
  increment syndication_count and sources_seen (this is a coverage signal).
- Track precision/recall against the labelled set; thresholds are config.

--- S4. ENRICHMENT (fan-out, parallel consumer groups) ----------------
- embed:     title + lead paragraph -> 384-d vector
- ner:       PERSON / ORG / GPE / EVENT / PRODUCT mentions with char spans
- keyphrases, topic (zero-shot or classifier), sentiment, stance
- Each enricher writes its own columns; a join barrier emits to S5 when all
  required enrichers have finished (or a timeout fires with partial data).

--- S5. ENTITY RESOLUTION --------------------------------------------
Goal: "Modi", "PM Modi", "Narendra Modi", "नरेंद्र मोदी" -> one entity.
1. Normalize mention (case, titles, punctuation, transliteration).
2. Blocking: candidate entities via alias table + trigram index.
3. Scoring: name similarity + type match + context embedding similarity +
   co-occurring-entity overlap + recency prior.
4. Decision: score > hi -> link; lo < score < hi -> queue for LLM tie-break
   (cached); score < lo -> create new entity.
5. Maintenance jobs: merge duplicates, split overloaded entities,
   Wikidata/Wikipedia ID linking for canonical identity.
6. Update graph edges with time decay:
   weight = sum(exp(-lambda * age_hours)).

--- S6. EVENT CLUSTERING (online) ------------------------------------
Each event keeps: centroid embedding, entity set, first_seen, last_seen,
article_count, source_count, status (emerging / active / cooling / dead).
For each new article:
1. Candidates = top-k events by ANN on centroid within a 72h window.
2. Score = a*cosine + b*entity_jaccard + c*title_keyphrase_overlap
           + d*time_proximity_decay.
3. score >= T_assign  -> attach; update centroid (running mean) + entity set.
   score <  T_new     -> create new event (status = emerging).
   in between         -> hold in a short "pending pool" for N minutes,
                         re-evaluate as neighbours arrive.
4. Merge check: two events whose centroids + entities converge -> merge,
   keep a redirect (event_merges table) so old URLs still work.
5. Split check: event whose internal cohesion drops (bimodal) -> split.
6. Nightly repair: HDBSCAN over the last 7 days, compare with online
   assignments, fix mismatches, log drift metrics.
- All decisions are logged to cluster_decisions (replayable + explainable:
  "why was this article put in this event?").

--- S7. EVENT INTELLIGENCE -------------------------------------------
- Timeline: chronological milestones per event (first report, official
  statement, correction, escalation). Detected via novelty of new article
  content vs. the event so far (new entities / numbers / quotes).
- Coverage tracker, snapshotted every 5 min per active event:
  article velocity, source count, source-tier mix, geographic spread,
  sentiment drift, framing terms drift, first-reporter source, lag between
  first report and mainstream pickup.
- Summary: LLM generates and caches a short summary only when an event
  crosses a size/velocity threshold or materially changes (hash of timeline).
- Lifecycle: emerging -> active -> cooling -> dead (based on velocity decay).

--- S8. PERSONAL RANKER ----------------------------------------------
score(event, me) =
    w1 * interest_sim(event.centroid, profile_vector)
  + w2 * entity_affinity(event.entities, my_followed_entities)
  + w3 * importance(velocity, source_count, tier_mix)
  + w4 * recency_decay(last_update)
  + w5 * novelty(event vs. what I already saw)
  - w6 * repetition_penalty(same story shown before)
  + w7 * serendipity (small exploration bonus)
- Profile vector = exponential moving average of embeddings of events I
  opened / saved / dismissed (negative signal for dismiss).
- Weights learned online (logistic regression or contextual bandit) from
  click/dwell/dismiss. Keep an offline evaluator (NDCG on logged feedback).
- Controls on the site: follow/mute entity, topic sliders, "why am I seeing this".

--- S9. LIVE PUSH ----------------------------------------------------
- Every change to an event publishes a compact delta on Redis pubsub
  channel "event.updated" and "feed.top".
- FastAPI SSE endpoint /live streams deltas; the client patches the DOM.
- Reconnect-safe: client sends Last-Event-ID; server replays missed deltas
  from a Redis stream window.
- Latency budget (target p95 < 5 s from publish to screen on tier-1 sources):
    fetch+parse 1.0s | normalize 0.5s | dedup 0.3s | enrich 1.5s
    | resolve+cluster 0.7s | rank+push 0.3s
- Every article carries a trace_id and per-stage timestamps, so the System
  page shows a live waterfall for any article.

--- S10. BRIEFING GENERATOR (cron) ------------------------------------
- 07:00 IST daily + on-demand: take top-N events by personal score, group by
  topic, pick per event a headline, 3-line summary, "what changed since
  yesterday", coverage note (e.g. "only 2 outlets so far"), key entities.
- Delivers as a web page; optional email/Telegram bot.
- Diversity pass (MMR) so the briefing isn't 10 versions of one story.

--- S11. BACKFILL / REPLAY -------------------------------------------
- Raw log is append-only and offset-indexed.
- `replay --from <ts> --stage <name> --pipeline-version vN` re-runs a stage
  over history into shadow tables, then compares v(N-1) vs vN (cluster
  purity, dedup F1). Promote with an atomic pointer swap.
- Historical backfill: GDELT / archived feeds into the same pipeline at
  controlled speed, so cold start has history.
- Crash recovery: consumers use consumer groups; XAUTOCLAIM reclaims
  pending messages of dead workers; stages are idempotent.

======================================================================
6. DATA MODEL (Postgres)
======================================================================
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

Indexes: pgvector HNSW on articles.embedding and events.centroid;
trigram GIN on entity_aliases.alias; btree on (published_at), (last_seen).

======================================================================
7. REDIS LAYOUT
======================================================================
Streams:  s1.raw  s2.normalized  s3.unique  s4.enriched  s5.resolved
          s6.event_updates  s9.live_deltas  dlq.*
Groups:   one consumer group per stage; N workers per group
Keys:     simhash:band:{0..3}:{bits}   -> set of article ids
          event:centroid:{id}          -> hot centroid cache
          ratelimit:host:{host}        -> token bucket
          seen:{source_id}             -> guid bloom/set
          quota:{provider}             -> LLM free-tier counters

======================================================================
8. WEBSITE (pages)
======================================================================
/            Live feed: ranked events, auto-updating via SSE, pulse badge
             when an event grows, "why am I seeing this".
/event/:id   Timeline, all articles grouped by source tier, coverage chart
             (velocity, source mix, framing drift), entity chips, summary.
/entities    Interactive entity graph (force layout), click -> events.
/briefing    Today's briefing + archive.
/explore     Search (keyword + semantic), filter by entity/topic/time.
/system      THE FLEX PAGE: live pipeline diagram with per-stage lag, queue
             depth, throughput, DLQ count, source health grid (200 dots),
             per-article trace waterfall, dedup/cluster accuracy cards,
             replay/backfill controls.
/settings    Followed/muted entities, topic weights, source weights.

======================================================================
9. REPO STRUCTURE
======================================================================
kairos/
  docker-compose.yml
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

======================================================================
10. BUILD ORDER (each phase ends with a demo)
======================================================================
P1  Skeleton: docker-compose (Postgres+pgvector, Redis), source registry,
    fetcher for 20 feeds -> raw_log -> articles table. Demo: rows streaming in.
P2  Normalizer + exact/SimHash dedup. Demo: dup counter climbing live.
P3  Embeddings + semantic dedup + labelled-set eval script.
P4  NER + entity resolution + entity graph. Demo: graph view of today.
P5  Online event clustering + decision log + pending pool.
    Demo: watch articles land in events with explanations.
P6  Live push (SSE) + live feed UI. Demo: event updates on screen in seconds.
P7  Timeline + coverage tracker + event page charts.
P8  Ranker + feedback loop + profile; "why am I seeing this".
P9  Briefing generator + cron + MMR diversity.
P10 /system page, metrics, trace waterfall, DLQ + XAUTOCLAIM recovery.
P11 Replay/backfill + versioned pipeline A/B compare + HDBSCAN repair.
P12 Scale to 200+ sources, adaptive polling, chaos tests (kill workers,
    block a source, flood a stream) with recovery demo.

======================================================================
11. FAILURE MODES TO HANDLE (and demo)
======================================================================
- Source down / slow / changes format -> breaker opens, health grid goes red.
- Worker crash mid-message -> message reclaimed, no loss, no duplicate effect.
- Redis/Postgres restart -> consumers reconnect, streams resume from offset.
- Burst (breaking news floods 500 articles/min) -> backpressure, priority lane
  for tier-1 sources, lag visible on /system.
- LLM quota exhausted -> fallback provider -> fallback to extractive summary.
- Clusters drift or wrongly merge -> nightly repair + manual merge/split UI.
- Clock skew / bad timestamps -> repaired and flagged.

======================================================================
12. FIRST SOURCES (all free)
======================================================================
BBC World, Al Jazeera, The Hindu, Indian Express, NDTV, Hindustan Times,
Times of India, Reuters-via-Google-News RSS, Guardian, NPR, HN front page,
r/worldnews + r/technology RSS, arXiv cs.AI/cs.LG, Wikipedia Current Events
portal, GDELT 2.0 (15-min updates + backfill), GitHub trending, company
newsrooms, government PIB/RBI/SEBI feeds. Grow to 200+ by category:
world, India, tech, business, science, sports, policy, regional languages.