# ⚡ KAIROS: Live Event Intelligence Engine

> *The world, clustered, as it happens.*  
> Personal-use real-time news intelligence platform. **$0 cost.** Built with Python, Redis Streams, and PostgreSQL + pgvector.

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![Database](https://img.shields.io/badge/PostgreSQL-pgvector-336791.svg)](https://github.com/pgvector/pgvector)
[![Broker](https://img.shields.io/badge/Redis-Streams-DC382D.svg)](https://redis.io/)
[![API](https://img.shields.io/badge/FastAPI-SSE-009688.svg)](https://fastapi.tiangolo.com/)
[![Runner](https://img.shields.io/badge/Orchestrator-Native%20Runner%20%2F%20Procfile-success.svg)](https://github.com/nickstenning/honcho)

---

## 📌 Overview

**Kairos** is an autonomous, event-sourced intelligence engine designed to turn high-velocity global news feeds into structured, actionable understanding in real time. 

Instead of overwhelming you with hundreds of raw RSS entries and duplicate wire articles, Kairos:
1. **Ingests** 200+ global RSS, Atom, GDELT, and API sources continuously.
2. **Normalizes & Cleans** article text, metadata, and timestamps.
3. **Deduplicates** stories through a 3-tier cascade (Exact Hash $\rightarrow$ SimHash LSH $\rightarrow$ Semantic Vector Cosine).
4. **Extracts & Resolves Entities** into a unified, time-decaying global co-occurrence graph.
5. **Clusters Articles into Events Online** using incremental centroid vector search, merge/split guards, and nightly drift repairs.
6. **Tracks Coverage Evolution** (reporting velocity, source-tier distribution, framing drift, first-reporter tracking).
7. **Ranks & Personalizes** feeds against an evolving interest vector via online feedback learning.
8. **Pushes Updates Live** directly to a browser interface via Server-Sent Events (SSE) with $< 5\text{s}$ p95 publish-to-screen latency.

For the exhaustive engineering specification, data models, and formulas, see [**Workflow.md**](file:///e:/mvp/Kairos/Workflow.md).

---

## 🏗️ Architecture & Pipeline Flow

The entire system is decoupled into independent, stream-driven workers communicating via Redis Streams and persisting into PostgreSQL:

```text
  [200+ sources: RSS, Atom, GDELT, HN, Reddit, arXiv, Wikipedia]
                                |
                                v
  [S0/S1] Scheduler & Adaptive Fetcher (ETag/304, Circuit Breaker)
                                |  (s1.raw)
                                v
  [S2] Normalizer (Trafilatura, Canonical URL, Content Hash)
                                |  (s2.normalized)
                                v
  [S3] 3-Tier Deduplication (Exact -> SimHash 64-bit -> Semantic ANN)
                                |  (s3.unique)
                                v
  [S4] Enrichment Workers (Local bge-small Embedder, spaCy NER, Keyphrases)
                                |  (s4.enriched)
                                v
  [S5] Entity Resolution & Dynamic Knowledge Graph (PostgreSQL edges)
                                |  (s5.resolved)
                                v
  [S6] Incremental Event Clusterer (Centroid ANN + Entity Overlap + Time Decay)
                                |  (s6.event_updates)
                                v
  [S7] Event Intelligence (Milestone Timeline, Velocity & Framing Drift)
                                |
                                v
  [S8] Personal Interest Ranker (Bandit / Online Feedback Learning)
                                |
                +---------------+---------------+
                |                               |
                v                               v
    [S9] Live SSE Push (FastAPI)     [S10] Daily Briefing (Cron + MMR)
                |                               |
                +---------------+---------------+
                                |
                                v
               [Web App & /system Health Console]
```

---

## ⚙️ Core Design Principles

- **Event-Sourced Source of Truth:** The raw article log is immutable. Clusters, graphs, and rankings are derived views that can be wiped and backfilled at any time.
- **Strictly Idempotent Workers:** Processing any message multiple times results in the identical system state.
- **Zero Silent Drops:** Backpressure and Dead Letter Queues (`DLQ`) are configured across all streams.
- **Zero Cost ($0):** Utilizes local open-source models (`bge-small-en-v1.5`, `spaCy`) and free-tier LLMs (Gemini / Groq) gated behind quota-conscious caching routers.
- **No Docker Required:** Runs directly on native Python, connecting to either local services or free managed cloud databases (Neon, Supabase, Upstash) with a single-command process runner (`run_pipeline.py` or `Procfile`).
- **A/B Backfill & Replay:** Every derived record stores `pipeline_version`, enabling shadow runs and side-by-side metric comparisons before promoting algorithm updates.

---

## 🛠️ Tech Stack

| Component | Choice | Rationale |
| :--- | :--- | :--- |
| **Language** | Python 3.12 (`asyncio`) | High-concurrency I/O and broad NLP/ML ecosystem |
| **Stream Broker** | Redis Streams | Consumer groups, acknowledgments, pending message reclaim (`XAUTOCLAIM`) |
| **Primary Storage** | PostgreSQL 16 + `pgvector` | Relational consistency, HNSW vector indexing, graph tables |
| **Cache & LSH** | Redis | SimHash band buckets, hot centroid caches, token-bucket rate limits |
| **Text & Extraction** | `trafilatura`, `feedparser`, `httpx` | Fast, robust HTML content stripping and HTTP/2 requests |
| **Embeddings** | `bge-small-en-v1.5` (sentence-transformers) | Compact 384-d vectors, high quality, runs efficiently on CPU |
| **NER & Resolution** | spaCy (`en_core_web_sm`/`md`) | Fast entity span extraction with custom alias resolution rules |
| **Clustering** | NumPy, scikit-learn, HDBSCAN | Online nearest-neighbour clustering with nightly HDBSCAN repair pass |
| **Web & Real-Time** | FastAPI + SSE (`EventSource`) | Lightweight async API with instantaneous client DOM patching |
| **Process Orchestration** | Native Python runner (`run_pipeline.py`) or `Procfile` (`honcho`) | Multi-process supervisor with graceful shutdown — zero containers needed |
| **Hosting** | Local / Free VM + Cloudflare Tunnel | $0 hosting cost with encrypted public access |

---

## 📊 Pipeline Stages At a Glance

| Stage | Name | Key Functionality |
| :---: | :--- | :--- |
| **S0** | **Source Registry** | Adaptive poll intervals, source tiering, automated circuit breakers. |
| **S1** | **Fetcher** | Min-heap priority fetch pool, per-host token bucket politeness, `ETag` conditional GETs. |
| **S2** | **Normalizer** | URL parameter stripping, trafilatura text extraction, timestamp repair, `content_hash`. |
| **S3** | **Dedup Engine** | L1 exact content match, L2 64-bit SimHash LSH, L3 cosine semantic match ($> 0.92$). |
| **S4** | **Enricher** | Parallel consumer groups for 384-d vectors, spaCy NER, and keyphrase extraction. |
| **S5** | **Entity Resolution** | Name normalization, alias blocking, disambiguation scoring, and time-decay graph updates. |
| **S6** | **Event Clustering** | Online pgvector ANN centroid assignment, pending pools, convergence merge/split checks. |
| **S7** | **Event Intelligence** | Milestones timeline, reporting velocity, framing drift, first-reporter detection. |
| **S8** | **Personal Ranker** | Contextual scoring based on interest profile, entity affinity, novelty, and recency. |
| **S9** | **Live Push** | Redis pub/sub to FastAPI SSE streaming deltas with a sub-5s end-to-end latency budget. |
| **S10** | **Briefing Generator** | Scheduled 07:00 IST digest with MMR (Maximal Marginal Relevance) diversity filtering. |
| **S11** | **Backfill & Replay** | Offset-based re-execution of historical pipelines with atomic pointer swapping. |

---

## 📁 Repository Structure

```text
kairos/
├── run_pipeline.py             # Single-command native async multi-process worker manager
├── Procfile                    # Process definitions for honcho / foreman / PM2
├── Workflow.md                 # Full end-to-end technical specification
├── README.md                   # Project overview & documentation
├── core/                       # Shared configurations, DB, Redis, versions, tracing
├── scheduler/                  # Source registry, adaptive interval calculator, priority heap
├── fetcher/                    # Async fetching worker, politeness limits, circuit breaker
├── normalizer/                 # URL canonicalizer, body extractor, timestamp repair
├── dedup/                      # SimHash, LSH indexing, exact & semantic deduplication
├── enrich/                     # Local embeddings, spaCy NER, keyphrases, join barrier
├── entities/                   # Alias blocking, entity resolver, dynamic knowledge graph
├── events/                     # Online incremental clusterer, pending pool, merge/split
├── intel/                      # Event milestones, coverage tracker, LLM summarizer
├── ranker/                     # Profile vector updater, feature extractor, online learner
├── briefing/                   # Daily briefing builder, MMR diversity filter, cron task
├── replay/                     # Historical replayer CLI, shadow comparisons, GDELT backfill
├── api/                        # FastAPI app, SSE streams, endpoints
├── web/                        # Frontend UI (Live feed, entity graph, system dashboard)
├── eval/                       # Hand-labelled evaluation datasets & benchmarking scripts
└── ops/                        # Prometheus metrics, DLQ reprocessors, chaos injection
```

---

## 🚦 System Web Interface

The Kairos web frontend provides real-time access to the processed intelligence:

- **`/` (Live Feed):** Dynamic feed updating via SSE as events emerge, showing pulse badges and novelty indicators.
- **`/event/:id` (Event Detail):** Chronological milestone timeline, source-tier breakdown, velocity charts, and bias/framing shifts.
- **`/entities` (Entity Graph):** Interactive force-directed knowledge graph displaying real-time entity co-occurrences.
- **`/briefing`:** Clean morning digest highlighting critical developments since yesterday.
- **`/explore`:** Hybrid keyword + semantic vector search across historical events.
- **`/system` (The Flex Page):** Live end-to-end pipeline dashboard displaying per-stage lag waterfall, consumer queue depths, DLQ sizes, and a 200-node source health grid.

---

## 🚀 Getting Started (No Docker Required)

### 1. Prerequisites
- [Python 3.12+](https://www.python.org/)
- **Database & Broker Options:**
  - **Option A (Free Cloud Services — Zero Local Installation, \$0):**
    - PostgreSQL + pgvector: [Neon](https://neon.tech/) (free serverless Postgres with pgvector pre-installed) or [Supabase](https://supabase.com/)
    - Redis: [Upstash](https://upstash.com/) (free serverless Redis with stream support) or [Redis Cloud](https://redis.io/cloud/)
  - **Option B (Native Local Services):**
    - Local PostgreSQL with the `pgvector` extension installed
    - Local Redis (native or Windows port / Memurai)

### 2. Environment Configuration
Create a `.env` file in the project root:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/kairos
REDIS_URL=redis://localhost:6379/0

# Optional free-tier keys for LLM summaries & briefings
GEMINI_API_KEY=your_gemini_key_here
GROQ_API_KEY=your_groq_key_here
```

### 3. Initialize Database
Install dependencies and run the initial migration:
```bash
pip install -r requirements.txt
python -m core.db init
```

### 4. Run the Pipeline
Start all pipeline stages using the native process runner or `honcho`:

**Using the built-in runner:**
```bash
python run_pipeline.py
```

**Or using `honcho` (Procfile):**
```bash
pip install honcho
honcho start
```

### 5. Launch the Web Interface
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
Navigate to `http://localhost:8000` to view the live event stream, or `http://localhost:8000/system` for live pipeline telemetry.

---

## ☁️ Deploying on Render ($0 Free Tier)

Kairos is architected to deploy directly onto Render as a **single Web Service** using native Python (no Docker):

1. **Connect Repository:** Link your GitHub repo to [Render](https://render.com/).
2. **Create Web Service:**
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn api.main:app --host 0.0.0.0 --port $PORT`
   - **Plan:** `Free`
   - **Health Check Path:** `/health`
3. **Environment Variables:**
   - `RUN_EMBEDDED_WORKERS`: `true` *(Runs background crawler & clustering loops inside the web process, saving money on separate background workers)*
   - `DATABASE_URL`: *(Optional)* Your [Neon](https://neon.tech/) connection string (`postgresql+asyncpg://...`)
   - `REDIS_URL`: *(Optional)* Your [Upstash](https://upstash.com/) connection string (`rediss://...`)
   *(If omitted, Kairos automatically runs with its built-in in-memory broker and SQLite)*
4. **Blueprint Support:** You can also deploy via `render.yaml` by choosing **New > Blueprint** in Render!

---

## 🗺️ Build Roadmap

- [x] **P1 Skeleton:** Local/Cloud DB setup (PostgreSQL + pgvector, Redis), process runner (`run_pipeline.py`), source registry, async fetcher, immutable raw log.
- [ ] **P2 Deduplication:** Normalizer + exact hash & 64-bit SimHash LSH deduplication.
- [ ] **P3 Semantic Layer:** Local `bge-small` embeddings + semantic deduplication with labelled-set evaluation.
- [ ] **P4 Knowledge Graph:** spaCy NER + entity resolution + PostgreSQL time-decayed co-occurrence edges.
- [ ] **P5 Incremental Clustering:** Online pgvector ANN clusterer, pending pool, decision logging.
- [ ] **P6 Real-Time Delivery:** FastAPI SSE stream + reactive live feed UI ($< 5\text{s}$ latency).
- [ ] **P7 Intelligence & Timelines:** Event milestones, coverage velocity tracker, and framing drift charts.
- [ ] **P8 Personalization:** User profile vector tracking and bandit-based ranking models.
- [ ] **P9 Briefings:** Daily cron summary generator with MMR diversity filtering.
- [ ] **P10 Observability:** `/system` dashboard, Prometheus metrics, and automated `XAUTOCLAIM` recovery.
- [ ] **P11 Replay & Backfill:** Versioned pipeline A/B shadow evaluator + nightly HDBSCAN repair.
- [ ] **P12 Scaling & Resilience:** 200+ live sources, adaptive crawling, and chaos recovery validation.

---

## 📖 Complete Documentation

For the in-depth architectural breakdown, mathematical formulations, database schemas, and failure mode specifications, refer to [**Workflow.md**](file:///e:/mvp/Kairos/Workflow.md).
