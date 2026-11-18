import json
import logging
import asyncio
from typing import Optional, Dict, Any, List, Set
from collections import defaultdict, deque
import redis.asyncio as aioredis
from core.config import settings

logger = logging.getLogger("kairos.broker")

class InMemoryStreamBroker:
    """Zero-dependency in-memory stream and key-value broker fallback when Redis is not configured."""
    def __init__(self):
        self.streams: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))
        self.kv: Dict[str, Any] = {}
        self.sets: Dict[str, Set[str]] = defaultdict(set)
        self.subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def xadd(self, stream: str, data: Dict[str, Any]) -> str:
        async with self._lock:
            msg_id = f"{int(asyncio.get_event_loop().time() * 1000)}-{len(self.streams[stream])}"
            serialized = {k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v) for k, v in data.items()}
            self.streams[stream].append((msg_id, serialized))
            return msg_id

    async def sadd(self, key: str, *members: str) -> int:
        async with self._lock:
            initial_len = len(self.sets[key])
            for m in members:
                self.sets[key].add(str(m))
            return len(self.sets[key]) - initial_len

    async def sismember(self, key: str, member: str) -> bool:
        async with self._lock:
            return str(member) in self.sets[key]

    async def smembers(self, key: str) -> Set[str]:
        async with self._lock:
            return set(self.sets[key])

    async def set(self, key: str, value: Any, ex: Optional[int] = None):
        async with self._lock:
            self.kv[key] = value

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            return self.kv.get(key)

    async def publish(self, channel: str, message: str) -> int:
        async with self._lock:
            for q in list(self.subscribers):
                try:
                    q.put_nowait({"channel": channel, "data": message})
                except Exception:
                    pass
            return len(self.subscribers)

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self.subscribers:
            self.subscribers.remove(q)

class StreamClient:
    """Wrapper that seamlessly bridges real Redis Streams or the in-memory broker."""
    def __init__(self):
        self.real_client: Optional[aioredis.Redis] = None
        self.mem_broker: InMemoryStreamBroker = InMemoryStreamBroker()
        self.is_connected = False

    async def connect(self):
        if settings.REDIS_URL:
            try:
                self.real_client = aioredis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=3.0
                )
                await self.real_client.ping()
                self.is_connected = True
                logger.info("Connected to remote Redis successfully.")
            except Exception as e:
                logger.warning(f"Could not connect to Redis ({e}). Falling back to in-memory broker.")
                self.real_client = None
                self.is_connected = False
        else:
            logger.info("No REDIS_URL configured; using high-speed in-memory broker.")
            self.is_connected = False

    async def emit_stream(self, stream_name: str, payload: Dict[str, Any]) -> str:
        """Publishes an event to a stream."""
        if self.real_client:
            try:
                # Convert complex values to JSON strings with safe default=str
                flattened = {k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v) for k, v in payload.items()}
                return await self.real_client.xadd(stream_name, flattened)
            except Exception as e:
                logger.error(f"Redis xadd error: {e}")
        return await self.mem_broker.xadd(stream_name, payload)

    async def publish_live_delta(self, channel: str, message: Dict[str, Any]):
        """Publishes live updates for SSE subscribers."""
        serialized = json.dumps(message, default=str)
        if self.real_client:
            try:
                await self.real_client.publish(channel, serialized)
            except Exception as e:
                logger.error(f"Redis publish error: {e}")
        await self.mem_broker.publish(channel, serialized)

    async def check_seen(self, source_id: str, guid: str) -> bool:
        """Fast seen-guard to skip already fetched GUIDs."""
        key = f"seen:{source_id}"
        if self.real_client:
            try:
                return bool(await self.real_client.sismember(key, guid))
            except Exception:
                pass
        return await self.mem_broker.sismember(key, guid)

    async def mark_seen(self, source_id: str, guid: str):
        """Marks a GUID as processed in the seen filter."""
        key = f"seen:{source_id}"
        if self.real_client:
            try:
                await self.real_client.sadd(key, guid)
                return
            except Exception:
                pass
        await self.mem_broker.sadd(key, guid)

    async def add_simhash_bucket(self, band: int, bucket_key: int, article_id: str):
        """Stores article ID into SimHash LSH band bucket."""
        key = f"simhash:band:{band}:{bucket_key}"
        if self.real_client:
            try:
                await self.real_client.sadd(key, article_id)
                return
            except Exception:
                pass
        await self.mem_broker.sadd(key, article_id)

    async def get_simhash_candidates(self, band: int, bucket_key: int) -> Set[str]:
        """Retrieves candidate duplicate article IDs from LSH bucket."""
        key = f"simhash:band:{band}:{bucket_key}"
        if self.real_client:
            try:
                return set(await self.real_client.smembers(key))
            except Exception:
                pass
        return await self.mem_broker.smembers(key)

stream_client = StreamClient()
