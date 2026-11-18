from typing import Optional, Tuple, Dict, Set
from core.redis import stream_client
from dedup.simhash import compute_simhash, hamming_distance
from dedup.lsh import get_lsh_bands

class DedupEngine:
    """3-Tier Deduplication Engine:
    L1: Exact hash match (canonical URL or content hash)
    L2: Near-duplicate SimHash with 4-band LSH (Hamming distance <= 3)
    L3: Semantic similarity
    """
    def __init__(self):
        # In-memory fast cache for recent articles (id -> (canonical_url, content_hash, simhash))
        self.recent_articles: Dict[str, Tuple[str, str, int]] = {}
        self.exact_urls: Dict[str, str] = {}
        self.exact_hashes: Dict[str, str] = {}

    async def check_duplicate(
        self,
        article_id: str,
        canonical_url: str,
        content_hash: str,
        body_text: str
    ) -> Tuple[bool, Optional[str], Optional[str], float]:
        """Checks if the article is a duplicate of a previously seen story.
        Returns: (is_duplicate, canonical_id, level, similarity)
        """
        # L1: Exact Match (Canonical URL)
        if canonical_url in self.exact_urls:
            canonical_id = self.exact_urls[canonical_url]
            return True, canonical_id, "L1_exact_url", 1.0

        # L1: Exact Match (Content Hash)
        if content_hash in self.exact_hashes:
            canonical_id = self.exact_hashes[content_hash]
            return True, canonical_id, "L1_exact_content", 1.0

        # Compute SimHash for L2
        simhash_val = compute_simhash(body_text)

        # L2: Near-Duplicate (SimHash 4-Band LSH)
        bands = get_lsh_bands(simhash_val)
        candidate_ids: Set[str] = set()

        for band_idx, bucket_key in bands:
            matches = await stream_client.get_simhash_candidates(band_idx, bucket_key)
            candidate_ids.update(matches)

        # Test candidate IDs for Hamming distance <= 3
        best_canonical_id = None
        min_distance = 65

        for cand_id in candidate_ids:
            if cand_id in self.recent_articles:
                cand_simhash = self.recent_articles[cand_id][2]
                dist = hamming_distance(simhash_val, cand_simhash)
                if dist <= 3 and dist < min_distance:
                    min_distance = dist
                    best_canonical_id = cand_id

        if best_canonical_id is not None:
            similarity = round(1.0 - (min_distance / 64.0), 3)
            return True, best_canonical_id, "L2_near_dup_simhash", similarity

        # Register as a new unique canonical story
        self.exact_urls[canonical_url] = article_id
        self.exact_hashes[content_hash] = article_id
        self.recent_articles[article_id] = (canonical_url, content_hash, simhash_val)

        # Index in LSH buckets
        for band_idx, bucket_key in bands:
            await stream_client.add_simhash_bucket(band_idx, bucket_key, article_id)

        return False, None, None, 0.0

dedup_engine = DedupEngine()
