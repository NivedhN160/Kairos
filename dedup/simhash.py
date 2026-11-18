import re
import hashlib
from typing import List, Iterable

def _hash64(token: str) -> int:
    """Computes a 64-bit integer hash from a token."""
    return int(hashlib.md5(token.encode("utf-8")).hexdigest()[:16], 16)

def tokenize_shingles(text: str, n: int = 3) -> List[str]:
    """Tokenizes text into word n-grams (shingles) for structural fingerprinting."""
    words = re.findall(r"\w+", text.lower())
    if len(words) < n:
        return words or [text.lower()]
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

def compute_simhash(text: str) -> int:
    """Computes a 64-bit SimHash fingerprint for text."""
    shingles = tokenize_shingles(text)
    if not shingles:
        return 0

    v = [0] * 64
    for shingle in shingles:
        h = _hash64(shingle)
        for i in range(64):
            bit = (h >> i) & 1
            if bit:
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)

    return fingerprint

def hamming_distance(h1: int, h2: int) -> int:
    """Computes the Hamming bit distance between two 64-bit integers."""
    x = (h1 ^ h2) & 0xFFFFFFFFFFFFFFFF
    return bin(x).count("1")
