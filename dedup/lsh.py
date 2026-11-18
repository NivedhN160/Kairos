from typing import List, Tuple

def get_lsh_bands(simhash: int) -> List[Tuple[int, int]]:
    """Splits a 64-bit SimHash into 4 bands of 16 bits each.
    Returns: List of (band_index, bucket_key)
    """
    bands = []
    for band_idx in range(4):
        shift = band_idx * 16
        bucket_key = (simhash >> shift) & 0xFFFF
        bands.append((band_idx, bucket_key))
    return bands
