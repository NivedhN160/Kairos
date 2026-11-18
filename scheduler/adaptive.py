def calculate_adaptive_interval(avg_new_per_hour: float, tier: int = 2) -> int:
    """Calculates adaptive polling interval in seconds.
    Formula: interval = clamp(k / avg_new_per_hour, min, max)
    Breaking tier-1 feeds poll faster; slow academic feeds poll slower.
    """
    if tier == 1:
        min_interval, max_interval, k = 20, 90, 60.0
    elif tier == 3:
        min_interval, max_interval, k = 120, 1800, 3600.0
    else:
        min_interval, max_interval, k = 45, 600, 300.0

    if avg_new_per_hour <= 0:
        return max_interval

    computed = k / avg_new_per_hour
    return int(max(min_interval, min(max_interval, computed)))
