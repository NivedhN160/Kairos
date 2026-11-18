import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "msclkid", "ref", "source", "ocid", "cmpid", "xtor"
}

def canonicalize_url(raw_url: str) -> str:
    """Strips marketing trackers, AMP prefixes, and trailing slashes from URLs."""
    if not raw_url:
        return ""
    try:
        parsed = urlparse(raw_url.strip())
        # Clean query parameters
        filtered_q = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() not in TRACKING_PARAMS]
        clean_query = urlencode(filtered_q)

        # Normalize path
        path = parsed.path.rstrip("/")
        # Remove AMP patterns if present in path
        path = re.sub(r"/amp/?$", "", path, flags=re.IGNORECASE)

        # Normalize scheme and host
        scheme = parsed.scheme.lower() or "https"
        netloc = parsed.netloc.lower()

        clean_url = urlunparse((scheme, netloc, path, parsed.params, clean_query, ""))
        return clean_url
    except Exception:
        return raw_url.strip()
