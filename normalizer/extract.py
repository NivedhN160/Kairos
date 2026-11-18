import re
import html
from bs4 import BeautifulSoup

def clean_html_text(raw_html: str) -> str:
    """Strips HTML tags, removes excess whitespace, and decodes HTML entities."""
    if not raw_html:
        return ""
    try:
        # Use fast html parser
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "form"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
    except Exception:
        # Fallback to regex
        text = re.sub(r"<[^>]+>", " ", raw_html)

    text = html.unescape(text)
    # Collapse multiple whitespaces and newlines
    text = re.sub(r"\s+", " ", text).strip()
    return text
