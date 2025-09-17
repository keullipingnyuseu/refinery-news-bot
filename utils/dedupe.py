# utils/dedupe.py
import re
from urllib.parse import urlparse, parse_qs, urlunparse
from rapidfuzz.distance import Levenshtein

TITLE_TAGS_RE = re.compile(r'^\s*(\[속보\]|\[단독\]|\[영상\]|\[포토\]|\(종합\)|\[종합\])\s*', re.I)

def normalize_url(u):
    try:
        p = urlparse(u)
        qs = parse_qs(p.query)
        qs = {k: v for k, v in qs.items() if not k.lower().startswith("utm")}
        q = "&".join(f"{k}={v[0]}" for k, v in qs.items())
        return urlunparse((p.scheme, p.netloc, p.path, "", q, ""))
    except:
        return u

def normalize_title(t: str) -> str:
    t = (t or "").strip()
    t = TITLE_TAGS_RE.sub("", t)
    t = re.sub(r'\s+', ' ', t)
    return t

def is_similar_title(a, b, threshold=0.88):
    if not a or not b:
        return False
    a2 = normalize_title(a)
    b2 = normalize_title(b)
    ratio = 1 - (Levenshtein.distance(a2, b2) / max(len(a2), len(b2)))
    return ratio >= threshold

def dedupe_items(items):
    seen_urls = set()
    result = []
    for it in items:
        nu = normalize_url(it["link"])
        if nu in seen_urls:
            continue
        if any(is_similar_title(it["title"], x["title"]) for x in result):
            continue
        seen_urls.add(nu)
        result.append(it)
    return result

def dedupe_by_title_similarity(items, threshold=0.88):
    result = []
    seen_titles = []
    for it in items:
        title = normalize_title(it.get("title") or "")
        dup = False
        for seen in seen_titles:
            sim = 1 - (Levenshtein.distance(title, seen) / max(len(title), len(seen) or 1))
            if sim >= threshold:
                dup = True
                break
        if not dup:
            result.append(it)
            seen_titles.append(title)
    return result
