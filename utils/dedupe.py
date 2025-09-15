# utils/dedupe.py
import re
from urllib.parse import urlparse, parse_qs, urlunparse
from rapidfuzz.distance import Levenshtein

def normalize_url(u):
    try:
        p = urlparse(u)
        # UTM 등 추적 파라미터 제거
        qs = parse_qs(p.query)
        qs = {k:v for k,v in qs.items() if not k.lower().startswith("utm")}
        q = "&".join(f"{k}={v[0]}" for k,v in qs.items())
        return urlunparse((p.scheme, p.netloc, p.path, "", q, ""))
    except:
        return u

def is_similar_title(a, b, threshold=0.88):
    if not a or not b:
        return False
    a2 = re.sub(r'\s+', ' ', a.strip())
    b2 = re.sub(r'\s+', ' ', b.strip())
    ratio = 1 - (Levenshtein.distance(a2, b2) / max(len(a2), len(b2)))
    return ratio >= threshold

def dedupe_items(items):
    seen_urls = set()
    result = []
    for it in items:
        nu = normalize_url(it["link"])
        if nu in seen_urls:
            continue
        # 제목 유사도 중복 제거
        if any(is_similar_title(it["title"], x["title"]) for x in result):
            continue
        seen_urls.add(nu)
        result.append(it)
    return result
