import re
from urllib.parse import urlparse, parse_qs, urlunparse
from rapidfuzz.distance import Levenshtein

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = parse_qs(p.query)
        qs = {k: v for k, v in qs.items() if not k.lower().startswith("utm")}
        q = "&".join(f"{k}={v[0]}" for k, v in qs.items())
        return urlunparse((p.scheme, p.netloc, p.path, "", q, ""))
    except:
        return u

def is_similar_title(a: str, b: str, threshold=0.88) -> bool:
    if not a or not b:
        return False
    a2 = re.sub(r'\s+', ' ', a.strip())
    b2 = re.sub(r'\s+', ' ', b.strip())
    sim = 1 - (Levenshtein.distance(a2, b2) / max(len(a2), len(b2)))
    return sim >= threshold

def dedupe_items(items: list) -> list:
    """소분류 내 URL/제목 유사도 중복 제거"""
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

def dedupe_by_title_similarity(items: list, threshold=0.88) -> list:
    """최종 단계 전역 제목 유사도 중복 제거"""
    result, seen = [], []
    for it in items:
        t = (it.get("title") or "").strip()
        if any(is_similar_title(t, s, threshold) for s in seen):
            continue
        result.append(it)
        seen.append(t)
    return result
