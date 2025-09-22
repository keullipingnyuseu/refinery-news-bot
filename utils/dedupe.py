import re
from urllib.parse import urlparse, parse_qs, urlunparse
from rapidfuzz.distance import Levenshtein

# ---- 기존 URL 정규화 ----
def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = parse_qs(p.query)
        qs = {k: v for k, v in qs.items() if not k.lower().startswith("utm")}
        q = "&".join(f"{k}={v[0]}" for k, v in qs.items())
        return urlunparse((p.scheme, p.netloc, p.path, "", q, ""))
    except:
        return u

# ---- 제목 문자열 유사도(레벤슈타인) ----
def is_similar_title(a: str, b: str, threshold=0.88) -> bool:
    if not a or not b:
        return False
    a2 = re.sub(r'\s+', ' ', a.strip())
    b2 = re.sub(r'\s+', ' ', b.strip())
    sim = 1 - (Levenshtein.distance(a2, b2) / max(len(a2), len(b2)))
    return sim >= threshold

# ================== 새로 추가: 단어 기반 중복 판정 ==================

# 간단 불용어(ko/en). 필요 시 확장 가능
STOPWORDS_KO = {
    "속보","단독","영상","포토","사진","인터뷰","기자",
    "오늘","내일","어제","금일","이번","지난","관련","총정리",
    "누적","최신","전체","전망","분석","종합","브리핑","현황",
    "정부","당국","당일","업계","업체","회사","그룹","해외","국내"
}
STOPWORDS_EN = {
    "the","a","an","and","or","for","to","of","in","on","at","by","with",
    "from","as","is","are","be","being","been","this","that","these","those",
    "update","breaking","exclusive","photo","video","report"
}

TOKEN_RE = re.compile(r"[A-Za-z0-9가-힣]+")

def _tokenize_title(title: str):
    """제목에서 한/영/숫자 토큰만 추출, 소문자화, 불용어/1글자 제거"""
    if not title:
        return []
    tokens = TOKEN_RE.findall(title.lower())
    # 1글자 토큰 제거(‘a’, ‘가’ 등) + 불용어 제거
    toks = [t for t in tokens if len(t) >= 2 and t not in STOPWORDS_KO and t not in STOPWORDS_EN]
    return toks

def _word_overlap_count(a: str, b: str) -> int:
    sa, sb = set(_tokenize_title(a)), set(_tokenize_title(b))
    return len(sa & sb)

def is_duplicate_by_overlap(a: str, b: str, min_overlap: int = 3) -> bool:
    """공통 단어가 min_overlap개 이상이면 중복으로 간주"""
    if not a or not b:
        return False
    return _word_overlap_count(a, b) >= min_overlap

# ================== 통합 중복 제거 함수들 ==================

def dedupe_items(items: list) -> list:
    """소분류 내부: URL/제목 유사도/단어 중복으로 중복 제거"""
    seen_urls = set()
    result = []
    for it in items:
        nu = normalize_url(it["link"])
        if nu in seen_urls:
            continue
        t = (it.get("title") or "").strip()
        # 이미 담긴 결과와 제목 중복/유사도 검사
        dup = False
        for prev in result:
            pt = (prev.get("title") or "").strip()
            if is_similar_title(t, pt, threshold=0.88) or is_duplicate_by_overlap(t, pt, min_overlap=3):
                dup = True
                break
        if dup:
            continue
        seen_urls.add(nu)
        result.append(it)
    return result

def dedupe_by_title_similarity(items: list, threshold=0.88, min_overlap=3) -> list:
    """전역 단계: (레벤슈타인 유사도) OR (단어 중복≥min_overlap) 로 중복 제거"""
    result, seen_titles = [], []
    for it in items:
        t = (it.get("title") or "").strip()
        dup = False
        for st in seen_titles:
            if is_similar_title(t, st, threshold=threshold) or is_duplicate_by_overlap(t, st, min_overlap=min_overlap):
                dup = True
                break
        if not dup:
            result.append(it)
            seen_titles.append(t)
    return result
