# utils/scoring.py
import re

def compute_score(text, cfg):
    text_low = text.lower()
    s = 0.0
    s += cfg["scoring"]["base_match"]

    # 정유 연관성 가중치(+)
    fuel_terms = cfg["scoring"]["refinery_relevance"]["fuel_terms"]
    s += cfg["scoring"]["refinery_relevance"]["weight"] * sum(1 for t in fuel_terms if t.lower() in text_low)

    # 정치성/연성 페널티
    for term in cfg["scoring"]["political_penalty"]["terms"]:
        if term.lower() in text_low:
            s += cfg["scoring"]["political_penalty"]["weight"]
    for term in cfg["scoring"]["soft_penalty"]["terms"]:
        if term.lower() in text_low:
            s += cfg["scoring"]["soft_penalty"]["weight"]

    # 숫자·단위 기반 약한 힌트(정유 기사에 흔한 패턴)
    if re.search(r'\b(bbl|배럴|원\/l|원\/리터|정제|정유|리파이너리|유가|스프레드|크랙)\b', text_low):
        s += 0.5

    return s

def apply_unrelated_penalty(hit_keywords, text, cfg):
    # 키워드 1개만 매치 + 전체 텍스트가 정유 용어 거의 없음 → 페널티
    text_low = text.lower()
    refinery_hits = sum(text_low.count(t.lower()) for t in cfg["scoring"]["refinery_relevance"]["fuel_terms"])
    if len(hit_keywords) <= 1 and refinery_hits == 0:
        return cfg["scoring"]["unrelated_penalty"]["weight"]
    return 0.0
