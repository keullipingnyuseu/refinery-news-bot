# utils/scoring.py
import re

def _contains_any(text_low, terms):
    return any(t.lower() in text_low for t in terms)

def compute_score(text, cfg):
    tl = (text or "").lower()
    s = 0.0

    # 수요/가격 신호 가중
    s += cfg["scoring"]["price_signals"]["weight"] * sum(1 for t in cfg["scoring"]["price_signals"]["terms"] if t.lower() in tl)
    s += cfg["scoring"]["demand_signals"]["weight"] * sum(1 for t in cfg["scoring"]["demand_signals"]["terms"] if t.lower() in tl)

    # 연료/유종 용어 가중
    s += cfg["scoring"]["fuel_terms"]["weight"] * sum(1 for t in cfg["scoring"]["fuel_terms"]["terms"] if t.lower() in tl)

    # 채널별 수요 신호(수협/훼리)
    ch = cfg["scoring"]["channel_signals"]
    s += ch["weight"] * sum(1 for t in ch["suhyup_terms"] if t.lower() in tl)
    s += ch["weight"] * sum(1 for t in ch["ferry_terms"] if t.lower() in tl)

    # 정치/연예 페널티(강)
    for t in cfg["scoring"]["political_penalty"]["terms"]:
        if t.lower() in tl:
            s += cfg["scoring"]["political_penalty"]["weight"]
    for t in cfg["scoring"]["soft_penalty"]["terms"]:
        if t.lower() in tl:
            s += cfg["scoring"]["soft_penalty"]["weight"]

    # 단위/지표 힌트(약)
    if re.search(r'\b(bbl|배럴|정제|정유|스프레드|크랙|리터|원\/l|원\/리터)\b', tl):
        s += 0.5

    return s

def apply_unrelated_penalty(hit_keywords, text, cfg):
    tl = (text or "").lower()
    hits_kw = sum(tl.count(k.lower()) for k in hit_keywords)
    hits_fu = sum(tl.count(t.lower()) for t in cfg["scoring"]["fuel_terms"]["terms"])
    if hits_kw <= 0 and hits_fu <= 0:
        return cfg["scoring"]["unrelated_penalty"]["weight"]
    return 0.0
