# utils/relevance.py
import os, time, re
import pytz
from datetime import datetime
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# 간단 휴리스틱: 정유 관련 용어가 있고, 금칙어가 없으면 True 쪽으로 가중
def heuristic_relevance(text: str, fuel_terms: list, block_keywords: list) -> float:
    t = (text or "").lower()
    if any(b.lower() in t for b in block_keywords):
        return 0.0
    hits = sum(1 for w in fuel_terms if w.lower() in t)
    if hits == 0:
        return 0.2
    return min(1.0, 0.4 + 0.2 * hits)

AI_SYSTEM = """너는 정유사 내부 뉴스 필터링 담당자다.
아래 텍스트가 '정유/석유제품/연료/정제/조달/발전/해운 연료'와 직접적으로 관련이 있는지 판정하라.
- 정치/정당/선거/연예/스포츠/순수 문화 기사는 '관련 없음'.
- 관련 있으면 relevant=True, 없으면 False.
JSON 한 줄로만 답하라. 예: {"relevant": true, "confidence": 0.85, "reason": "정제마진/경유 가격 언급"}
"""

def ai_relevance_score(text: str, cfg: dict) -> float:
    """OpenAI 분류 결과를 0~1 점수로 환산. 실패 시 -1 반환."""
    if not cfg.get("openai", {}).get("enable_ai_filter", False):
        return -1.0
    api_key = os.getenv(cfg["openai"].get("api_key_env", "OPENAI_API_KEY") or "")
    if not api_key or OpenAI is None:
        return -1.0
    model = cfg["openai"].get("relevance_model", "gpt-4o-mini")
    delay = float(cfg["openai"].get("relevance_delay_secs", 0.8))
    backoff = float(cfg["openai"].get("relevance_backoff_secs", 15))

    client = OpenAI(api_key=api_key)
    prompt = f"텍스트:\n{text[:4000]}"
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role":"system", "content": AI_SYSTEM},
                          {"role":"user",   "content": prompt}],
                temperature=0,
                max_tokens=120,
            )
            ans = resp.choices[0].message.content or ""
            # 간단 파싱
            rel = re.search(r'"relevant"\s*:\s*(true|false)', ans, re.I)
            conf = re.search(r'"confidence"\s*:\s*([0-9.]+)', ans, re.I)
            relevant = (rel.group(1).lower() == "true") if rel else False
            conf_v = float(conf.group(1)) if conf else (0.8 if relevant else 0.2)
            if delay > 0:
                time.sleep(delay)
            return conf_v if relevant else (1.0 - conf_v) * 0.2  # 관련없으면 낮은 점수
        except Exception as e:
            if "429" in str(e) or "Rate limit" in str(e):
                time.sleep(backoff)
                continue
            return -1.0
    return -1.0

def is_relevant(text: str, cfg: dict) -> bool:
    """AI 점수(가능시) 또는 휴리스틱으로 관련성 최종 판정."""
    fuel_terms = cfg["scoring"]["refinery_relevance"]["fuel_terms"]
    block_keywords = cfg["filters"].get("block_keywords", [])
    thr = float(cfg["openai"].get("relevance_threshold", 0.5))

    # 최우선: 블랙리스트 키워드 등장 시 즉시 제외
    lowtext = (text or "").lower()
    if any(b.lower() in lowtext for b in block_keywords):
        return False

    # AI 점수 시도
    score = ai_relevance_score(text, cfg)
    if score >= 0:
        return score >= thr

    # 폴백: 휴리스틱
    h = heuristic_relevance(text, fuel_terms, block_keywords)
    return h >= thr
