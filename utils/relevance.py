# utils/relevance.py
import os, time, re
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

AI_SYSTEM = """너는 정유사 직원이다.
텍스트가 석유제품(경유, 휘발유, 등유, 항공유, 아스팔트 등), 정유사, 정제공정, 연료유, 발전소 연료, 해운/철도/군/수협/발전소 유류 공급과 직접 관련 있으면 relevant=True.
정치, 사회, 법률, 제도, 선거, 정당, 이념, 종교, 연예, 스포츠 등은 모두 관련 없음.
'사전경유제', '경유하다' 같은 동음이의어도 관련 없음.
JSON으로만 답하라. 예: {"relevant": true, "confidence": 0.85, "reason": "..."}"""

def heuristic_relevance(text: str, fuel_terms: list, block_keywords: list) -> float:
    low = (text or "").lower()
    if any(b.lower() in low for b in block_keywords):
        return 0.0
    hits = sum(1 for w in fuel_terms if w.lower() in low)
    return min(1.0, 0.4 + 0.2 * hits) if hits else 0.2

def ai_relevance_score(text: str, cfg: dict) -> float:
    if not cfg.get("openai", {}).get("enable_ai_filter", False):
        return -1.0
    api_key = os.getenv(cfg["openai"].get("api_key_env", "OPENAI_API_KEY") or "")
    if not api_key or OpenAI is None:
        return -1.0
    client = OpenAI(api_key=api_key)
    try:
        resp = client.chat.completions.create(
            model=cfg["openai"].get("relevance_model", "gpt-4o-mini"),
            messages=[{"role":"system", "content": AI_SYSTEM},
                      {"role":"user",   "content": text[:1500]}],
            temperature=0,
            max_tokens=120,
        )
        ans = resp.choices[0].message.content or ""
        rel = re.search(r'"relevant"\s*:\s*(true|false)', ans, re.I)
        conf = re.search(r'"confidence"\s*:\s*([0-9.]+)', ans, re.I)
        relevant = (rel and rel.group(1).lower() == "true")
        conf_v = float(conf.group(1)) if conf else (0.8 if relevant else 0.2)
        return conf_v if relevant else 0.0
    except Exception:
        return -1.0

def is_relevant(text: str, cfg: dict) -> bool:
    fuel_terms = cfg["scoring"]["refinery_relevance"]["fuel_terms"]
    block_keywords = cfg["filters"].get("block_keywords", [])
    thr = float(cfg["openai"].get("relevance_threshold", 0.5))

    # 우선 블랙리스트 즉시 컷
    if any(b.lower() in (text or "").lower() for b in block_keywords):
        return False

    score = ai_relevance_score(text, cfg)
    if score >= 0:
        return score >= thr

    return heuristic_relevance(text, fuel_terms, block_keywords) >= thr
