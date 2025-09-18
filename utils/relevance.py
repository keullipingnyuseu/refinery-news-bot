import os, time, re
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

AI_SYSTEM = """너는 정유사 영업/기획 담당자다.
아래 텍스트가 '연료 수요 증가' 또는 '유종 가격 변동'과 직접적으로 연관되면 relevant=True.
예시(관련): 교통량/여객 증가, 조업/어획 증가, 항만/물동량 증가, 항로 신설·운항 증가, 아스콘/도로 발주, 한파·폭염에 따른 난방·냉방 수요, 명절 이동,
정제마진/크랙/국제유가/세금·정책 변화, 조달/입찰/장기공급, 가동·정비·재가동, OPEC 감산/재고 변화 등.
정치/정당/선거/연예/사건사고(비에너지적)/사회 이슈는 관련 없음. '경유(지나가다)' 같은 동음이의어는 관련 없음.
JSON 한 줄: {"relevant": true/false, "confidence": 0~1} 로만 답하라."""

def ai_relevance_score(text: str, cfg: dict) -> float:
    if not cfg.get("openai", {}).get("enable_ai_filter", False):
        return -1.0
    api_key = os.getenv(cfg["openai"].get("api_key_env", "OPENAI_API_KEY") or "")
    if not api_key or OpenAI is None:
        return -1.0

    client = OpenAI(api_key=api_key)
    model = cfg["openai"].get("relevance_model", "gpt-4o-mini")
    delay = float(cfg["openai"].get("relevance_delay_secs", 1.0))
    backoff = float(cfg["openai"].get("relevance_backoff_secs", 8))

    for _ in range(3):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role":"system","content":AI_SYSTEM},
                          {"role":"user","content":(text or "")[:1800]}],
                temperature=0,
                max_tokens=100,
            )
            ans = resp.choices[0].message.content or ""
            rel = re.search(r'"relevant"\s*:\s*(true|false)', ans, re.I)
            conf = re.search(r'"confidence"\s*:\s*([0-9.]+)', ans, re.I)
            ok = (rel and rel.group(1).lower() == "true")
            c  = float(conf.group(1)) if conf else (0.8 if ok else 0.2)
            if delay > 0:
                time.sleep(delay)
            return c if ok else 0.0
        except Exception as e:
            if "429" in str(e) or "Rate limit" in str(e):
                time.sleep(backoff)
                continue
            return -1.0
    return -1.0

def is_relevant(text: str, cfg: dict) -> bool:
    thr = float(cfg["openai"].get("relevance_threshold", 0.65))
    # 블랙리스트 즉시 컷
    low = (text or "").lower()
    for w in cfg["filters"].get("block_keywords", []):
        if w.lower() in low:
            return False
    score = ai_relevance_score(text, cfg)
    if score >= 0:
        return score >= thr
    # AI 실패 시 보수적 휴리스틱: 가격/수요/ops 시그널이 하나라도 있으면 True
    tl = low
    sig = cfg["scoring"]
    def _hit(terms): return any(t.lower() in tl for t in terms)
    if _hit(sig["price_signals"]["terms"]) or _hit(sig["demand_signals"]["terms"]) or _hit(sig["ops_supply_signals"]["terms"]):
        return True
    return False
