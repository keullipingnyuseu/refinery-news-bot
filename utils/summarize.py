# utils/summarize.py
import os, time, re
try:
    from openai import OpenAI
    import openai as openai_pkg
except Exception:
    OpenAI = None
    openai_pkg = None

SYSTEM_PROMPT = """너는 정유사 내부 전파용 뉴스 요약 비서다.
- 정유/석유제품/유통/조달/채널 관련 핵심 사실만 1~2문장으로.
- 정치/이념/연예 코멘트 금지.
- 가격/정책/공급/수요/정제마진/조달/규제/사고/가동/정비 이슈 우선."""

def _heuristic(text: str) -> str:
    """모델 없이 규칙 기반 1~2문장 요약(간단, 안정)."""
    txt = (text or "").strip()
    # 문장 단위 분할
    sents = re.split(r'(?<=[.!?。！？])\s+', txt)
    # 너무 길면 앞에서 2문장만
    head = " ".join(sents[:2]).strip()
    if not head:
        head = txt[:180]
    return head[:220] + ("…" if len(head) > 220 else "")

def summarize_openai(text: str, cfg: dict, delay_secs: float, backoff_secs: float):
    """OpenAI로 1~2문장 요약. 429일 때 백오프."""
    if OpenAI is None:
        return _heuristic(text)
    api_key = os.getenv(cfg["openai"].get("api_key_env", "OPENAI_API_KEY") or "")
    if not api_key:
        return _heuristic(text)

    client = OpenAI(api_key=api_key)
    prompt = f"다음 기사를 1~2문장으로 요약:\n\n{(text or '')[:4000]}"

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=cfg["openai"].get("model", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=120,
            )
            # 호출 간 rate 조절
            if delay_secs and delay_secs > 0:
                time.sleep(delay_secs)
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            msg = str(e)
            # rate limit → 백오프 후 재시도
            if "Rate limit" in msg or "rate limit" in msg or "429" in msg:
                time.sleep(backoff_secs or 20)
                continue
            # 기타 오류는 폴백
            return _heuristic(text)
    return _heuristic(text)

def summarize_1_2(text: str, cfg: dict):
    """외부에서 호출되는 단일 API. provider와 enable_summarize에 따라 분기."""
    if not cfg.get("openai", {}).get("enable_summarize", False):
        return _heuristic(text)

    provider = (cfg.get("openai", {}).get("provider") or "openai").lower()
    if provider == "heuristic":
        return _heuristic(text)

    delay = float(cfg["openai"].get("summarize_delay_secs", 1.5))
    backoff = float(cfg["openai"].get("summarize_backoff_secs", 20))
    return summarize_openai(text, cfg, delay, backoff)
