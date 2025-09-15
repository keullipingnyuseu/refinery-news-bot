# utils/summarize.py
import os
from openai import OpenAI

SYSTEM_PROMPT = """너는 정유사 내부 전파용 뉴스 요약 비서다.
- 정유/석유제품/유통/조달/채널 관련 핵심 사실만 1~2문장으로.
- 정치/이념/연예 등의 코멘트는 넣지 말 것.
- 가격/정책/공급/수요/정제마진/조달/계약/규제 변경/사고/가동/정비 이슈 우선."""

def summarize_1_2(text, cfg):
    if not cfg["openai"]["enable_summarize"]:
        # 요약 미사용 시, 앞부분 자르고 리턴
        return (text or "").strip()[:180] + ("…" if len(text) > 180 else "")

    api_key = os.getenv(cfg["openai"]["api_key_env"])
    if not api_key:
        return (text or "").strip()[:180] + ("…" if len(text) > 180 else "")

    client = OpenAI(api_key=api_key)
    prompt = f"다음 기사를 1~2문장으로 요약:\n\n{text[:4000]}"
    resp = client.chat.completions.create(
        model=cfg["openai"]["model"],
        messages=[
            {"role":"system","content":SYSTEM_PROMPT},
            {"role":"user","content":prompt}
        ],
        temperature=0.2,
        max_tokens=120
    )
    return resp.choices[0].message.content.strip()
