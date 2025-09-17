def _count_hits(text_low: str, terms: list) -> int:
    return sum(1 for t in terms if t.lower() in text_low)

def compute_score(text: str, cfg: dict) -> float:
    tl = (text or "").lower()
    S = 0.0
    S += cfg["scoring"].get("base", 0.0)

    S += cfg["scoring"]["fuel_core_terms"]["weight"] * _count_hits(tl, cfg["scoring"]["fuel_core_terms"]["terms"])
    S += cfg["scoring"]["demand_signals"]["weight"] * _count_hits(tl, cfg["scoring"]["demand_signals"]["terms"])
    S += cfg["scoring"]["price_signals"]["weight"]  * _count_hits(tl, cfg["scoring"]["price_signals"]["terms"])
    S += cfg["scoring"]["ops_supply_signals"]["weight"] * _count_hits(tl, cfg["scoring"]["ops_supply_signals"]["terms"])

    # 연성/정치 잡음 페널티
    S += cfg["scoring"]["soft_penalty"]["weight"] * _count_hits(tl, cfg["scoring"]["soft_penalty"]["terms"])

    # 노이즈 토큰 소폭 페널티
    S += cfg["scoring"]["noise_tokens"]["weight"] * _count_hits(tl, cfg["scoring"]["noise_tokens"]["terms"])
    return S

def apply_unrelated_penalty(hit_keywords: set, text: str, cfg: dict) -> float:
    """키워드 한두 개만 걸리고 유종/시그널 연관이 거의 없으면 소폭 페널티(선택)."""
    # 간단히 보수적 처리: 키워드 매칭 없으면 -0.5
    tl = (text or "").lower()
    if not any(k.lower() in tl for k in hit_keywords):
        return -0.5
    return 0.0
