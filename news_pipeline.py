# -*- coding: utf-8 -*-
"""
news_pipeline.py (제목만 표시 / 전역 URL+제목 유사도 중복 제거 / AI 최소화)
- Google News(RSS) 수집
- 블랙리스트 사전 차단 → (선택) AI 관련성 필터(상위 N개만) → 득점/정렬/TopN
- HTML 메일: 제목, 게시 시각(KST), 링크 버튼  ※ 요약/본문 미표시
"""

import os, sys, smtplib, pytz, yaml, feedparser, requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote_plus

from rapidfuzz.distance import Levenshtein

from utils.scoring import compute_score, apply_unrelated_penalty
from utils.dedupe import dedupe_items, normalize_url   # URL 전역 중복 제거에 사용
from utils.relevance import is_relevant                # AI/휴리스틱 관련성 판정

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
except Exception:
    BlockingScheduler = None

# ----------------- Helpers -----------------

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def google_news_rss(query, cfg):
    base = cfg["sources"]["google_news"]["base"]
    params = {
        "q": f'{query} when:1d',
        "hl": cfg["sources"]["google_news"]["hl"],
        "gl": cfg["sources"]["google_news"]["gl"],
        "ceid": cfg["sources"]["google_news"]["ceid"],
    }
    q = "&".join([f"{k}={quote_plus(v)}" for k,v in params.items()])
    url = f"{base}?{q}"
    headers = {"User-Agent": "Mozilla/5.0 (refinery-news-bot; +https://github.com)"}
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    return feedparser.parse(r.text)

def extract_text(entry):
    title = entry.get("title", "")
    summary_html = entry.get("summary", "")
    try:
        summary = BeautifulSoup(summary_html, "html5lib").get_text(" ", strip=True)
    except Exception:
        summary = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)
    return title, summary

def is_block_domain(link, cfg):
    for d in cfg["filters"]["block_domains"]:
        if d in link:
            return True
    return False

def within_window(published, tz, start_dt, end_dt):
    if not published:
        return True
    try:
        dt = datetime(*published[:6], tzinfo=pytz.utc).astimezone(tz)
        return start_dt <= dt <= end_dt
    except Exception:
        return True

def to_local_str(published, tz):
    try:
        dt = datetime(*published[:6], tzinfo=pytz.utc).astimezone(tz)
        return dt.strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        return "시간 정보 없음"

# 제목 유사도 전역 중복 제거 (Levenshtein similarity)
def dedupe_by_title_similarity(items, threshold=0.88):
    result = []
    seen_titles = []
    for it in items:
        title = (it.get("title") or "").strip()
        dup = False
        for seen in seen_titles:
            # 유사도 = 1 - (edit distance / max len)
            sim = 1 - (Levenshtein.distance(title, seen) / max(len(title), len(seen) or 1))
            if sim >= threshold:
                dup = True
                break
        if not dup:
            result.append(it)
            seen_titles.append(title)
    return result

# ----------------- Pipeline pieces -----------------

def build_query_terms(taxonomy_item):
    return taxonomy_item["keywords"]

def block_by_keywords(title, summary, cfg):
    low = f"{title} {summary}".lower()
    for w in cfg["filters"].get("block_keywords", []):
        if w.lower() in low:
            return True
    return False

def score_and_filter(items, hit_keywords, cfg):
    scored = []
    for it in items:
        text = f'{it["title"]} {it.get("summary","")}'
        s = compute_score(text, cfg)
        s += apply_unrelated_penalty(hit_keywords, text, cfg)
        if s < 0:
            continue
        it["score"] = s
        scored.append(it)
    return sorted(scored, key=lambda x: x["score"], reverse=True)

def make_html_email(grouped, cfg, start_dt, end_dt):
    # 제목 + 게시 시각 + 링크 버튼만 출력
    head = f"""
    <html><body style="font-family:Arial,Helvetica,sans-serif;">
      <h2>정유 뉴스 요약 ({start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')} KST)</h2>
      <p style="color:#666;">정유사 직원 관점 유의미 기사만 선별했습니다. (각 카드에 게시 시각 표시)</p>
    """
    cards = []
    for major, minors in grouped.items():
        cards.append(f'<h3 style="border-bottom:2px solid #eee;padding-bottom:4px;">{major}</h3>')
        for minor, items in minors.items():
            if not items:
                continue
            cards.append(f'<h4 style="margin:10px 0 6px 0;color:#0a4;">{minor}</h4>')
            for it in items:
                posted = it.get("published_local", "시간 정보 없음")
                cards.append(f"""
                <div style="border:1px solid #eee;border-radius:10px;padding:12px;margin:8px 0;">
                  <div style="color:#888;font-size:0.9em;margin-bottom:4px;">📅 {posted}</div>
                  <div style="font-weight:600;margin-bottom:10px;">{it['title']}</div>
                  <a style="display:inline-block;background:#1565C0;color:#fff;padding:8px 12px;border-radius:6px;text-decoration:none;"
                     href="{it['link']}" target="_blank" rel="noopener">원문 보기</a>
                </div>
                """)
    tail = "</body></html>"
    return head + "\n".join(cards) + tail

def send_email(html, cfg):
    print(f"[SMTP] host={cfg['email']['smtp_host']} port={cfg['email']['smtp_port']} tls={cfg['email']['use_tls']}")
    msg = MIMEMultipart("alternative")
    subject = f"{cfg['email']['subject_prefix']} {datetime.now().strftime('%Y-%m-%d')}"
    msg["Subject"] = subject
    msg["From"] = f"{cfg['email']['from_name']} <{cfg['email']['from_addr']}>"
    msg["To"] = ", ".join(cfg["email"]["to_addrs"])
    msg.attach(MIMEText(html, "html", "utf-8"))

    server = smtplib.SMTP(cfg["email"]["smtp_host"], cfg["email"]["smtp_port"], timeout=20)
    if cfg["email"]["use_tls"]:
        server.starttls()
    pwd = os.getenv(cfg["email"]["password_env"], "")
    if cfg["email"]["username"]:
        print(f"[SMTP] login as {cfg['email']['username']} (pwd={'SET' if bool(pwd) else 'EMPTY'})")
        server.login(cfg["email"]["username"], pwd)
    server.sendmail(cfg["email"]["from_addr"], cfg["email"]["to_addrs"], msg.as_string())
    server.quit()

# ----------------- Main run -----------------

def run_once():
    cfg = load_config()
    tz = pytz.timezone(cfg["app"]["timezone"])
    now = datetime.now(tz)
    end_dt = now
    start_dt = end_dt - timedelta(hours=cfg["app"]["lookback_hours"])

    print(f"[INFO] Window: {start_dt} ~ {end_dt} {cfg['app']['timezone']}")
    taxonomy = cfg["taxonomy"]
    grouped = {}
    total_raw = 0
    total_kept = 0

    # 전역 URL 중복 방지용 세트 (정규화 URL 기준)
    global_seen_urls = set()

    # AI 사용 예산
    ai_budget = int(cfg["openai"].get("relevance_max_checks", 30)) if cfg.get("openai") else 0
    ai_used = 0
    use_ai = bool(cfg.get("openai", {}).get("enable_ai_filter", False))

    for tax in taxonomy:
        major, minor = tax["major"], tax["minor"]
        grouped.setdefault(major, {})
        grouped[major].setdefault(minor, [])
        keywords = build_query_terms(tax)

        bucket = []
        for kw in keywords:
            try:
                d = google_news_rss(kw, cfg)
                got = len(d.entries)
                print(f"[FETCH] {major}/{minor}/{kw}: entries={got}")
                items = []
                for e in d.entries:
                    link = e.get("link", "")
                    if not link or is_block_domain(link, cfg):
                        continue
                    title, summary = extract_text(e)
                    if not title:
                        continue
                    if not within_window(getattr(e, "published_parsed", None), tz, start_dt, end_dt):
                        continue
                    if block_by_keywords(title, summary, cfg):
                        continue
                    published_local = to_local_str(getattr(e, "published_parsed", None), tz)
                    items.append({
                        "title": title,
                        "summary": summary,   # 내부 스코어링용, 렌더링에는 미사용
                        "link": link,
                        "published": getattr(e, "published", ""),
                        "published_local": published_local,
                        "source": getattr(e, "source", {}).get("title") if hasattr(e, "source") else "",
                        "major": major,       # 최종 재그룹핑을 위해 보관
                        "minor": minor
                    })
                total_raw += len(items)
                bucket.extend(items)
            except Exception as ex:
                print(f"[WARN] fetch failed for {kw}: {ex}")

        # 1) 소분류 내 중복 제거
        before = len(bucket)
        bucket = dedupe_items(bucket)
        after_dedupe = len(bucket)

        # 2) 휴리스틱 프리-스코어 정렬
        prelims = []
        hitset = set(keywords)
        for it in bucket:
            txt = f"{it['title']} {it.get('summary','')}"
            pre = compute_score(txt, cfg) + apply_unrelated_penalty(hitset, txt, cfg)
            it["_pre_score"] = pre
            prelims.append(it)
        prelims.sort(key=lambda x: x["_pre_score"], reverse=True)

        # 3) 상위 N개만 AI 필터, 나머지는 휴리스틱
        global_limit = int(cfg.get("openai", {}).get("relevance_max_checks", 30))
        can_use = max(0, global_limit - ai_used)
        top_n = min(can_use, len(prelims))

        filtered = []
        cfg_no_ai = {**cfg, "openai": {**cfg.get("openai", {}), "enable_ai_filter": False}}
        for idx, it in enumerate(prelims):
            txt = f"{it['title']}. {it.get('summary','')}"
            if idx < top_n and use_ai:
                rel = is_relevant(txt, cfg)
                ai_used += 1
            else:
                rel = is_relevant(txt, cfg_no_ai)  # 휴리스틱만
            if rel:
                filtered.append(it)

        # 4) 최종 스코어링
        bucket = score_and_filter(filtered, hitset, cfg)

        # 5) 전역 URL 중복 제거하면서 상위 N개 보존
        kept_unique = []
        for it in bucket:
            nu = normalize_url(it["link"])
            if nu in global_seen_urls:
                continue  # 이미 다른 대/중분류에서 채택됨 → 스킵
            kept_unique.append(it)
            global_seen_urls.add(nu)
            if len(kept_unique) >= cfg["app"]["max_items_per_subcategory"]:
                break

        grouped[major][minor] = kept_unique
        total_kept += len(kept_unique)
        print(f"[KEEP] {major}/{minor}: raw={before}, deduped={after_dedupe}, "
              f"ai_used_now={min(top_n,len(prelims)) if use_ai else 0}, kept_unique={len(kept_unique)}")

    # ---------------- 최종 단계: 제목 유사도 전역 중복 제거 ----------------
    # 6) 모든 최종 아이템 평탄화
    all_final = []
    for major, minors in grouped.items():
        for minor, items in minors.items():
            all_final.extend(items)

    # 7) 제목 유사도 기반 전역 dedupe (threshold=0.88 권장)
    deduped_final = dedupe_by_title_similarity(all_final, threshold=0.88)

    # 8) 카테고리 구조로 재구성 (원 소속 유지)
    grouped_clean = {}
    for it in deduped_final:
        major, minor = it.get("major"), it.get("minor")
        grouped_clean.setdefault(major, {}).setdefault(minor, []).append(it)

    # HTML 생성 (제목만 표시)
    html = make_html_email(grouped_clean, cfg, start_dt, end_dt)

    # 미리보기 저장
    try:
        with open("email_preview.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("[INFO] Saved email_preview.html")
    except Exception as ex:
        print(f"[WARN] preview save failed: {ex}")

    print(f"[SUMMARY] total_raw={total_raw}, total_kept={total_kept}, ai_used={ai_used}, final={len(deduped_final)}")
    if len(deduped_final) == 0:
        print("[INFO] No items kept; sending email anyway (empty) to validate SMTP...")

    # 메일 전송
    try:
        send_email(html, cfg)
        print(f"[OK] Sent {len(deduped_final)} items.")
    except Exception as ex:
        print(f"[ERROR] send_email failed: {ex}")
        raise

def main():
    if "--once" in sys.argv or BlockingScheduler is None:
        run_once()
        return
    cfg = load_config()
    tz = pytz.timezone(cfg["app"]["timezone"])
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(run_once, 'cron', hour=cfg["app"]["run_time_hour"], minute=0)
    print("[Scheduler] Started. Will run daily at %02d:00 %s." % (cfg["app"]["run_time_hour"], cfg["app"]["timezone"]))
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    main()
