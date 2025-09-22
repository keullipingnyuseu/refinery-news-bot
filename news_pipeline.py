# -*- coding: utf-8 -*-
"""
news_pipeline.py (수요·가격 시그널 중심 / AI 최소 사용 / 전역 제목 유사도 dedupe)
- Google News(RSS) 수집
- 도메인/블랙리스트 차단 → (휴리스틱 스코어) → 상위 N개만 AI 관련성 → 최종 정렬/선발
- 메일 카드: 제목, 게시 시각(KST), 원문 링크
- 최종 단계: 제목 유사도 전역 dedupe
"""

import os, sys, smtplib, pytz, yaml, feedparser, requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote_plus

from utils.scoring import compute_score, apply_unrelated_penalty
from utils.relevance import is_relevant
from utils.dedupe import dedupe_items, normalize_url, dedupe_by_title_similarity

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
except Exception:
    BlockingScheduler = None

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
TO_LIST = os.environ.get("TO_LIST", "").split(",")

if not OPENAI_API_KEY or not GMAIL_USER or not GMAIL_PASS:
    print("❌ 환경변수가 올바르게 설정되지 않았습니다.")
    exit(1)


# ----------------- helpers -----------------

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
    q = "&".join([f"{k}={quote_plus(v)}" for k, v in params.items()])
    url = f"{base}?{q}"
    headers = {"User-Agent": "Mozilla/5.0 (refinery-news-bot; +github)"}
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

def published_dt_kst(published_parsed, tz):
    try:
        if published_parsed:
            return datetime(*published_parsed[:6], tzinfo=pytz.utc).astimezone(tz)
    except Exception:
        pass
    return datetime(1970,1,1, tzinfo=tz)

def make_html_email(grouped, cfg, start_dt, end_dt):
    head = f"""
    <html><body style="font-family:Arial,Helvetica,sans-serif;">
      <h2> [이원호 사원의 특수영업팀 일일 정유 뉴스] ({start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')} KST)</h2>
      <p style="color:#666;">수요/가격 변동에 직결된 기사 위주로 선별했습니다. (게시 시각 표기)</p>
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
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import smtplib
    from datetime import datetime
    import os

    GMAIL_USER = os.environ.get("GMAIL_USER")
    GMAIL_PASS = os.environ.get("GMAIL_PASS")
    TO_LIST = os.environ.get("TO_LIST", "").split(",")

    subject = f"[특수영업팀 Daily 뉴스클리핑] {datetime.now().strftime('%Y-%m-%d')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = ", ".join(TO_LIST)

    msg.attach(MIMEText("[이원호 사원의 특수영업팀 일일 정유 뉴스] (HTML 버전 참조)", "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(GMAIL_USER, GMAIL_PASS)
    refused = server.sendmail(GMAIL_USER, TO_LIST, msg.as_string())
    server.quit()

    if refused:
        print("❌ 일부 수신자가 거부됨:", refused)
    else:
        print(f"[OK] 메일 발송 완료 ({len(TO_LIST)}명)")




# ----------------- main -----------------

def run_once():
    cfg = load_config()
    tz = pytz.timezone(cfg["app"]["timezone"])
    now = datetime.now(tz)
    end_dt = now
    start_dt = end_dt - timedelta(hours=cfg["app"]["lookback_hours"])

    selection_mode = (cfg.get("app", {}).get("selection_mode") or "scored").lower()

    print(f"[INFO] Window: {start_dt} ~ {end_dt} {cfg['app']['timezone']} (mode={selection_mode})")
    taxonomy = cfg["taxonomy"]

    grouped = {}                 # ← 반드시 run_once() 내부에서 초기화
    total_raw = 0
    total_kept = 0
    global_seen_urls = set()

    ai_budget = int(cfg.get("openai", {}).get("relevance_max_checks", 20))
    ai_used = 0
    use_ai = bool(cfg.get("openai", {}).get("enable_ai_filter", False))

    # --- 수집/1차필터/선별 ---
    for tax in taxonomy:
        major, minor = tax["major"], tax["minor"]
        grouped.setdefault(major, {})
        grouped[major].setdefault(minor, [])
        keywords = tax["keywords"]

        bucket = []
        for kw in keywords:
            try:
                d = google_news_rss(kw, cfg)
                print(f"[FETCH] {major}/{minor}/{kw}: entries={len(d.entries)}")
                for e in d.entries:
                    link = e.get("link", "")
                    if not link or is_block_domain(link, cfg):
                        continue
                    title, summary = extract_text(e)
                    if not title:
                        continue
                    pub_p = getattr(e, "published_parsed", None)
                    if not within_window(pub_p, tz, start_dt, end_dt):
                        continue
                    low = f"{title} {summary}".lower()
                    if any(w.lower() in low for w in cfg["filters"].get("block_keywords", [])):
                        continue
                    bucket.append({
                        "title": title,
                        "summary": summary,
                        "link": link,
                        "published": getattr(e, "published", ""),
                        "published_local": to_local_str(pub_p, tz),
                        "published_dt": published_dt_kst(pub_p, tz),
                        "major": major,
                        "minor": minor
                    })
            except Exception as ex:
                print(f"[WARN] fetch failed for {kw}: {ex}")

        before = len(bucket)
        bucket = dedupe_items(bucket)  # (URL + 제목유사도 + 단어중복) 소분류 dedupe
        after_dedupe = len(bucket)

        if selection_mode == "scored":
            hitset = set(keywords)
            for it in bucket:
                txt = f"{it['title']} {it.get('summary','')}"
                it["_pre_score"] = compute_score(txt, cfg) + apply_unrelated_penalty(hitset, txt, cfg)
            bucket.sort(key=lambda x: x["_pre_score"], reverse=True)

            can_use = max(0, ai_budget - ai_used)
            top_n = min(can_use, len(bucket))
            filtered = []
            cfg_no_ai = {**cfg, "openai": {**cfg.get("openai", {}), "enable_ai_filter": False}}
            ai_used_now = 0
            for idx, it in enumerate(bucket):
                txt = f"{it['title']}. {it.get('summary','')}"
                if idx < top_n and use_ai:
                    rel = is_relevant(txt, cfg)     # 빠른 버전 relevance (delay=0, retries=1)
                    ai_used += 1
                    ai_used_now += 1
                else:
                    rel = is_relevant(txt, cfg_no_ai)
                if rel:
                    filtered.append(it)

            # 점수 우선, 동점 최신순
            filtered.sort(key=lambda it: (it.get("_pre_score", 0.0), it.get("published_dt")), reverse=True)
        else:
            filtered = sorted(bucket, key=lambda it: it.get("published_dt"), reverse=True)
            ai_used_now = 0

        kept_unique = []
        for it in filtered:
            nu = normalize_url(it["link"])
            if nu in global_seen_urls:
                continue
            kept_unique.append(it)
            global_seen_urls.add(nu)
            if len(kept_unique) >= cfg["app"]["max_items_per_subcategory"]:
                break

        grouped[major][minor] = kept_unique
        total_kept += len(kept_unique)
        total_raw += before
        print(f"[KEEP] {major}/{minor}: raw={before}, deduped={after_dedupe}, ai_used_now={ai_used_now}, kept={len(kept_unique)}")

    # ---------------- 최종: 제목 유사도 + 단어 중복 기반 전역 dedupe ----------------
    all_final = []
    for major, minors in grouped.items():
        for minor, items in minors.items():
            all_final.extend(items)

    # utils.dedupe.dedupe_by_title_similarity(threshold=0.88, min_overlap=2)를 사용
    final_dedup = dedupe_by_title_similarity(all_final, threshold=0.88, min_overlap=2)

    grouped_clean = {}
    for it in final_dedup:
        grouped_clean.setdefault(it["major"], {}).setdefault(it["minor"], []).append(it)

    html = make_html_email(grouped_clean, cfg, start_dt, end_dt)

    try:
        with open("email_preview.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("[INFO] Saved email_preview.html")
    except Exception as ex:
        print(f"[WARN] preview save failed: {ex}")

    print(f"[SUMMARY] total_raw={total_raw}, total_kept={total_kept}, final={len(final_dedup)}, ai_used_total={ai_used}")

    try:
        send_email(html, cfg)   # ← 환경변수 기반 send_email(html, cfg) 유지
        print(f"[OK] Sent {len(final_dedup)} items.")
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
    print(f"[Scheduler] Started. Will run daily at %02d:00 %s." % (cfg["app"]["run_time_hour"], cfg["app"]["timezone"]))
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    main()
