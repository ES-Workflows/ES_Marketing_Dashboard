import os, csv, re, time, logging, requests, pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import quote

# ========= Config from env =========
SCRAPINGDOG_API_KEY = os.environ["SCRAPINGDOG_API_KEY"]
SUPABASE_URL        = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]
BUCKET_NAME         = os.environ.get("BUCKET_NAME", "csv-files")
COMPANY_LINKID      = os.environ.get("COMPANY_LINKID", "extrastaff-recruitment")
COMPANY_URL         = os.environ.get("COMPANY_URL", f"https://www.linkedin.com/company/{COMPANY_LINKID}")

# file names (don’t change unless you want new outputs)
FOLLOWERS_CSV       = "linkedin_followers.csv"
POSTS_CSV           = "lnkdn.csv"
FOLLOWERS_DAILY_CSV = "followers_daily.csv"
CHANNEL_SUMMARY_CSV = "channel_summary.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========= Helpers =========
def upload_csv_to_supabase(file_path: str):
    """Upload CSV to Supabase Storage; handles spaces via URL-encoding."""
    file_name  = os.path.basename(file_path)
    bucket_enc = quote(BUCKET_NAME, safe="")
    fname_enc  = quote(file_name,   safe="")
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket_enc}/{fname_enc}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "text/csv",
        "x-upsert": "true"
    }
    with open(file_path, "rb") as f:
        resp = requests.post(url, headers=headers, data=f.read(), timeout=120)
    if resp.status_code in (200, 201):
        logging.info(f"Upload OK: {file_name} → bucket '{BUCKET_NAME}'")
    else:
        logging.error(f"Upload failed {file_name}: {resp.status_code} {resp.text[:300]}")

def parse_int(text: str):
    """Convert '1.2K'/'846K'/'3M' or '52,345' to int."""
    if text is None: return None
    s = str(text).replace(",", "").strip().lower()
    m = re.match(r"^(\d+(\.\d+)?)([km])?$", s)
    if m:
        num = float(m.group(1))
        suf = m.group(3)
        if suf == "k": num *= 1_000
        if suf == "m": num *= 1_000_000
        return int(num)
    m2 = re.search(r"(\d[\d\.]*)", s)
    return int(float(m2.group(1))) if m2 else None

# ========= Followers =========
def fetch_followers():
    """Scrape company page via Scrapingdog generic endpoint (JS rendered)."""
    api = "https://api.scrapingdog.com/scrape"
    params = {"api_key": SCRAPINGDOG_API_KEY, "url": COMPANY_URL, "render_js": "true"}
    r = requests.get(api, params=params, timeout=60)
    if r.status_code != 200:
        logging.error(f"Followers fetch HTTP {r.status_code}")
        return None
    soup = BeautifulSoup(r.text, "lxml")
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(\d[\d,\.]*\s*[KkMm]?)\s*followers", txt)
    return parse_int(m.group(1)) if m else None

def append_followers_row(count: int):
    file_exists = os.path.exists(FOLLOWERS_CSV)
    row = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
           "linkedin_url": COMPANY_URL,
           "followers": int(count)}
    with open(FOLLOWERS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "linkedin_url", "followers"])
        if not file_exists: w.writeheader()
        w.writerow(row)
    logging.info(f"Saved followers row: {row}")

# ========= Posts: fetch feed =========
def try_company_updates_calls():
    """Try several Scrapingdog endpoints/params; return list of updates dicts."""
    attempts = [
        ("https://api.scrapingdog.com/linkedin", {"type": "company", "linkId": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin", {"type": "company", "username": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin/company", {"linkId": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin/company", {"username": COMPANY_LINKID}),
    ]
    for base, extra in attempts:
        params = {"api_key": SCRAPINGDOG_API_KEY, **extra}
        try:
            r = requests.get(base, params=params, timeout=60)
            if r.status_code != 200:
                logging.error(f"Posts page HTTP {r.status_code} for {base} with {extra}")
                continue
            data = r.json()
            updates = (data or [{}])[0].get("updates", []) if isinstance(data, list) else []
            if updates:
                return updates
        except Exception as e:
            logging.error(f"Posts fetch error ({base}): {e}")
    return []

def fetch_posts_all_pages(limit=20, max_pages=8, sleep_sec=1):
    """Attempt pagination; fall back to single call if not supported."""
    all_posts = []
    first = try_company_updates_calls()
    if first:
        all_posts.extend(first)

    paginated_attempts = [
        ("https://api.scrapingdog.com/linkedin", {"type": "company", "linkId": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin", {"type": "company", "username": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin/company", {"linkId": COMPANY_LINKID}),
        ("https://api.scrapingdog.com/linkedin/company", {"username": COMPANY_LINKID}),
    ]
    for base, extra in paginated_attempts:
        got_any = False
        for i in range(max_pages):
            params = {"api_key": SCRAPINGDOG_API_KEY, **extra, "start": i*limit, "limit": limit}
            try:
                r = requests.get(base, params=params, timeout=60)
                if r.status_code != 200: break
                data = r.json()
                page = (data or [{}])[0].get("updates", []) if isinstance(data, list) else []
                if not page: break
                got_any = True
                all_posts.extend(page)
                if len(page) < limit: break
                time.sleep(sleep_sec)
            except Exception as e:
                logging.error(f"Pagination error ({base}): {e}")
                break
        if got_any: break
    return all_posts

def save_posts_append_dedupe():
    posts = fetch_posts_all_pages()
    if not posts:
        logging.info("No posts returned.")
        return
    keep = ["text","article_posted_date","total_likes","article_title","article_sub_title","article_link"]
    df_new = pd.DataFrame(posts)
    df_new = df_new[[c for c in keep if c in df_new.columns]]

    if os.path.exists(POSTS_CSV):
        df_all = pd.concat([pd.read_csv(POSTS_CSV), df_new], ignore_index=True)
    else:
        df_all = df_new

    for c in ["impressions","reactions","comments","reposts"]:
        if c not in df_all.columns:
            df_all[c] = pd.NA

    if "article_link" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["article_link"], keep="last")

    df_all.to_csv(POSTS_CSV, index=False, encoding="utf-8")
    logging.info(f"Saved {len(df_all)} total posts to {POSTS_CSV}")

# ========= Posts: enrich per-post metrics =========
def _extract_from_json_blocks(html_text):
    nums = {}
    candidates = [
        ("impressions", r'"impressions"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("views",       r'"views"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("likes",       r'"likes"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("reactions",   r'"reactions"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("comments",    r'"comments"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("replies",     r'"replies"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("reposts",     r'"reposts"\s*:\s*"?([\d,\.KkMm]+)"?'),
        ("shares",      r'"shares"\s*:\s*"?([\d,\.KkMm]+)"?'),
    ]
    for key, pat in candidates:
        m = re.search(pat, html_text, flags=re.IGNORECASE)
        if m:
            nums[key] = parse_int(m.group(1))
    out = {}
    out["impressions"] = nums.get("impressions") or nums.get("views")
    out["reactions"]   = nums.get("reactions")  or nums.get("likes")
    out["comments"]    = nums.get("comments")   or nums.get("replies")
    out["reposts"]     = nums.get("reposts")    or nums.get("shares")
    return {k: v for k, v in out.items() if v is not None}

def fetch_post_metrics(article_link: str):
    """Scrape post page (JS rendered) and parse metrics with several fallbacks."""
    try:
        api = "https://api.scrapingdog.com/scrape"
        params = {"api_key": SCRAPINGDOG_API_KEY, "url": article_link, "render_js": "true"}
        r = requests.get(api, params=params, timeout=60)
        if r.status_code != 200:
            logging.warning(f"metrics HTTP {r.status_code} for {article_link}")
            return {}
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)

        def grab(patterns):
            for p in patterns:
                m = re.search(p, text, flags=re.IGNORECASE)
                if m: return parse_int(m.group(1))
            return None

        metrics = {
            "impressions": grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*impressions', r'(\d[\d,\.]*\s*[KkMm]?)\s*views']),
            "reactions":   grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reactions',   r'(\d[\d,\.]*\s*[KkMm]?)\s*likes']),
            "comments":    grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*comments?']),
            "reposts":     grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reposts?',    r'(\d[\d,\.]*\s*[KkMm]?)\s*shares?']),
        }

        # JSON fallback
        missing = [k for k, v in metrics.items() if v is None]
        if missing:
            from_json = _extract_from_json_blocks(html)
            for k in missing:
                if k in from_json: metrics[k] = from_json[k]
        return {k: v for k, v in metrics.items() if v is not None}
    except Exception as e:
        logging.error(f"metric parse failed for {article_link}: {e}")
        return {}

def enrich_posts_metrics(csv_path=POSTS_CSV, max_to_enrich=40, sleep_sec=2):
    if not os.path.exists(csv_path): return
    df = pd.read_csv(csv_path)
    for c in ["impressions","reactions","comments","reposts"]:
        if c not in df.columns:
            df[c] = pd.NA
    need = df[df[["impressions","reactions","comments","reposts"]].isna().any(axis=1)]
    need = need.head(max_to_enrich)
    if need.empty:
        logging.info("No posts need enrichment.")
        return
    updated = 0
    for idx, row in need.iterrows():
        url = row.get("article_link")
        if not isinstance(url, str) or not url.startswith("http"): continue
        m = fetch_post_metrics(url)
        if "reactions" not in m and not pd.isna(row.get("total_likes")):
            m["reactions"] = int(row["total_likes"])   # fallback
        if m:
            for k, v in m.items():
                df.at[idx, k] = v
            logging.info(f"Enriched: {url} -> {m}")
            updated += 1
        time.sleep(sleep_sec)
    if updated:
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logging.info(f"Enriched metrics for {updated} posts.")

# ========= Derived outputs for Power BI =========
def build_followers_daily():
    """From raw timestamped followers, build daily last value + daily delta."""
    if not os.path.exists(FOLLOWERS_CSV):
        return
    df = pd.read_csv(FOLLOWERS_CSV, parse_dates=["timestamp"])
    if df.empty: return
    df["date"] = df["timestamp"].dt.date
    daily = (df.sort_values("timestamp")
               .groupby("date", as_index=False)
               .tail(1)[["date","followers"]])  # last reading per day
    daily = daily.sort_values("date")
    daily["new_followers"] = daily["followers"].diff().fillna(0).astype(int)
    daily.to_csv(FOLLOWERS_DAILY_CSV, index=False, encoding="utf-8")
    upload_csv_to_supabase(FOLLOWERS_DAILY_CSV)

def build_channel_summary():
    """
    Produce a single summary CSV with columns useful for the 'Channel performance' table:
    platform, total_followers, new_followers, post_shares, post_impressions
    (For now LinkedIn only; others can be added later.)
    """
    total_followers = 0
    new_followers   = 0
    if os.path.exists(FOLLOWERS_DAILY_CSV):
        fd = pd.read_csv(FOLLOWERS_DAILY_CSV)
        if not fd.empty:
            total_followers = int(fd["followers"].iloc[-1])
            new_followers   = int(fd["new_followers"].iloc[-1])

    post_impressions = 0
    post_shares      = 0
    if os.path.exists(POSTS_CSV):
        p = pd.read_csv(POSTS_CSV)
        if "impressions" in p.columns:
            post_impressions = int(pd.to_numeric(p["impressions"], errors="coerce").fillna(0).sum())
        if "reposts" in p.columns:
            post_shares = int(pd.to_numeric(p["reposts"], errors="coerce").fillna(0).sum())

    out = pd.DataFrame([{
        "platform": "LinkedIn Pages",
        "total_followers": total_followers,
        "new_followers": new_followers,
        "post_shares": post_shares,
        "post_impressions": post_impressions
    }])
    out.to_csv(CHANNEL_SUMMARY_CSV, index=False, encoding="utf-8")
    upload_csv_to_supabase(CHANNEL_SUMMARY_CSV)

# ========= Main =========
if __name__ == "__main__":
    logging.info("🚀 Running LinkedIn data pipeline")

    followers = fetch_followers()
    if followers is not None:
        append_followers_row(followers)
        upload_csv_to_supabase(FOLLOWERS_CSV)

    save_posts_append_dedupe()
    enrich_posts_metrics(POSTS_CSV, max_to_enrich=40, sleep_sec=2)
    upload_csv_to_supabase(POSTS_CSV)

    build_followers_daily()
    build_channel_summary()

    logging.info("✅ Pipeline complete")
