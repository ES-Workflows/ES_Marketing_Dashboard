import os, csv, re, time, logging, requests, pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import quote

# ---------- Env ----------
SCRAPINGDOG_API_KEY = os.environ["SCRAPINGDOG_API_KEY"]
SUPABASE_URL        = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]
BUCKET_NAME         = os.environ.get("BUCKET_NAME", "csv-files")     # may contain spaces
COMPANY_LINKID      = os.environ.get("COMPANY_LINKID", "extrastaff-recruitment")
COMPANY_URL         = os.environ.get("COMPANY_URL", "https://www.linkedin.com/company/extrastaff-recruitment")

FOLLOWERS_CSV = "linkedin_followers.csv"
POSTS_CSV     = "lnkdn.csv"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Helpers ----------
def upload_csv_to_supabase(file_path: str):
    """Uploads/overwrites CSV to Supabase Storage (handles spaces via URL-encoding)."""
    file_name = os.path.basename(file_path)
    bucket_safe = quote(BUCKET_NAME, safe="")     # encodes spaces etc.
    fname_safe  = quote(file_name,   safe="")
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket_safe}/{fname_safe}"
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
        num = float(m.group(1)); suf = m.group(3)
        if suf == "k": num *= 1_000
        if suf == "m": num *= 1_000_000
        return int(num)
    m2 = re.search(r"(\d[\d\.]*)", s)
    return int(float(m2.group(1))) if m2 else None

# ---------- Followers ----------
def fetch_followers():
    """Use Scrapingdog generic scrape to avoid LinkedIn login wall."""
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

# ---------- Posts (company feed) ----------
def fetch_company_posts_page(start=None, limit=None):
    api = "https://api.scrapingdog.com/linkedin"
    params = {"api_key": SCRAPINGDOG_API_KEY, "type": "company", "linkId": COMPANY_LINKID}
    if start is not None: params["start"] = start
    if limit is not None: params["limit"] = limit
    r = requests.get(api, params=params, timeout=60)
    if r.status_code != 200:
        logging.error(f"Posts page HTTP {r.status_code}")
        return []
    data = r.json()
    return (data or [{}])[0].get("updates", []) if isinstance(data, list) else []

def fetch_posts_all_pages(limit=20, max_pages=10):
    all_posts = []
    for i in range(max_pages):
        page = fetch_company_posts_page(start=i*limit, limit=limit)
        if not page: break
        all_posts.extend(page)
        if len(page) < limit: break
        time.sleep(1)
    if not all_posts:     # if pagination not supported, at least get first page
        all_posts = fetch_company_posts_page()
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

# ---------- Per-post metric enrichment ----------
def fetch_post_metrics(article_link: str):
    try:
        api = "https://api.scrapingdog.com/scrape"
        params = {"api_key": SCRAPINGDOG_API_KEY, "url": article_link, "render_js": "true"}
        r = requests.get(api, params=params, timeout=60)
        if r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(" ", strip=True)

        def grab(patterns):
            for p in patterns:
                m = re.search(p, text)
                if m: return parse_int(m.group(1))
            return None

        return {
            "impressions": grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*impressions', r'(\d[\d,\.]*\s*[KkMm]?)\s*views']),
            "reactions":   grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reactions',   r'(\d[\d,\.]*\s*[KkMm]?)\s*likes']),
            "comments":    grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*comments?']),
            "reposts":     grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reposts?',    r'(\d[\d,\.]*\s*[KkMm]?)\s*shares?']),
        }
    except Exception as e:
        logging.error(f"metric parse failed for {article_link}: {e}")
        return {}

def enrich_posts_metrics(csv_path=POSTS_CSV, max_to_enrich=20, sleep_sec=2):
    if not os.path.exists(csv_path): return
    df = pd.read_csv(csv_path)
    need = df[df[["impressions","reactions","comments","reposts"]].isna().all(axis=1)]
    need = need.head(max_to_enrich)
    if need.empty:
        logging.info("No posts need enrichment.")
        return
    updated = 0
    for idx, row in need.iterrows():
        url = row.get("article_link")
        if not isinstance(url, str) or not url.startswith("http"): continue
        m = fetch_post_metrics(url)
        if m:
            for k,v in m.items(): df.at[idx, k] = v
            updated += 1
        time.sleep(sleep_sec)
    if updated:
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logging.info(f"Enriched metrics for {updated} posts.")

# ---------- Main ----------
if __name__ == "__main__":
    logging.info("🚀 Running LinkedIn data pipeline")

    # followers
    followers = fetch_followers()
    if followers is not None:
        append_followers_row(followers)
        upload_csv_to_supabase(FOLLOWERS_CSV)

    # posts
    save_posts_append_dedupe()
    enrich_posts_metrics(POSTS_CSV, max_to_enrich=20, sleep_sec=2)
    upload_csv_to_supabase(POSTS_CSV)

    logging.info("✅ Pipeline complete")
