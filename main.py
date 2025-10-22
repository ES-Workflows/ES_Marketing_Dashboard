import os, csv, re, time, logging, requests, pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# ---------- Config (from env) ----------
SCRAPINGDOG_API_KEY = os.environ["SCRAPINGDOG_API_KEY"]
SUPABASE_URL        = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]
BUCKET_NAME         = os.environ.get("BUCKET_NAME", "csv-files")

COMPANY_LINKID = os.environ.get("COMPANY_LINKID", "extrastaff-recruitment")
COMPANY_URL    = os.environ.get("COMPANY_URL", "https://www.linkedin.com/company/extrastaff-recruitment")

FOLLOWERS_CSV = "linkedin_followers.csv"
POSTS_CSV     = "lnkdn.csv"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Helpers ----------
def upload_csv_to_supabase(file_path: str):
    """Uploads/overwrites a CSV to Supabase Storage."""
    file_name = os.path.basename(file_path)
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{file_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "text/csv",
        "x-upsert": "true"
    }
    with open(file_path, "rb") as f:
        resp = requests.post(url, headers=headers, data=f.read(), timeout=120)
    if resp.status_code in (200, 201):
        logging.info(f"Uploaded {file_name} to bucket {BUCKET_NAME}")
    else:
        logging.error(f"Upload failed {file_name}: {resp.status_code} {resp.text[:300]}")

def parse_int(text: str):
    """Convert '1.2K' / '846K' / '3M' or '52,345' → int."""
    if text is None:
        return None
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
    """Fetch one page of company updates via Scrapingdog LinkedIn endpoint."""
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
    """Try to paginate; if not supported, we'll still get first page."""
    all_posts = []
    for i in range(max_pages):
        page = fetch_company_posts_page(start=i*limit, limit=limit)
        if not page:
            break
        all_posts.extend(page)
        # if returned less than limit, likely finished
        if len(page) < limit:
            break
        time.sleep(1)
    # If nothing came back with pagination params, try once without them
    if not all_posts:
        all_posts = fetch_company_posts_page()
    return all_posts

def save_posts_append_dedupe():
    """Append latest posts & dedupe by article_link. Keep metrics columns."""
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

    # ensure metric columns exist (will be enriched later)
    for c in ["impressions","reactions","comments","reposts"]:
        if c not in df_all.columns:
            df_all[c] = pd.NA

    if "article_link" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["article_link"], keep="last")

    df_all.to_csv(POSTS_CSV, index=False, encoding="utf-8")
    logging.info(f"Saved {len(df_all)} total posts to {POSTS_CSV}")

# ---------- Per-post metric enrichment ----------
def fetch_post_metrics(article_link: str):
    """Fetch a post page and parse impressions/reactions/comments/reposts."""
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
                if m:
                    return parse_int(m.group(1))
            return None

        impressions = grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*impressions',
                            r'(\d[\d,\.]*\s*[KkMm]?)\s*views'])
        reactions  = grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reactions',
                           r'(\d[\d,\.]*\s*[KkMm]?)\s*likes'])
        comments   = grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*comments?'])
        reposts    = grab([r'(\d[\d,\.]*\s*[KkMm]?)\s*reposts?',
                           r'(\d[\d,\.]*\s*[KkMm]?)\s*shares?'])

        return {"impressions": impressions, "reactions": reactions, "comments": comments, "reposts": reposts}
    except Exception as e:
        logging.error(f"metric parse failed for {article_link}: {e}")
        return {}

def enrich_posts_metrics(csv_path=POSTS_CSV, max_to_enrich=20, sleep_sec=2):
    """Fill missing metrics for a small batch each run (rate-limit friendly)."""
    if not os.path.exists(csv_path):
        return
    df = pd.read_csv(csv_path)
    # rows with all four metrics missing
    need = df[df[["impressions","reactions","comments","reposts"]].isna().all(axis=1)]
    need = need.head(max_to_enrich)
    if need.empty:
        logging.info("No posts need enrichment.")
        return

    updated = 0
    for idx, row in need.iterrows():
        url = row.get("article_link")
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        metrics = fetch_post_metrics(url)
        if metrics:
            for k, v in metrics.items():
                df.at[idx, k] = v
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

    # posts (append+dedupe) + enrich (batch) + upload
    save_posts_append_dedupe()
    enrich_posts_metrics(POSTS_CSV, max_to_enrich=20, sleep_sec=2)
    upload_csv_to_supabase(POSTS_CSV)

    logging.info("✅ Pipeline complete")
