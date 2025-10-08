import os
import schedule
import csv
import logging
import requests
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
import os 
import time 
import csv 
import logging 
import schedule 
import requests 
import pandas as pd
import re 
from datetime import datetime
from bs4 import BeautifulSoup

# ----------------------------
# CONFIG
# ----------------------------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
LINKEDIN_URL = "https://www.linkedin.com/company/extrastaff-recruitment"
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BUCKET_NAME = "csv-files"

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_fetcher.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Supabase CSV Upload (HTTP with upsert)
# ----------------------------
def upload_csv_to_supabase(file_path, bucket_name):
    """Uploads CSV to Supabase Storage, overwriting existing files if necessary."""
    file_name = os.path.basename(file_path)
    file_path = os.path.abspath(file_path)

    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        upload_url = f"{SUPABASE_URL}/storage/v1/object/{bucket_name}/{file_name}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "text/csv",
            "x-upsert": "true"  # forces overwrite
        }

        response = requests.post(upload_url, headers=headers, data=file_bytes)

        if response.status_code in (200, 201):
            logging.info(f"✅ Successfully uploaded '{file_name}' to '{bucket_name}'.")
        else:
            logging.error(f"❌ Supabase upload failed ({response.status_code}): {response.text}")

    except Exception as e:
        logging.error(f"❌ Upload failed for {file_name}: {e}")

# ----------------------------
# LinkedIn Follower Extractor
# ----------------------------
class LinkedInFollowerExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def extract_followers(self, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        text_content = soup.get_text()
        pattern = r"(\d+(?:,\d+)*)\s+followers"
        match = re.search(pattern, text_content, re.IGNORECASE)
        return int(match.group(1).replace(",", "")) if match else None

    def get_followers(self, linkedin_url):
        try:
            response = self.session.get(linkedin_url, timeout=15)
            if response.status_code != 200:
                logging.error(f"HTTP {response.status_code}")
                return None
            if "login" in response.url.lower():
                logging.error("Blocked by LinkedIn login/authwall")
                return None
            return self.extract_followers(response.text)
        except Exception as e:
            logging.error(f"Error fetching followers: {e}")
            return None

# ----------------------------
# Save Follower Data
# ----------------------------
def save_follower_data(followers):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {"timestamp": timestamp, "linkedin_url": LINKEDIN_URL, "followers": int(followers)}

    file_path = "linkedin_followers.csv"
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "linkedin_url", "followers"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

    logging.info(f"📊 Follower data appended: {followers}")
    upload_csv_to_supabase(file_path, BUCKET_NAME)

def fetch_linkedin_followers():
    extractor = LinkedInFollowerExtractor()
    followers = extractor.get_followers(LINKEDIN_URL)
    if followers:
        save_follower_data(followers)
    else:
        logging.error("Failed to fetch followers")

# ----------------------------
# Fetch LinkedIn Posts
# ----------------------------
def fetch_linkedin_posts():
    try:
        url = "https://api.scrapingdog.com/linkedin"
        params = {"api_key": SCRAPINGDOG_API_KEY, "type": "company", "linkId": "extrastaff-recruitment"}
        response = requests.get(url, params=params, timeout=45)
        data = response.json()

        if not data or not isinstance(data, list):
            logging.error("Invalid API response for posts")
            return

        posts = data[0].get("updates", [])

        if posts:
            df = pd.DataFrame(posts)
            file_path = "linkedin_posts.csv"
            df.to_csv(file_path, index=False)
            logging.info(f"📰 Saved {len(df)} posts")

            # -------------------
            # Delete old file in Supabase if exists
            # -------------------
            try:
                delete_url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{os.path.basename(file_path)}"
                headers = {
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}"
                }
                del_response = requests.delete(delete_url, headers=headers)
                if del_response.status_code in (200, 204):
                    logging.info("🗑 Old posts file deleted from Supabase (if it existed).")
                else:
                    logging.info("ℹ No previous posts file to delete or failed to delete.")
            except Exception as e:
                logging.error(f"Error deleting old posts file: {e}")

            # Upload new file
            upload_csv_to_supabase(file_path, BUCKET_NAME)
        else:
            logging.info("No new posts found")

    except Exception as e:
        logging.error(f"Error fetching posts: {e}")


# ---------------------------- #
# Scheduler
# ---------------------------- #

# Followers every 5 minutes
schedule.every(5).minutes.do(fetch_linkedin_followers)

# Posts every 4 hours  
schedule.every(4).hours.do(fetch_linkedin_posts)

logging.info("🚀 Starting LinkedIn data pipeline...")

# Initial run
fetch_linkedin_followers()
fetch_linkedin_posts()

while True:
    schedule.run_pending()
    time.sleep(1)
