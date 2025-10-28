import os, sys, datetime, hashlib, pandas as pd
from playwright.sync_api import sync_playwright

# === CONFIG (override via env vars) ===
URL = os.getenv("SCRAPE_URL", "https://example.com/products")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/tmp/artifacts")  # ephemeral local dir
UPLOAD_TO = os.getenv("UPLOAD_TO", "none")  # "s3", "gcs", or "none"
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
os.makedirs(OUTPUT_DIR, exist_ok=True)

LATEST_PATH = os.path.join(OUTPUT_DIR, "latest.csv")
ARCHIVE_DIR = os.path.join(OUTPUT_DIR, "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def log(msg):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)

def get_hash(value: str):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def scrape_page():
    log(f"Starting scrape of {URL}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)

        items = page.query_selector_all(".product-card")
        data = []
        for item in items:
            name_el = item.query_selector(".product-name")
            price_el = item.query_selector(".price")
            name = name_el.inner_text().strip() if name_el else ""
            price = price_el.inner_text().strip() if price_el else ""
            hash_val = get_hash(name + "|" + price)
            data.append({"name": name, "price": price, "hash": hash_val})
        browser.close()
        df = pd.DataFrame(data)
        log(f"Scraped {len(df)} items")
        return df

def detect_deltas(df_new, df_old):
    old_hashes = set(df_old["hash"].astype(str))
    new_hashes = set(df_new["hash"].astype(str))
    added_hashes = new_hashes - old_hashes
    removed_hashes = old_hashes - new_hashes
    added = df_new[df_new["hash"].isin(added_hashes)].copy()
    removed = df_old[df_old["hash"].isin(removed_hashes)].copy()
    return added, removed

def upload_to_cloud(path: str, dest_name: str):
    if UPLOAD_TO == "s3":
        import boto3
        s3 = boto3.client("s3")
        s3.upload_file(path, BUCKET_NAME, dest_name)
        log(f"Uploaded to s3://{BUCKET_NAME}/{dest_name}")
    elif UPLOAD_TO == "gcs":
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(dest_name)
        blob.upload_from_filename(path)
        log(f"Uploaded to gs://{BUCKET_NAME}/{dest_name}")
    else:
        log(f"Skipped upload (UPLOAD_TO={UPLOAD_TO})")

def main():
    df_new = scrape_page()
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    df_new.to_csv(LATEST_PATH, index=False)
    log(f"Saved latest snapshot: {LATEST_PATH}")

    # Load previous if exists
    prev_path = os.path.join(ARCHIVE_DIR, "last_snapshot.csv")
    if os.path.exists(prev_path):
        df_old = pd.read_csv(prev_path)
    else:
        df_old = pd.DataFrame(columns=df_new.columns)

    added, removed = detect_deltas(df_new, df_old)
    if not added.empty or not removed.empty:
        added_path = os.path.join(ARCHIVE_DIR, f"added_{timestamp}.csv")
        removed_path = os.path.join(ARCHIVE_DIR, f"removed_{timestamp}.csv")
        if not added.empty:
            added.to_csv(added_path, index=False)
            upload_to_cloud(added_path, f"added_{timestamp}.csv")
        if not removed.empty:
            removed.to_csv(removed_path, index=False)
            upload_to_cloud(removed_path, f"removed_{timestamp}.csv")
        log("Changes detected and uploaded.")
    else:
        log("No changes detected.")

    # Save new snapshot for next run
    df_new.to_csv(prev_path, index=False)
    upload_to_cloud(prev_path, f"snapshot_{timestamp}.csv")
    log("Completed run.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL ERROR: {e}")
        sys.exit(1)
