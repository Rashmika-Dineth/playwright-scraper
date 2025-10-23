from playwright.sync_api import sync_playwright
import pandas as pd
import hashlib, os, datetime, sys

# === CONFIG ===
URL = "https://example.com/products"  # <--- change to your target
OUTPUT_DIR = "artifacts"               # Action-friendly directory
LATEST_PATH = os.path.join(OUTPUT_DIR, "latest.csv")
ARCHIVE_DIR = os.path.join(OUTPUT_DIR, "archive")
LOG_PATH = os.path.join(OUTPUT_DIR, "run.log")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def log(msg):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def get_hash(value: str):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def scrape_page():
    log(f"Starting scrape of {URL}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)

        # --- EDIT selectors below to suit your target site ---
        items = page.query_selector_all(".product-card")
        data = []
        for item in items:
            # safe extraction with fallback
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
    # We treat rows present in df_new but not in df_old (by hash) as new/changed,
    # and rows present in df_old but not in df_new as removed.
    old_hashes = set(df_old["hash"].astype(str).tolist())
    new_hashes = set(df_new["hash"].astype(str).tolist())

    added_hashes = new_hashes - old_hashes
    removed_hashes = old_hashes - new_hashes

    added = df_new[df_new["hash"].isin(added_hashes)].copy()
    removed = df_old[df_old["hash"].isin(removed_hashes)].copy()
    return added, removed

def main():
    try:
        df_new = scrape_page()
    except Exception as e:
        log(f"ERROR during scrape: {e}")
        raise

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Save new snapshot
    df_new.to_csv(LATEST_PATH, index=False)
    log(f"Wrote latest snapshot: {LATEST_PATH}")

    if os.path.exists(LATEST_PATH) and os.path.exists(LATEST_PATH) and os.path.getsize(LATEST_PATH) > 0:
        # If an old snapshot exists in archive, try to find the latest archived snapshot
        archived_snapshots = sorted([p for p in os.listdir(ARCHIVE_DIR) if p.startswith("snapshot_")])
        if archived_snapshots:
            latest_archived = archived_snapshots[-1]
            df_old = pd.read_csv(os.path.join(ARCHIVE_DIR, latest_archived))
        else:
            # If no archived snapshot exists, treat empty old df
            df_old = pd.DataFrame(columns=df_new.columns)

        added, removed = detect_deltas(df_new, df_old)

        if not added.empty or not removed.empty:
            delta_path_added = os.path.join(ARCHIVE_DIR, f"added_{timestamp}.csv")
            delta_path_removed = os.path.join(ARCHIVE_DIR, f"removed_{timestamp}.csv")
            if not added.empty:
                added.to_csv(delta_path_added, index=False)
                log(f"Found {len(added)} added/changed rows -> {delta_path_added}")
            if not removed.empty:
                removed.to_csv(delta_path_removed, index=False)
                log(f"Found {len(removed)} removed rows -> {delta_path_removed}")
        else:
            log("No changes detected.")

    # Archive the new snapshot for next run
    snap_archive_path = os.path.join(ARCHIVE_DIR, f"snapshot_{timestamp}.csv")
    df_new.to_csv(snap_archive_path, index=False)
    log(f"Archived snapshot to {snap_archive_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FATAL ERROR: {exc}")
        sys.exit(2)
    sys.exit(0)
