"""
scrape_carwale.py
AutoKnowMus - Production CarWale listings scraper.

Features:
  - Multi-city support (Bangalore/Mumbai/Delhi/Pune/Indore/Chennai/etc.)
  - Configurable parallelism (1=safest, 2=balanced, 3=fastest)
  - Resume capability (Ctrl+C, then restart picks up from last checkpoint)
  - Saturation auto-stop (stops if N consecutive pages add no new listings)
  - Adaptive delays (slows down if CarWale starts rate-limiting)
  - Per-page checkpointing (saves every N pages)
  - Failed-page retry queue (retries timeouts at end of run)

Usage examples:
  python scrape_carwale.py --city bangalore --pages 150 --parallel 2
  python scrape_carwale.py --city bangalore --pages 259 --parallel 1     # full inventory, slowest+safest
  python scrape_carwale.py --city mumbai --pages 200 --parallel 2
  python scrape_carwale.py --city bangalore --pages 150 --resume          # continue interrupted run

Output CSV columns match listing_csv_parser.py: o-C href, o-o, o-j1, o-j5
"""

import argparse
import csv
import json
import os
import random
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright


# =============================================================================
# CITY CONFIG - add new cities here as we expand
# =============================================================================
CITY_CONFIGS = {
    "bangalore": {
        "url_path": "cars-in-bangalore",
        "url_filter_segment": "/used/bangalore/",
        "display_name": "Bangalore",
        "expected_total": 6191,  # for progress estimates
    },
    "mumbai": {
        "url_path": "cars-in-mumbai",
        "url_filter_segment": "/used/mumbai/",
        "display_name": "Mumbai",
        "expected_total": None,
    },
    "delhi": {
        "url_path": "cars-in-new-delhi",
        "url_filter_segment": "/used/new-delhi/",
        "display_name": "Delhi NCR",
        "expected_total": None,
    },
    "pune": {
        "url_path": "cars-in-pune",
        "url_filter_segment": "/used/pune/",
        "display_name": "Pune",
        "expected_total": None,
    },
    "chennai": {
        "url_path": "cars-in-chennai",
        "url_filter_segment": "/used/chennai/",
        "display_name": "Chennai",
        "expected_total": None,
    },
    "hyderabad": {
        "url_path": "cars-in-hyderabad",
        "url_filter_segment": "/used/hyderabad/",
        "display_name": "Hyderabad",
        "expected_total": None,
    },
    "indore": {
        "url_path": "cars-in-indore",
        "url_filter_segment": "/used/indore/",
        "display_name": "Indore",
        "expected_total": None,
    },
}

# =============================================================================
# RUNTIME CONSTANTS
# =============================================================================
PAGE_LOAD_TIMEOUT_MS = 45000
SCROLL_PAUSE_MS = 700
MAX_SCROLL_ITERATIONS = 12
SATURATION_PAGES_THRESHOLD = 10  # stop if N consecutive pages = 0 new
CHECKPOINT_EVERY_N_PAGES = 10
MAX_RETRIES_PER_PAGE = 2
RATE_LIMIT_BACKOFF_SEC = 30  # if many failures in a row, pause this long

# Delay tuning per parallelism level (between consecutive page fetches by same worker)
DELAYS_BY_PARALLELISM = {
    1: (1.5, 3.5),   # safest: human-like
    2: (1.0, 2.0),   # balanced
    3: (0.4, 0.9),   # aggressive
}


# =============================================================================
# OUTPUT FILE PATHS
# =============================================================================
def output_paths(city_key):
    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "csv": f"{city_key}_listings_{today}.csv",
        "checkpoint": f".{city_key}_checkpoint.json",
        "log": f"{city_key}_scrape_log_{today}.txt",
    }


CSV_COLUMNS = ["o-C href", "o-o", "o-j1", "o-j5"]


# =============================================================================
# CHECKPOINT MANAGEMENT (for resume capability)
# =============================================================================
class Checkpoint:
    """Saves progress to disk so we can resume after Ctrl+C or crash."""

    def __init__(self, path):
        self.path = path
        self.lock = Lock()
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "city_key": None,
            "pages_completed": [],
            "pages_failed": [],
            "seen_urls": [],
            "started_at": None,
        }

    def save(self):
        with self.lock:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)

    def mark_done(self, page_num):
        with self.lock:
            if page_num not in self.data["pages_completed"]:
                self.data["pages_completed"].append(page_num)
            if page_num in self.data["pages_failed"]:
                self.data["pages_failed"].remove(page_num)

    def mark_failed(self, page_num):
        with self.lock:
            if page_num not in self.data["pages_failed"]:
                self.data["pages_failed"].append(page_num)

    def add_urls(self, urls):
        with self.lock:
            existing = set(self.data["seen_urls"])
            existing.update(urls)
            self.data["seen_urls"] = list(existing)

    def is_done(self, page_num):
        return page_num in self.data["pages_completed"]

    def reset(self, city_key):
        with self.lock:
            self.data = {
                "city_key": city_key,
                "pages_completed": [],
                "pages_failed": [],
                "seen_urls": [],
                "started_at": datetime.now().isoformat(),
            }
            self.save()

    def delete(self):
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


# =============================================================================
# PAGE SCRAPING (Playwright)
# =============================================================================
def auto_scroll(page):
    """Scroll page in increments to trigger lazy-load of all listings."""
    previous_height = 0
    same_count = 0
    for _ in range(MAX_SCROLL_ITERATIONS):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight / 4)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        current_height = page.evaluate("document.body.scrollHeight")
        if current_height == previous_height:
            same_count += 1
            if same_count >= 2:
                break
        else:
            same_count = 0
        previous_height = current_height
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(SCROLL_PAUSE_MS)


def build_extraction_js(url_filter_segment):
    """Build the JS that runs in the browser to extract listings.
    Filter by city URL segment to exclude nearby-city contamination."""
    return r"""
    (cityFilter) => {
      const results = [];
      const seen = new Set();
      const cityRegex = new RegExp(cityFilter.replace(/\//g, '\\/') + '[a-z0-9-]+\\/[a-z0-9]{4,}\\/?$', 'i');
      const anchors = document.querySelectorAll('a[href*="' + cityFilter + '"]');
      for (const a of anchors) {
        const href = a.getAttribute('href');
        if (!href) continue;
        if (!cityRegex.test(href)) continue;
        if (seen.has(href)) continue;

        let card = a;
        let cardRoot = null;
        for (let i = 0; i < 15; i++) {
          card = card.parentElement;
          if (!card) break;
          const text = card.innerText || '';
          if (/Rs\.?\s*[\d.,]+\s*(Lakh|Crore|Cr)/i.test(text) &&
              /[\d,]+\s*km/i.test(text)) {
            cardRoot = card;
            break;
          }
        }
        if (!cardRoot) continue;
        const cardText = cardRoot.innerText || '';
        const lines = cardText.split('\n').map(l => l.trim()).filter(l => l);

        let title = null;
        for (const line of lines) {
          if (/^\d{4}\s+\w/.test(line) && line.length > 10) {
            title = line; break;
          }
        }
        if (!title) continue;

        let kmFuelLoc = null;
        for (const line of lines) {
          if (/[\d,]+\s*km\s*\|/i.test(line)) {
            kmFuelLoc = line; break;
          }
        }
        if (!kmFuelLoc) continue;

        let price = null;
        for (const line of lines) {
          if (/^\s*Rs\.?\s*[\d.,]+\s*(Lakh|Lac|Crore|Cr)\s*$/i.test(line)) {
            price = line.trim(); break;
          }
        }
        if (!price) continue;

        seen.add(href);
        results.push({href, title, kmFuelLoc, price});
      }
      return results;
    }
    """


def scrape_single_page(context, page_num, city_config):
    """Fetch one page. Returns list of dicts (one per listing) or empty list."""
    url = f"https://www.carwale.com/used/{city_config['url_path']}/page-{page_num}/"
    page = context.new_page()
    try:
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT_MS, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        auto_scroll(page)
        page.wait_for_timeout(1000)

        js = build_extraction_js(city_config["url_filter_segment"])
        raw = page.evaluate(js, city_config["url_filter_segment"])

        rows = []
        for item in raw:
            full_url = urljoin("https://www.carwale.com", item["href"])
            rows.append({
                "o-C href": full_url,
                "o-o": item["title"],
                "o-j1": item["kmFuelLoc"],
                "o-j5": item["price"],
            })
        return rows
    finally:
        try:
            page.close()
        except Exception:
            pass


def scrape_with_retries(context, page_num, city_config, max_retries=MAX_RETRIES_PER_PAGE):
    """Wrapper with retries. Returns (rows, error_str_or_None)."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return scrape_single_page(context, page_num, city_config), None
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:150]}"
            if attempt < max_retries:
                time.sleep(2 ** attempt)
    return [], last_err


# =============================================================================
# CSV WRITING (thread-safe append)
# =============================================================================
csv_write_lock = Lock()


def append_rows_to_csv(rows, csv_path):
    """Append rows to CSV. Creates header if file doesn't exist."""
    with csv_write_lock:
        write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)


# =============================================================================
# WORKER POOL
# =============================================================================
class WorkerStats:
    """Thread-safe shared stats across workers."""

    def __init__(self):
        self.lock = Lock()
        self.seen_urls_global = set()
        self.total_unique = 0
        self.consecutive_zero_new = 0
        self.recent_errors = []  # last 20 errors for adaptive delay
        self.adaptive_extra_delay = 0.0

    def add_unique(self, rows):
        """Returns list of newly-unique rows."""
        with self.lock:
            new = []
            for r in rows:
                if r["o-C href"] not in self.seen_urls_global:
                    self.seen_urls_global.add(r["o-C href"])
                    new.append(r)
            self.total_unique += len(new)
            if len(new) == 0 and len(rows) > 0:
                self.consecutive_zero_new += 1
            elif len(new) > 0:
                self.consecutive_zero_new = 0
            return new

    def record_error(self, err):
        with self.lock:
            self.recent_errors.append(time.time())
            if len(self.recent_errors) > 20:
                self.recent_errors.pop(0)
            # If 5+ errors in last 60 seconds, slow down
            recent_in_window = sum(1 for t in self.recent_errors if time.time() - t < 60)
            if recent_in_window >= 5:
                self.adaptive_extra_delay = min(self.adaptive_extra_delay + 1.0, 8.0)

    def relax_delay(self):
        with self.lock:
            self.adaptive_extra_delay = max(0.0, self.adaptive_extra_delay - 0.2)


def page_worker(worker_id, page_queue, browser, city_config, stats, checkpoint, csv_path,
                min_delay, max_delay, total_pages, log):
    """One worker pulls pages off shared queue and processes them."""
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 900},
    )
    try:
        while True:
            try:
                page_num = page_queue.pop(0) if page_queue else None
            except IndexError:
                page_num = None
            if page_num is None:
                break

            rows, err = scrape_with_retries(context, page_num, city_config)

            if err:
                stats.record_error(err)
                checkpoint.mark_failed(page_num)
                log(f"  page {page_num:3d}/{total_pages}: [W{worker_id}] FAILED - {err}")
            else:
                new_rows = stats.add_unique(rows)
                if new_rows:
                    append_rows_to_csv(new_rows, csv_path)
                    checkpoint.add_urls([r["o-C href"] for r in new_rows])
                checkpoint.mark_done(page_num)
                stats.relax_delay()
                dupe_note = f" ({len(rows) - len(new_rows)} dupes)" if len(rows) != len(new_rows) else ""
                log(f"  page {page_num:3d}/{total_pages}: [W{worker_id}] {len(rows):3d} found, {len(new_rows):3d} new{dupe_note}  (total: {stats.total_unique})")

            # Periodic checkpoint flush
            if page_num % CHECKPOINT_EVERY_N_PAGES == 0:
                checkpoint.save()

            # Inter-page delay (with adaptive component)
            base_delay = random.uniform(min_delay, max_delay)
            total_delay = base_delay + stats.adaptive_extra_delay
            time.sleep(total_delay)
    finally:
        try:
            context.close()
        except Exception:
            pass


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================
def setup_signal_handlers(checkpoint, log):
    """Save checkpoint cleanly on Ctrl+C."""
    def handler(signum, frame):
        log("")
        log(">>> Interrupt received. Saving checkpoint and exiting...")
        log(">>> Run with --resume to continue.")
        checkpoint.save()
        sys.exit(130)
    signal.signal(signal.SIGINT, handler)
    try:
        signal.signal(signal.SIGTERM, handler)
    except Exception:
        pass  # Windows doesn't always support SIGTERM


def make_logger(log_path):
    log_lock = Lock()

    def log(msg):
        line = msg if isinstance(msg, str) else str(msg)
        with log_lock:
            print(line, flush=True)
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass
    return log


def parse_args():
    parser = argparse.ArgumentParser(description="AutoKnowMus CarWale scraper")
    parser.add_argument("--city", default="bangalore", choices=list(CITY_CONFIGS.keys()),
                        help="City to scrape (default: bangalore)")
    parser.add_argument("--pages", type=int, default=150,
                        help="Number of pages to crawl (default: 150)")
    parser.add_argument("--parallel", type=int, default=2, choices=[1, 2, 3],
                        help="Parallel workers: 1=safest, 2=balanced (default), 3=aggressive")
    parser.add_argument("--resume", action="store_true",
                        help="Resume interrupted run from last checkpoint")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry only previously-failed pages from checkpoint")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.city not in CITY_CONFIGS:
        print(f"ERROR: Unknown city '{args.city}'. Available: {list(CITY_CONFIGS.keys())}")
        sys.exit(1)

    city_config = CITY_CONFIGS[args.city]
    paths = output_paths(args.city)
    log = make_logger(paths["log"])

    log("=" * 70)
    log(f" AutoKnowMus - CarWale Scraper - {city_config['display_name']}")
    log("=" * 70)
    log(f" City:        {args.city} ({city_config['display_name']})")
    log(f" Pages:       {args.pages}")
    log(f" Parallel:    {args.parallel} worker(s)")
    log(f" Resume:      {args.resume}")
    log(f" Retry mode:  {args.retry_failed}")
    log(f" Output CSV:  {paths['csv']}")
    log(f" Checkpoint:  {paths['checkpoint']}")
    log(f" Log:         {paths['log']}")
    log(f" Started:     {datetime.now().strftime('%H:%M:%S')}")
    log("=" * 70)
    log("")

    checkpoint = Checkpoint(paths["checkpoint"])
    setup_signal_handlers(checkpoint, log)

    # Determine page list to process
    if args.retry_failed:
        if not checkpoint.data["pages_failed"]:
            log("No failed pages to retry. Exiting.")
            return
        pages_to_do = sorted(checkpoint.data["pages_failed"])
        log(f"Retrying {len(pages_to_do)} previously-failed pages: {pages_to_do[:20]}{'...' if len(pages_to_do) > 20 else ''}")
    elif args.resume:
        if checkpoint.data["city_key"] != args.city:
            log(f"WARNING: Checkpoint is for city '{checkpoint.data['city_key']}', not '{args.city}'. Resetting.")
            checkpoint.reset(args.city)
            pages_to_do = list(range(1, args.pages + 1))
        else:
            done = set(checkpoint.data["pages_completed"])
            pages_to_do = [p for p in range(1, args.pages + 1) if p not in done]
            log(f"Resume: {len(checkpoint.data['pages_completed'])} pages already done. Resuming with {len(pages_to_do)} remaining.")
    else:
        # Fresh run
        if checkpoint.data["city_key"] is not None:
            log("Existing checkpoint found. Use --resume to continue, or it will be overwritten.")
            log("Continuing with FRESH run in 3 seconds. Press Ctrl+C to abort.")
            time.sleep(3)
        checkpoint.reset(args.city)
        pages_to_do = list(range(1, args.pages + 1))
        # Also delete old CSV for fresh run
        if os.path.exists(paths["csv"]):
            os.remove(paths["csv"])

    # Restore seen URLs from checkpoint (for resume mode)
    stats = WorkerStats()
    if args.resume or args.retry_failed:
        with stats.lock:
            stats.seen_urls_global.update(checkpoint.data.get("seen_urls", []))
            stats.total_unique = len(stats.seen_urls_global)
        log(f"Restored {stats.total_unique} previously-seen URLs from checkpoint")

    if not pages_to_do:
        log("Nothing to do. Exiting.")
        return

    min_delay, max_delay = DELAYS_BY_PARALLELISM[args.parallel]
    log(f"Inter-page delay per worker: {min_delay}-{max_delay} sec")
    log("")

    # Run workers
    start_time = time.time()
    pages_total = args.pages
    page_queue = list(pages_to_do)  # workers pop from this shared list

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            with ThreadPoolExecutor(max_workers=args.parallel) as executor:
                futures = []
                for w in range(args.parallel):
                    futures.append(executor.submit(
                        page_worker,
                        w + 1,
                        page_queue,
                        browser,
                        city_config,
                        stats,
                        checkpoint,
                        paths["csv"],
                        min_delay,
                        max_delay,
                        pages_total,
                        log,
                    ))
                # Monitor saturation - early exit if N consecutive zero-new pages
                while not all(f.done() for f in futures):
                    time.sleep(2)
                    if stats.consecutive_zero_new >= SATURATION_PAGES_THRESHOLD:
                        log("")
                        log(f">>> SATURATION DETECTED: {SATURATION_PAGES_THRESHOLD} consecutive pages added zero new listings.")
                        log(">>> Likely captured all available inventory. Stopping early.")
                        page_queue.clear()  # workers will exit naturally
                        break
                # Wait for in-flight pages to finish
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        log(f"Worker exception: {e}")
        finally:
            browser.close()

    checkpoint.save()
    elapsed = time.time() - start_time

    # Summary
    log("")
    log("=" * 70)
    log(" CRAWL COMPLETE")
    log("=" * 70)
    log(f" Pages requested:    {pages_total}")
    log(f" Pages completed:    {len(checkpoint.data['pages_completed'])}")
    log(f" Pages failed:       {len(checkpoint.data['pages_failed'])}")
    if checkpoint.data["pages_failed"]:
        failed_preview = sorted(checkpoint.data["pages_failed"])[:20]
        log(f" Failed page nums:   {failed_preview}{'...' if len(checkpoint.data['pages_failed']) > 20 else ''}")
        log(f" To retry: python scrape_carwale.py --city {args.city} --retry-failed")
    log(f" Total unique listings: {stats.total_unique}")
    log(f" Output CSV:         {paths['csv']}")
    log(f" Elapsed time:       {elapsed/60:.1f} min ({elapsed:.0f} sec)")
    log(f" Avg per page:       {elapsed/max(len(checkpoint.data['pages_completed']),1):.1f} sec")
    log(f" Finished:           {datetime.now().strftime('%H:%M:%S')}")
    log("=" * 70)

    if stats.total_unique == 0:
        log("")
        log(" [ERROR] Zero listings collected.")
        sys.exit(1)


if __name__ == "__main__":
    main()
