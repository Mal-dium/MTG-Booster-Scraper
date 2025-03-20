import json
import logging
import asyncio
import time
import os
import threading
import atexit
import signal
import sys
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# Progress tracking setup
PROGRESS_FILE = "scrape_progress.json"
progress_lock = threading.Lock()

browsers = []  # List to track all browser instances

class ManagedBrowser:
    """Context manager for Playwright browsers to ensure they are closed properly."""
    def __init__(self, browser):
        self.browser = browser
        browsers.append(self.browser)

    async def __aenter__(self):
        return self.browser

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.browser.close()
        browsers.remove(self.browser)

async def cleanup_browsers():
    """Close all tracked browsers."""
    await asyncio.gather(*(browser.close() for browser in browsers if browser))
    browsers.clear()

def handle_exit_signal(signum, frame):
    asyncio.create_task(cleanup_browsers())
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit_signal)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_exit_signal)  # Termination signal

class ProgressTracker:
    """Thread-safe progress tracking for async scraping"""
    def __init__(self, total_items: int, items_to_scrape: int):
        self.total_items = total_items
        self.items_to_scrape = items_to_scrape
        self.processed = 0
        self.failed = 0
        self.successful = 0
        self.start_time = time.time()
        self._initialize_progress_file()

    def _initialize_progress_file(self):
        with progress_lock:
            with open(PROGRESS_FILE, "w") as f:
                json.dump({
                    "total_items": self.total_items,
                    "items_to_scrape": self.items_to_scrape,
                    "processed": self.processed,
                    "failed": self.failed,
                    "successful": self.successful,
                    "estimated_remaining_time": 0
                }, f)
                f.flush()
                os.fsync(f.fileno())

    def update_progress(self, success: bool):
        with progress_lock:
            self.processed += 1
            if success:
                self.successful += 1
            else:
                self.failed += 1

            elapsed_time = time.time() - self.start_time
            avg_time_per_item = elapsed_time / self.processed if self.processed > 0 else 0
            remaining_items = self.items_to_scrape - self.processed
            estimated_remaining_time = remaining_items * avg_time_per_item

            with open(PROGRESS_FILE, "w") as f:
                json.dump({
                    "total_items": self.total_items,
                    "items_to_scrape": self.items_to_scrape,
                    "processed": self.processed,
                    "failed": self.failed,
                    "successful": self.successful,
                    "estimated_remaining_time": estimated_remaining_time
                }, f)
                f.flush()
                os.fsync(f.fileno())

    def cleanup(self):
        with progress_lock:
            if os.path.exists(PROGRESS_FILE):
                try:
                    os.remove(PROGRESS_FILE)
                except Exception as e:
                    logging.warning(f"Progress cleanup error: {str(e)}")

# Load configuration from a JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Set up logging
logging.basicConfig(level=config.get("log_level", "INFO"), format="%(asctime)s - %(levelname)s - %(message)s")

def load_data(file_path: str) -> Optional[List[Dict[str, str]]]:
    try:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
        logging.info(f"Loaded {len(data)} entries from '{file_path}'.")
        return data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error loading data from '{file_path}': {str(e)}")
        return None

def save_data(data: List[Dict[str, str]], file_path: str) -> None:
    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)
    logging.info(f"Updated data saved to '{file_path}'.")

def should_scrape(item: Dict[str, str], scrape_interval_hours: int) -> bool:
    if scrape_interval_hours == 0:
        return True

    last_scrape = item.get("Last Scrape")
    if not last_scrape:
        return True

    try:
        last_scrape_time = datetime.strptime(last_scrape, "%Y-%m-%d %H:%M")
        time_since_last = datetime.now() - last_scrape_time
        return time_since_last >= timedelta(hours=scrape_interval_hours)
    except ValueError:
        logging.warning(f"Invalid 'Last Scrape' format for {item['Set']}. Scraping anyway.")
        return True

async def scrape_prices(data: List[Dict[str, str]], price_selector: str, retries: int, max_threads: int, scrape_interval_hours: int) -> List[Dict[str, str]]:
    total_items = len(data)
    items_to_scrape = [item for item in data if item.get("Link") and should_scrape(item, scrape_interval_hours)]

    if not items_to_scrape:
        logging.info("No items need scraping at this time.")
        return data

    progress = ProgressTracker(total_items, len(items_to_scrape))
    logging.info(f"Scraping {len(items_to_scrape)} items (out of {total_items})...")

    semaphore = asyncio.Semaphore(max_threads)
    async def scrape_single_price(item: Dict[str, str]) -> Optional[str]:
        async with semaphore:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                async with ManagedBrowser(browser) as browser:
                    set_name = item["Set"]
                    url = item["Link"]
                    result = None
                    try:
                        logging.info(f"Scraping {set_name}...")
                        start_time = time.time()

                        for attempt in range(retries):
                            try:
                                context = await browser.new_context(user_agent=config.get("user_agent"))
                                context.set_default_navigation_timeout(config.get("timeout", 30000))
                                for resource in config.get("block_resources", []):
                                    await context.route(f"**/*.{resource}", lambda route: route.abort())

                                page = await context.new_page()
                                await page.goto(url)
                                await page.wait_for_selector(price_selector, timeout=config.get("timeout", 30000))
                                price = await page.query_selector(price_selector)
                                result = await price.inner_text()

                                elapsed = (time.time() - start_time) * 1000
                                logging.info(f"Price for {set_name}: {result} ({elapsed:.2f} ms)")
                                break

                            except Exception as e:
                                elapsed = (time.time() - start_time)  * 1000
                                logging.warning(f"Attempt {attempt+1} failed for {set_name}: {str(e)} ({elapsed:.2f} ms)")
                                if attempt < retries - 1:
                                    wait = 2 ** attempt
                                    logging.info(f"Retrying {set_name} in {wait}s...")
                                    await asyncio.sleep(wait)
                    finally:
                        progress.update_progress(success=result is not None)
                    return result

    tasks = [scrape_single_price(item) for item in items_to_scrape]
    results = await asyncio.gather(*tasks)

    for item, price in zip(items_to_scrape, results):
        if price:
            item["Current Price"] = price
            item["Last Scrape"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            logging.info(f"Updated {item['Set']} with price: {price}")

    progress.cleanup()
    return data

async def main():
    data = load_data(config.get("output_file", "sheet_data.json"))
    if not data:
        return

    updated_data = await scrape_prices(
        data,
        price_selector=config["price_selector"],
        retries=config["retries"],
        max_threads=config["MAX_THREADS"],
        scrape_interval_hours=config["scrape_interval_hours"],
    )

    save_data(updated_data, config.get("output_file", "sheet_data.json"))

if __name__ == "__main__":
    asyncio.run(main())