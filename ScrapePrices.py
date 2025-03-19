import json
import logging
import asyncio
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# Load configuration from a JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Set up logging
logging.basicConfig(level=config.get("log_level", "INFO"), format="%(asctime)s - %(levelname)s - %(message)s")


def load_data(file_path: str) -> Optional[List[Dict[str, str]]]:
    """
    Load data from a JSON file.
    Returns the data as a list of dictionaries if successful, otherwise None.
    """
    try:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
        logging.info(f"Loaded {len(data)} entries from '{file_path}'.")
        return data
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found. Exiting.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in '{file_path}'. Exiting.")
        return None


def save_data(data: List[Dict[str, str]], file_path: str) -> None:
    """
    Save the updated data to a JSON file.
    """
    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)
    logging.info(f"Updated data saved to '{file_path}'.")


def should_scrape(item: Dict[str, str], scrape_interval_hours: int) -> bool:
    """
    Determine if a URL should be scraped based on the last scrape time.
    """
    if scrape_interval_hours == 0:
        return True  # Always scrape if interval is 0

    last_scrape = item.get("Last Scrape")
    if not last_scrape:
        return True  # Scrape if no last scrape timestamp exists

    try:
        # Parse the last scrape timestamp
        last_scrape_time = datetime.strptime(last_scrape, "%Y-%m-%d %H:%M")
        # Calculate the time difference
        time_since_last_scrape = datetime.now() - last_scrape_time
        # Check if the interval has passed
        return time_since_last_scrape >= timedelta(hours=scrape_interval_hours)
    except ValueError:
        logging.warning(f"Invalid 'Last Scrape' format for item: {item['Set']}. Scraping anyway.")
        return True


async def scrape_prices(
    data: List[Dict[str, str]], price_selector: str, retries: int, max_threads: int, scrape_interval_hours: int
) -> List[Dict[str, str]]:
    """
    Scrape prices for all items in the data list concurrently.
    Returns the updated data with scraped prices and dates.
    """
    semaphore = asyncio.Semaphore(max_threads)

    async def scrape_single_price(item: Dict[str, str]) -> Optional[str]:
        """
        Scrape the price for a single URL asynchronously.
        Returns the price as a string if successful, otherwise None.
        """
        set_name = item["Set"]
        url = item["Link"]

        async with semaphore:  # Limit concurrency
            logging.info(f"Scraping {set_name}...")  # Log when scraping starts

            start_time = time.time()  # Start the timer

            for attempt in range(retries):
                try:
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        context = await browser.new_context(user_agent=config.get("user_agent"))
                        context.set_default_navigation_timeout(config.get("timeout", 30000))

                        # Block specified resource types
                        block_resources = config.get("block_resources", [])
                        for resource in block_resources:
                            await context.route(f"**/*.{resource}", lambda route: route.abort())

                        page = await context.new_page()

                        await page.goto(url)
                        await page.wait_for_selector(price_selector, timeout=config.get("timeout", 30000))
                        price = await page.query_selector(price_selector)
                        price_text = await price.inner_text()

                        elapsed_time = (time.time() - start_time) * 1000  # Time in ms
                        logging.info(f"Price for {set_name}: {price_text} ({elapsed_time:.2f} ms)")  # Log price and time
                        return price_text

                except Exception as e:
                    elapsed_time = (time.time() - start_time) * 1000  # Time in ms
                    logging.warning(f"Attempt {attempt + 1} failed for {set_name} ({elapsed_time:.2f} ms): {e}")
                    if attempt == retries - 1:
                        logging.error(f"Failed to scrape {set_name} after {retries} attempts.")
                        return None
                    else:
                        # Exponential backoff: wait longer between retries
                        wait_time = 2 ** attempt  # Wait 2^attempt seconds
                        logging.info(f"Retrying {set_name} in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)

    # Filter items based on the scrape interval
    items_to_scrape = [item for item in data if item.get("Link") and should_scrape(item, scrape_interval_hours)]
    logging.info(f"Scraping {len(items_to_scrape)} items (out of {len(data)})...")

    # Create a list of tasks for scraping prices
    tasks = [scrape_single_price(item) for item in items_to_scrape]

    # Run all tasks concurrently
    results = await asyncio.gather(*tasks)

    # Update the data with scraped prices and dates
    for item, price in zip(items_to_scrape, results):
        if price:  # If the scrape succeeded
            item["Current Price"] = price
            item["Last Scrape"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            logging.info(f"Updated {item['Set']} with price: {price}")
        else:  # If the scrape failed
            logging.warning(f"No price scraped for {item['Set']}")

    return data


async def main():
    """Main function to orchestrate the scraping process."""
    # Load data from the JSON file
    data = load_data(config.get("output_file", "sheet_data.json"))
    if not data:
        return  # Exit if data loading fails

    # Scrape prices and update the data
    updated_data = await scrape_prices(
        data,
        price_selector=config["price_selector"],
        retries=config["retries"],
        max_threads=config["MAX_THREADS"],
        scrape_interval_hours=config["scrape_interval_hours"],
    )

    # Save the updated data back to JSON
    save_data(updated_data, config.get("output_file", "sheet_data.json"))


if __name__ == "__main__":
    asyncio.run(main())