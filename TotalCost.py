from typing import List, Dict, Optional
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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


def calculate_total_value(data: List[Dict[str, str]], ignore_highest: int = 0) -> float:
    """
    Calculate the total value of all scraped prices, ignoring the highest-cost items.

    Args:
        data (List[Dict[str, str]]): The JSON data containing scraped prices.
        ignore_highest (int): The number of highest-cost items to ignore.

    Returns:
        float: The total value of the scraped prices.
    """
    prices = []
    for item in data:
        price_str = item.get("Current Price", "").strip()
        if price_str and price_str.startswith("$"):
            try:
                price = float(price_str.replace("$", "").replace(",", ""))
                prices.append(price)
            except ValueError:
                logging.warning(f"Invalid price format: {price_str}")

    # Sort the prices in ascending order
    prices_sorted = sorted(prices)

    # Ignore the highest-cost items
    if ignore_highest > 0:
        prices_sorted = prices_sorted[:-ignore_highest]

    # Calculate the total value
    total = sum(prices_sorted)
    return total


def main():
    """Main function to calculate the total value of scraped prices."""
    # Load the data from the JSON file
    data = load_data("sheet_data.json")
    if not data:
        return  # Exit if data loading fails

    # Number of highest-cost items to ignore
    ignore_highest = 10  # Change this value as needed

    # Calculate the total value of scraped prices
    total_value = calculate_total_value(data, ignore_highest)
    logging.info(f"Total value of all scraped prices (ignoring {ignore_highest} highest-cost items): ${total_value:.2f}")


if __name__ == "__main__":
    main()
