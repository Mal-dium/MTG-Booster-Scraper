import json
import logging
from typing import List, Dict, Optional
import gspread
from google.oauth2 import service_account
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configuration from a JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)


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


def update_spreadsheet(data: List[Dict[str, str]]) -> None:
    """
    Update the Google Spreadsheet with the provided data.
    """
    try:
        # Authenticate with Google Sheets API
        creds = service_account.Credentials.from_service_account_file(
            config["service_account_file"], scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)

        # Open the spreadsheet and worksheet
        spreadsheet = gc.open_by_key(config["spreadsheet_id"])
        worksheet = spreadsheet.sheet1

        # Convert the data into a 2D list (rows and columns)
        headers = list(data[0].keys())  # Get headers from the first dictionary
        rows = []
        for item in data:
            row = [item[header] for header in headers]  # Create a row for each item
            # Convert the "Owned" value to a boolean
            if "Owned" in item:
                row[headers.index("Owned")] = item["Owned"].upper() == "TRUE"
            rows.append(row)

        # Update the spreadsheet with the new data
        worksheet.update(range_name=config["sheet_range"], values=rows)

        # Format the "Release Date" column as plain text
        headers = worksheet.row_values(1)  # Get headers from the first row
        release_date_col_index = headers.index("Release Date") + 1  # Convert to 1-based index
        worksheet.format(
            f"{gspread.utils.rowcol_to_a1(2, release_date_col_index)}:{gspread.utils.rowcol_to_a1(len(rows) + 1, release_date_col_index)}",
            {"numberFormat": {"type": "TEXT"}}
        )

        logging.info(f"Updated {len(data)} rows in the spreadsheet.")

    except GoogleAuthError as e:
        logging.error(f"Google Authentication Error: {e}")
    except APIError as e:
        logging.error(f"Google Sheets API Error: {e}")
    except Exception as e:
        logging.error(f"Failed to update the spreadsheet: {e}")


def main():
    """Main function to orchestrate the upload process."""
    # Load the updated data from the JSON file
    data = load_data(config.get("output_file", "output/sheet_data.json"))
    if not data:
        return  # Exit if data loading fails

    # Update the spreadsheet with the new data
    update_spreadsheet(data)


if __name__ == "__main__":
    main()