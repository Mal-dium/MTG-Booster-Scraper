import gspread
from google.oauth2 import service_account
from google.auth.exceptions import GoogleAuthError
import json
import os
import logging
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env file
load_dotenv()

# Load configuration from a JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Access environment variables
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", config["service_account_file"])
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", config["spreadsheet_id"])


def handle_error(error_message: str, log_level: str = "error") -> None:
    """
    Log an error message and return None.

    Args:
        error_message (str): The error message to log.
        log_level (str): The log level ('error', 'warning', etc.).
    """
    if log_level == "error":
        logging.error(error_message)
    elif log_level == "warning":
        logging.warning(error_message)
    else:
        logging.info(error_message)
    return None


def pull_sheet() -> Optional[List[Dict[str, str]]]:
    """
    Pull sheet info from Google Sheets and save to a JSON file.
    Returns the data as a list of dictionaries if successful, otherwise None.
    """
    try:
        # Authenticate with Google Sheets API
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)

        # Open the spreadsheet and worksheet
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.sheet1

        # Get the first row as headers
        headers = worksheet.row_values(1)  # Assumes headers are in the first row

        # Get all data rows (excluding the header row)
        data = worksheet.get_all_values()[1:]  # Skip the first row (headers)

        # Organize the data into a list of dictionaries
        rows = [
            {headers[i]: row[i] for i in range(len(row))}
            for row in data if len(row) >= len(headers)  # Ensure each row has enough columns
        ]

        # Write the data to a JSON file
        with open("sheet_data.json", "w") as json_file:
            json.dump(rows, json_file, indent=4)

        logging.info(f"Data saved to 'sheet_data.json' with {len(rows)} entries.")
        return rows

    except GoogleAuthError as e:
        return handle_error(f"Google Authentication Error: {e}")
    except gspread.exceptions.APIError as e:
        return handle_error(f"Google Sheets API Error: {e}")
    except Exception as e:
        return handle_error(f"Unexpected Error: {e}")


if __name__ == "__main__":
    pull_sheet()