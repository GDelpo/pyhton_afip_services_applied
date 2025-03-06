import logging
import os

import pandas as pd
from dotenv import load_dotenv

from afip_service import AFIPService
from utils import INSCRIPTION_ERROR_KEYS, filter_dictionary, save_report_json

# Load environment variables from the .env file
load_dotenv()

# Configure logging to save logs in 'error_report.log' with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    filename="error_report.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main function that reads configuration, processes the data from AFIPService,
    accumulates errors, filters successful records, and saves JSON reports.
    """
    try:
        # Read parameters from environment variables (or use default values if
        # needed)
        username = os.getenv("AFIP_USERNAME")
        password = os.getenv("AFIP_PASSWORD")
        base_url = os.getenv("AFIP_BASE_URL")
        chunk_size = int(os.getenv("AFIP_CHUNK_SIZE"))
        max_calls = int(os.getenv("AFIP_MAX_CALLS"))
        pause_duration = int(os.getenv("AFIP_PAUSE_DURATION"))
        max_retries = int(os.getenv("AFIP_MAX_RETRIES"))
        retry_delay = int(os.getenv("AFIP_RETRY_DELAY"))
        # For 'AFIP_SERVICES_AVAILABLE', assume it's a comma-separated list in
        # the .env file
        services_available = os.getenv("AFIP_SERVICES_AVAILABLE", "").split(",")

        # Load the Excel file with NITs to consult.
        # It is assumed that the Excel file has a column named 'nro_nit' with
        # the NITs.
        excel_file = os.getenv("EXCEL_FILE_PATH")
        df = pd.read_excel(excel_file)
        # Ensure there are no missing values and convert the column to integers
        df["nro_nit"] = df["nro_nit"].fillna(0).astype(int)
        nit_list = df["nro_nit"].dropna().unique().tolist()
        # Log the total number of NITs to consult
        logger.info("Total NITs to consult: %d", len(nit_list))
    except Exception as e:
        logger.error("Error reading configuration or Excel file: %s", str(e))
        return

    # Create an instance of AFIPService with the provided parameters
    service = AFIPService(
        username=username,
        password=password,
        base_url=base_url,
        chunk_size=chunk_size,
        max_calls=max_calls,
        pause_duration=pause_duration,
        max_retries=max_retries,
        retry_delay=retry_delay,
        services_available=services_available,
    )

    # Fetch data from the AFIP service
    fetched_data = service.fetch_data_service("inscription", nit_list)
    logger.info("Data fetched: %d records", len(fetched_data))

    # Accumulate errors found in the fetched data
    data_with_error = service.accumulate_errors_in_data(
        fetched_data, INSCRIPTION_ERROR_KEYS
    )
    logger.info("Records with errors: %d", len(data_with_error))
    logger.debug("Error record keys: %s", data_with_error.keys())
    logger.debug("Fetched data keys: %s", fetched_data.keys())

    # Filter out the records with errors from the fetched data
    fetched_data = filter_dictionary(fetched_data, data_with_error)

    # Save JSON reports for both errors and successful records
    save_report_json(len(nit_list), "errors", data_with_error)
    save_report_json(len(nit_list), "success", fetched_data)

    logger.info("Process completed.")


if __name__ == "__main__":
    main()
