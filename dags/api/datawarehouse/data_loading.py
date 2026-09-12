import json
from datetime import date
import logging

# Create a logger for this file/module
logger = logging.getLogger(__name__)

def load_data():
    # Build file path using today's date
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        # Log that we are processing the file
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

        # Return the loaded Python object (usually a dict or list)
        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise