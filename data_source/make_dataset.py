"""Make the dataset (LazyFrame) from a google sheet id and sheet names"""

import logging
from io import StringIO
import requests
import polars as pl


import aiohttp

# Configure logging
logging.basicConfig(level=logging.ERROR)

# To test Async
async def load_gsheet_data_async(sheet_id: str, sheet_name: str) -> pl.LazyFrame:
    """
    Loads a Google Sheet as a Polars LazyFrame asynchronously.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet to load.

    Returns:
        pl.LazyFrame: A LazyFrame containing the sheet data, or None if an error occurred.
    """
    link:str = "https://docs.google.com/spreadsheets"
    url:str = f"{link}/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                csv_data = StringIO(await response.text())
                return pl.read_csv(csv_data, try_parse_dates=True).lazy()

    except aiohttp.ClientError as e:
        logging.error(
            "An error occurred while trying to access the Google Sheet: %s", e
        )
        return pl.LazyFrame()
    except pl.exceptions.ComputeError as e:
        logging.error("An error occurred while parsing the CSV data: %s", e)
        return pl.LazyFrame()





def load_gsheet_data(sheet_id: str, sheet_name: str) -> pl.LazyFrame:
    """
    Loads a Google Sheet as a Polars LazyFrame.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet to load.

    Returns:
        pl.LazyFrame: A LazyFrame containing the sheet data, or None if an error occurred.
    """
    link:str = "https://docs.google.com/spreadsheets"
    url:str = f"{link}/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Use StringIO to avoid creating a temporary file
        csv_data = StringIO(response.text)
        return pl.read_csv(csv_data, try_parse_dates=True).lazy()

    except requests.exceptions.RequestException as e:
        logging.error(
            "An error occurred while trying to access the Google Sheet: %s", e
        )
        return pl.LazyFrame()
    except pl.exceptions.ComputeError as e:
        logging.error("An error occurred while parsing the CSV data: %s", e)
        return pl.LazyFrame()
