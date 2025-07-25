"""Make the dataset (LazyFrame) from a google sheet id and sheet names with async support"""
import logging
import asyncio
from io import StringIO
from typing import Dict
import aiohttp
import polars as pl

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Cache for loaded datasets to avoid redundant requests
data_cache: Dict[str, pl.LazyFrame] = {}

async def load_gsheet_data(sheet_id: str, sheet_name: str) -> pl.LazyFrame:
    """
    Loads a Google Sheet as a Polars LazyFrame asynchronously.
    Args:
        sheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet to load.
    Returns:
        pl.LazyFrame: A LazyFrame containing the sheet data,
        or empty LazyFrame if an error occurred.
    """
    # Create a cache key
    cache_key = f"{sheet_id}_{sheet_name}"

    # Check if data is already in cache
    if cache_key in data_cache:
        logger.info("Using cached data for %s", sheet_name)
        return data_cache[cache_key]

    link: str = "https://docs.google.com/spreadsheets"
    url: str = f"{link}/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as response:
                response.raise_for_status()
                csv_data = StringIO(await response.text())
                result = pl.read_csv(csv_data).lazy() #  try_parse_dates=True
                # Cache the result for future use
                data_cache[cache_key] = result
                return result
    except aiohttp.ClientError as e:
        logger.error(
            "An error occurred while trying to access the Google Sheet: %s", e
        )
        return pl.LazyFrame()
    except pl.exceptions.ComputeError as e:
        logger.error("An error occurred while parsing the CSV data: %s", e)
        return pl.LazyFrame()
    except Exception as e:
        logger.error("Unexpected error in load_gsheet_data: %s", e)
        return pl.LazyFrame()

# Batch load multiple sheets concurrently
async def load_multiple_sheets_async(sheets_info: list[tuple[str, str]]) -> dict[str, pl.LazyFrame]:
    """
    Load multiple sheets concurrently.
    Args:
        sheets_info: List of tuples containing (sheet_id, sheet_name)
    Returns:
        Dictionary mapping sheet names to their LazyFrames
    """
    tasks = [load_gsheet_data(sheet_id, sheet_name) for sheet_id, sheet_name in sheets_info]
    results = await asyncio.gather(*tasks)
    return {sheet_name: result for (_, sheet_name), result in zip(sheets_info, results)}

# Wrapper to ensure we don't return a coroutine when the function is used in the dataframes dictionary
def get_sheet_data(sheet_id: str, sheet_name: str):
    """
    Non-async wrapper to use in dataframe dictionaries.
    Instead of returning the LazyFrame directly, returns a function that when called,
    returns a coroutine that will resolve to the LazyFrame.
    
    This pattern allows your existing save system to correctly detect and await the coroutine.
    """
    # Return a function that when called, returns the coroutine
    return lambda: load_gsheet_data(sheet_id, sheet_name)

# Clear cache function
def clear_data_cache() -> None:
    """Clear the data cache to force reload of data"""
    global data_cache
    data_cache.clear()
    logger.info("Data cache cleared")

# For backwards compatibility
async def load_gsheet_data_async_wrapper(sheet_id: str, sheet_name: str) -> pl.LazyFrame:
    """Async wrapper for backward compatibility with synchronous code"""
    return await load_gsheet_data(sheet_id, sheet_name)
