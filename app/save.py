# 

"""Async Save to CSV module with optimizations"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, Dict, List, Any
import polars as pl
from app.logger import logger

# Import dataframes using relative imports to avoid circular dependencies
from all_dataframes.all_dataframes import (
    emr_dataframes,
    # netlist_dataframes,
    # bin_dispatch_dataframes,
    # shore_handling_dataframes,
    miscellaneous_dataframes,
    operations_dataframes,
    stuffing_dataframes,
    transport_dataframes,
)

# Create a thread pool executor for I/O bound operations
# Using a smaller number to avoid overwhelming the system
executor = ThreadPoolExecutor(max_workers=5)

# Type alias for readability
DataframeInfo = Tuple[str, pl.LazyFrame]
SaveResult = Tuple[str, Optional[Exception]]
DfCollection = Dict[str, pl.LazyFrame]

async def save_to_csv_async(dataframe_info: DataframeInfo) -> SaveResult:
    """Process the dataframes to CSV file asynchronously
    
    Args:
        dataframe_info: Tuple containing the dataframe name and dataframe
        
    Returns:
        tuple: Tuple containing the dataframe name and error if any
    """
    
    if not isinstance(dataframe_info, tuple) or len(dataframe_info) != 2:
        return "Invalid data format", TypeError("Invalid data format")

    dataframe_name, dataframe = dataframe_info

    try:
        # Add explicit path and ensure directory exists
        output_path = fr"P:\Verification & Invoicing\Validation Report\csv\{dataframe_name}.csv"
        
        # Ensure directory exists
        directory = os.path.dirname(output_path)
        os.makedirs(directory, exist_ok=True)
        
        # Optimize the write operation for large dataframes
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            executor, 
            lambda: dataframe.collect().write_csv(output_path)
        )
        
        logger.info("Successfully wrote %s to file", dataframe_name)
        return dataframe_name, None
    except (FileExistsError, FileNotFoundError) as e:
        error_msg = f"Error writing {dataframe_name}: {str(e)}"
        logger.error(error_msg)
        return dataframe_name, e
    except Exception as e:
        error_msg = f"Unexpected error writing {dataframe_name}: {str(e)}"
        logger.error(error_msg)
        return dataframe_name, e

async def save_df_to_csv_async(dataframes: Optional[str] = None) -> None:
    """
    Save the dataframes asynchronously with improved error handling
    
    Args:
        dataframes: Category of dataframes to save ('all' or specific category name)
    """
    df_dict: Dict[str, DfCollection] = {
        "emr": emr_dataframes,
        "operations": operations_dataframes,
        # "netlist": netlist_dataframes,
        # "bin_dispatch": bin_dispatch_dataframes,
        # "shore_handling": shore_handling_dataframes,
        "stuffing": stuffing_dataframes,
        "transport": transport_dataframes,
        "miscellaneous": miscellaneous_dataframes,
    }

    if dataframes == "all":
        # Process all dictionaries concurrently
        logger.info("Processing all dataframe categories concurrently")
        
        all_tasks = []
        for category, category_dfs in df_dict.items():
            logger.info("Queueing category: %s", category)
            for name, df in category_dfs.items():
                all_tasks.append(save_to_csv_async((name, df)))
        
        # Execute all tasks concurrently with a reasonable concurrency limit
        # Using asyncio.Semaphore to limit concurrency and avoid overwhelming the system
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent operations
        
        async def bounded_save(task):
            async with semaphore:
                return await task
        
        bounded_tasks = [bounded_save(task) for task in all_tasks]
        results = await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        # Process results
        successes = []
        failures = []
        
        for result in results:
            if isinstance(result, Exception):
                logger.error("Task raised exception: %s", str(result))
                continue
                        
            message, error = result
            if error:
                failures.append((message, error))
                logger.error("Error saving %s: %s", message, str(error))
            else:
                successes.append(message)
                logger.info("Successfully saved %s", message)

        # Summary
        logger.info("Save completed")
        logger.info("Successfully saved: %d files", len(successes))
        if failures:
            logger.error("Failed to save: %d files", len(failures))
            for name, err in failures:
                logger.error("  - %s: %s", name, str(err))
                
        return

    # Handle single category case
    data = df_dict.get(dataframes)
    
    if data is None:
        logger.error("Invalid dataframe option: %s", dataframes)
        return
    
    logger.info("Processing dataframe category: %s", dataframes)
    logger.info("Processing dataframes: %s", list(data.keys()))

    # Process each dataframe concurrently
    tasks = [save_to_csv_async((name, df)) for name, df in data.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    successes = []
    failures = []
    
    for result in results:
        if isinstance(result, Exception):
            logger.error("Task raised exception: %s", str(result))
            continue
                    
        message, error = result
        if error:
            failures.append((message, error))
            logger.error("Error saving %s: %s", message, str(error))
        else:
            successes.append(message)
            logger.info("Successfully saved %s", message)

    # Summary
    logger.info("Save completed")
    logger.info("Successfully saved: %s", ", ".join(successes))
    if failures:
        logger.error("Failed to save: %s", ", ".join(f"{name}: {str(err)}" for name, err in failures))