"""Async Save to CSV module with optimizations"""

import asyncio
import os
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, Dict, Any, Callable, Coroutine, Union
import polars as pl
from app.logger import logger

# Import dataframes using relative imports to avoid circular dependencies
from all_dataframes.all_dataframes import all_dataframes

# Create a thread pool executor for I/O bound operations
# Using a smaller number to avoid overwhelming the system
executor = ThreadPoolExecutor(max_workers=5)

# Type alias for readability
DataframeInfo = Tuple[str, Any]  # Can be a function, coroutine, or LazyFrame
SaveResult = Tuple[str, Optional[Exception]]
DfCollection = Dict[str, Any]  # Function, coroutine, or LazyFrame


def write_df_to_csv(df_to_write: pl.DataFrame, path: str) -> None:
    """
    Helper function to write a collected dataframe to CSV.
    
    Args:
        df_to_write: A Polars DataFrame (already collected)
        path: Path where to save the CSV
    """
    try:
        # Debug info
        logger.info(f"Writing to {path}, df type: {type(df_to_write)}")
        
        # Write to CSV
        df_to_write.write_csv(path)
    except Exception as e:
        logger.error(f"Error in write_df_to_csv: {e}")
        raise


async def call_async_function(df_function, name: str):
    """
    Add a workaround for async functions that don't await anything internally
    
    Args:
        df_function: An async function that returns a dataframe
        name: Name of the dataframe for logging
        
    Returns:
        The actual Polars dataframe
    """
    try:
        # Call the function to get the coroutine
        coro = df_function()
        
        # This is a special hack:
        # For coroutines that don't await anything internally,
        # we need to access the cr_frame.f_locals to get values from the function
        if hasattr(coro, 'cr_frame') and hasattr(coro.cr_frame, 'f_locals'):
            # Let's check if there's a 'result' or 'df' or similar variable in locals
            locals_dict = coro.cr_frame.f_locals
            logger.info(f"Coroutine locals for {name}: {list(locals_dict.keys())}")
            
            # Look for common dataframe variable names
            for var_name in ['df', 'result', 'dataframe', 'data', 'lazy_df', 'lf']:
                if var_name in locals_dict:
                    logger.info(f"Found '{var_name}' in coroutine locals for {name}")
                    return locals_dict[var_name]
            
            # If we didn't find a known variable, see if we can identify a dataframe object
            for key, value in locals_dict.items():
                if isinstance(value, pl.LazyFrame) or isinstance(value, pl.DataFrame):
                    logger.info(f"Found dataframe in variable '{key}' for {name}")
                    return value
        
        # If we couldn't extract from locals, try to await the coroutine normally
        logger.info(f"Attempting to await coroutine for {name}")
        return await coro
    except Exception as e:
        logger.error(f"Error in call_async_function for {name}: {str(e)}")
        raise


async def get_actual_df(df_function_or_object, name: str):
    """
    Helper function to handle various ways a dataframe might be provided.
    
    Args:
        df_function_or_object: Could be a function, coroutine, or dataframe
        name: Name of the dataframe for logging
        
    Returns:
        The actual Polars dataframe
    """
    try:
        # If it's a callable, we need to determine if it's a regular function or a coroutine function
        if callable(df_function_or_object):
            logger.info("Callable found for %s, checking if it's a coroutine function", name)
            
            # Check if it's a coroutine function (async function)
            if inspect.iscoroutinefunction(df_function_or_object):
                logger.info("%s is a coroutine function, using special handling", name)
                
                # Try our special handler for async functions
                return await call_async_function(df_function_or_object, name)
            else:
                # It's a regular function
                logger.info("%s is a regular function, calling it", name)
                result = df_function_or_object()
                
                # Still need to check if this returned a coroutine
                if inspect.iscoroutine(result):
                    logger.info("Regular function for %s returned a coroutine, awaiting it", name)
                    result = await result
                
                return result
        else:
            # It's not a function, so it should be a dataframe already
            logger.info("%s is not a function, assuming it's a dataframe already", name)
            return df_function_or_object
    except Exception as e:
        logger.error("Error in get_actual_df for %s: %s", name, str(e))
        raise




async def save_to_csv_async(dataframe_info: DataframeInfo) -> SaveResult:
    """Process the dataframes to CSV file asynchronously"""
    if not isinstance(dataframe_info, tuple) or len(dataframe_info) != 2:
        return "Invalid data format", TypeError("Invalid data format")

    dataframe_name, dataframe_function = dataframe_info

    try:
        # Add explicit path and ensure directory exists
        output_path = f"output/csv/{dataframe_name}.csv"
        directory = os.path.dirname(output_path)
        os.makedirs(directory, exist_ok=True)

        logger.info("Processing dataframe %s of type %s", dataframe_name, type(dataframe_function))

        try:
            # If it's a callable, call it and await the result
            if callable(dataframe_function):
                logger.info("Calling function for %s", dataframe_name)
                df_result = dataframe_function()
                
                # If the result is awaitable, await it
                if inspect.isawaitable(df_result):
                    logger.info("Awaiting result for %s", dataframe_name)
                    actual_dataframe = await df_result
                else:
                    actual_dataframe = df_result
            else:
                actual_dataframe = dataframe_function

            # Now we should have the actual dataframe - check if it needs to be collected
            if hasattr(actual_dataframe, 'collect'):
                logger.info("Collecting LazyFrame for %s", dataframe_name)
                collected_df = actual_dataframe.collect()
            else:
                collected_df = actual_dataframe
                
            logger.info("Successfully processed dataframe %s", dataframe_name)
            
            # Now run the CSV writing in the executor
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                executor, 
                write_df_to_csv,
                collected_df,
                output_path
            )
            
            logger.info("Successfully wrote %s to file", dataframe_name)
            return dataframe_name, None
            
        except Exception as e:
            logger.error("Error processing dataframe %s: %s", dataframe_name, str(e))
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

    all_df = await all_dataframes()

    df_dict: Dict[str, DfCollection] = {
        "emr": all_df.get("emr_dataframes"),
        "operations": all_df.get("operations_dataframes"),
        "netlist": all_df.get("netlist_dataframes"),
        "bin_dispatch": all_df.get("bin_dispatch_dataframes"),
        "shore_handling": all_df.get("shore_handling_dataframes"),
        "stuffing": all_df.get("stuffing_dataframes"),
        "transport": all_df.get("transport_dataframes"),
        "miscellaneous": all_df.get("miscellaneous_dataframes"),
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
        logger.error(
            "Failed to save: %s",
            ", ".join(f"{name}: {str(err)}" for name, err in failures),
        )