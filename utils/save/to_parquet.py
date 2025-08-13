"""Save dataframe to parquet"""
from typing import Union
from pathlib import Path
import polars as pl


def write_parquet_if_changed(
    df: Union[pl.DataFrame,pl.LazyFrame],
    file_path: str = "./dataset/price_data.parquet",
    verbose: bool = True,
) -> bool:
    """
    Write parquet file only if data has changed.
    Returns True if file was written, False if no changes detected.
    """
    file_path = Path(file_path)

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    # Check if file exists
    if not file_path.exists():
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if verbose:
            print(f"File doesn't exist. Writing new parquet file: {file_path}")
        df.write_parquet(file_path)
        return True

    # Load existing data
    try:
        existing_df = pl.read_parquet(file_path)
    except Exception as e:
        if verbose:
            print(f"Error reading existing file: {e}. Writing new file.")
        df.write_parquet(file_path)
        return True

    # Compare DataFrames
    if df.equals(existing_df):
        if verbose:
            print("No changes detected. Skipping parquet write.")
        return False
    else:
        if verbose:
            print("Changes detected. Updating parquet file.")
        df.write_parquet(file_path)
        return True
