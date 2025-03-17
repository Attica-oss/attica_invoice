"""Saves the dataframe to specified format
    Currently csv, excel (xlsx), parquet and sql(db) are supported.
"""
from enum import Enum
from pathlib import Path
import polars as pl
from sqlalchemy import create_engine





# File format
class FileFormat(Enum):
    """File format currently supported"""
    CSV:str = "csv"
    PARQUET:str = "parquet"
    EXCEL:str = "excel"
    SQL:str = "sql"

    @classmethod
    def get_all_file_formats(cls)->list[str]:
        """creates a list of file
        formats for validation check"""
        return [file_format.value for file_format in cls]

# Folder name
def folder_paths_name(file_format:FileFormat)->Path:
    """Provide the location to save file
        Args:
            file_format(FileFormat): the supported formats.
        Returns:
            Path: The path to the file on the OS
    """
    # Windows path
    path: Path = (
        fr"P:\Verification & Invoicing\Validation Report\{file_format}"
    )
    return path


def get_folder_path(file_format: FileFormat)->Path:
    """Determines the correct folder path based on the operating system and file format.

    Args:
        file_format (FileFormat): The file format (either "csv", "excel","parquet" or "sql").

    Raises:
        ValueError: If the provided file format is invalid.

    Returns:
        Path: The path to the desired folder.
    """
    return folder_paths_name(file_format=file_format)

# The various locations
FOLDER_PATH_CSV: Path = get_folder_path(file_format=FileFormat.CSV.value)
FOLDER_PATH_PARQUET: Path = get_folder_path(file_format=FileFormat.PARQUET.value)
FOLDER_PATH_EXCEL: Path = get_folder_path(file_format=FileFormat.EXCEL.value)
FOLDER_PATH_SQL: Path = get_folder_path(file_format=FileFormat.SQL.value)


def save_lazyframe_to_csv(
    ldf: pl.LazyFrame, file_name: str, path: Path = FOLDER_PATH_CSV
) -> None:
    """
    Saves lazyframe to csv

    Args:
        ldf (pl.LazyFrame): The lazyframe to filter and save.
        file_name (str): The name of the file to create.
        path (str): The path where the CSV will be saved.
    """
    try:
        ldf.collect().write_csv(Path(path)/ f"{file_name}.csv", separator=",")
        print(f"{file_name} {msg.DataFrameMessage.csv}")
    except FileNotFoundError:
        print(f"File  {file_name} not found")

def save_lazyframe_to_sql(
    ldf: pl.LazyFrame, file_name: str, path: Path = FOLDER_PATH_SQL
) -> None:
    """
    Saves lazyframe to csv

    Args:
        ldf (pl.LazyFrame): The lazyframe to filter and save.
        file_name (str): The name of the file to create.
        path (str): The path where the sql will be saved.
    """
    uri = create_engine(f"sqlite:///{Path(path)}/test.db")

    try:
        ldf.collect().write_database(
            table_name=f"{file_name}",
            connection= uri,
            if_table_exists="append"
            )
        print(f"{file_name} {msg.DataFrameMessage.csv}")
    except FileNotFoundError:
        print(f"File  {file_name} not found")


def save_lazyframe_to_parquet(
    lazyframe: pl.LazyFrame,
    file_name: str,
    path: Path = FOLDER_PATH_PARQUET,
) -> None:
    """Saves lazyframe to parquet

    Args:
        lazyframe (pl.LazyFrame): The lazyframe to filter and save.
        file_name (str): The name of the file to create.
        path (Path): The path where the CSV will be saved."""
    try:
        lazyframe.collect().write_parquet(
            Path(path / f"{file_name}.parquet")
        )
        print(f"{file_name} {msg.DataFrameMessage.parquet}")  # to change this
    except FileNotFoundError:
        print(f"File  {file_name} not found")


def save_lazyframe_to_excel(
    lazyframe: pl.LazyFrame,
    file_name: str,
    path: Path = FOLDER_PATH_EXCEL,
) -> None:
    """Saves lazyframe to Excel

    Args:
        lazyframe (pl.LazyFrame): The lazyframe to filter and save.
        file_name (str): The name of the file to create.
        path (Path): The path where the Excel will be saved."""
    try:
        lazyframe.collect().write_excel(
            Path(path) / f"{file_name}.xlsx"
        )
        print(f"{file_name} {msg.DataFrameMessage.excel}")  # to change this
    except FileNotFoundError:
        print(f"File  {file_name} not found")
