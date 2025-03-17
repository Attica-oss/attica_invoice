"print dataframe in a beautiful format"
import polars as pl

# Constants
DATAFRAME_LEN_THRESHOLD: int = 50
MIN_LEN_DATAFRAME: int = 30
STRING_LENGHT: int = 500
CHAR_WIDTH: int = 1000



def print_dataframe(
    dataframe: pl.DataFrame,
    max_lenght: int = DATAFRAME_LEN_THRESHOLD,
    min_lenght: int = MIN_LEN_DATAFRAME,
    string_lenght: int = STRING_LENGHT,
    char_width: int = CHAR_WIDTH,
) -> None:
    """
    Prints a DataFrame with formatted output, handling large DataFrames.

    This function takes a polars DataFrame as input and prints it to the console
    in a user-friendly format. It addresses potential issues with excessively
    wide DataFrames by:

    - Setting the column formatting string lengths to 500 characters to
      accommodate longer column names or values.
    - Automatically adjusting the table width to 1000 characters, ensuring
      most DataFrames fit comfortably on the screen.
    - Limiting the number of displayed rows to 30 for very large DataFrames
      (more than 50 rows) to avoid overwhelming the console. In such cases,
      the function displays the first 30 rows and a message indicating that
      the DataFrame has been truncated.

    Args:
        dataframe (pl.DataFrame): The polars DataFrame to be printed.
        max_length (int): Maximum number of rows before truncation.
        min_length (int): Number of rows to display if truncation occurs.
        string_length (int): Maximum length of strings in the DataFrame.
        char_width (int): Width of the table in characters.

    Returns:
        None

    Example Usage:
        >>> df = pl.DataFrame({
        ...    "col1": range(5),
        ...    "col2": ["example"] * 5,
        ... })
        >>> print_dataframe(df)
        shape: (5, 2)
        ┌──────┬─────────┐
        │ col1 ┆ col2    │
        │ ---  ┆ ---     │
        │ i64  ┆ str     │
        ╞══════╪═════════╡
        │ 0    ┆ example │
        │ 1    ┆ example │
        │ 2    ┆ example │
        │ 3    ┆ example │
        │ 4    ┆ example │
        └──────┴─────────┘
    """
    if dataframe.shape[0] > max_lenght:
        value = min_lenght
    value = dataframe.shape[0]

    with pl.Config(fmt_str_lengths=string_lenght) as cfg:
        cfg.set_tbl_width_chars(char_width)
        cfg.set_tbl_cols(dataframe.shape[1])
        cfg.set_tbl_rows(value)
        print(dataframe)
