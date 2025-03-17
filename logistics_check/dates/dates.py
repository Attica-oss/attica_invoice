"""dates functions"""

from datetime import date,datetime
import polars as pl

YEAR: int = datetime.now().year


def get_month_table(year: int) -> pl.DataFrame:
    """generate a month table"""
    start_date: list[date] = []
    for i in range(1, 13):
        _date = date(year, i, 1)
        start_date.append(_date)
    return pl.DataFrame({"start_date": start_date}).with_columns(
        end_date=pl.col("start_date").dt.month_end()
    )


def month_number_to_dates(month_num: int,year:int=YEAR) -> tuple[date, date]:
    """converts month number to dates"""
    df = (
        get_month_table(year)
        .with_columns(month=pl.col("end_date").dt.month())
        .filter(pl.col("month").eq(month_num))
        .select(pl.all().exclude(["month"]))
    )
    return (
        df.select(pl.col("start_date")).to_series().to_list()[0],
        df.select(pl.col("end_date")).to_series().to_list()[0],
    )
