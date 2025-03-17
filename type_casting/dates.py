"""
dates.py: Module for handling date and time-related operations and calculations.

This module provides utilities for working with dates, including:
- Custom Year class for year validation
- Enums for months and day names
- Functions for date range calculations
- Weekly table generation
- Public holiday calculations
- Month name to number conversions
"""

from datetime import date, datetime, timedelta, time
from enum import Enum
from typing import Literal
import polars as pl
from dateutil.relativedelta import relativedelta


MonthName = Literal[
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
]

# SPECIAL_DAY: list[str] = ['Sun', 'PH']

# Maximum Time
UPPER_BOUND_SPECIAL_DAY: time = time(16, 0, 0)
UPPER_BOUND: time = time(17, 0, 0)


class Year(int):
    """
    Represents a year with validation for a specific range.
    """

    min_year: int = 2000
    max_year: int = 3000

    def __new__(cls, value):
        """
        Custom constructor to validate the year value.

        Args:
            value (int): The year value to be validated.

        Raises:
            ValueError: If the year is outside the valid range.
        """
        if not Year.min_year <= value <= Year.max_year:
            raise ValueError(f"Year must be between {Year.min_year} - {Year.max_year}")
        return super().__new__(cls, value)

    @staticmethod
    def date_range_for_a_year(year: int) -> tuple[date, date]:
        """
        Calculates the start and end date of a given year, returning a tuple containing
        (start_date, end_date) or None if the year is invalid.

        Args:
            year (Year): The year for which to calculate the date range.

        Returns:
            tuple[date, date] | None: A tuple containing the start and end date for the year,
                                    or None if the year is invalid.
        """
        start_of_year: date = date(year, 1, 1)
        end_of_year: date = date(year, 12, 31)
        return start_of_year, end_of_year


# def date_range_for_a_year(year: Year) -> tuple[date, date]:
#     """
#     Calculates the start and end date of a given year, returning a tuple containing
#     (start_date, end_date) or None if the year is invalid.

#     Args:
#         year (Year): The year for which to calculate the date range.

#     Returns:
#         tuple[date, date] | None: A tuple containing the start and end date for the year,
#                                   or None if the year is invalid.
#     """
#     if not isinstance(year, int): # to replace int with Year
#         raise TypeError(f"Year should be a Year object, not {type(year)}")
#     start_of_year: date = date(year, 1, 1)
#     end_of_year: date = date(year, 12, 31)
#     return start_of_year, end_of_year


# Constants
CURRENT_DATE: date = datetime.now().date()
CURRENT_YEAR: Year = Year(CURRENT_DATE.year)
START_OF_YEAR: date = Year.date_range_for_a_year(CURRENT_YEAR)[0]
END_OF_YEAR: date = Year.date_range_for_a_year(CURRENT_YEAR)[1]


class Month(Enum):
    """
    Enum representing months with their corresponding numbers.

    This enum provides a mapping between month names and their numerical representations.
    It also includes class methods for conversion between names and numbers.
    """

    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12

    @classmethod
    def from_name(cls, name: MonthName) -> "Month":
        """Convert a month name to a Month enum member."""
        return cls[name.upper()]

    @classmethod
    def from_number(cls, number: int) -> "Month":
        """Convert a month number to a Month enum member."""
        return cls(number)

    @classmethod
    def to_number(cls, name: MonthName) -> int:
        """Convert a month name to its corresponding number."""
        return cls[name.upper()].value


class DayName(Enum):
    """
    Enum representing day names including PH (Public Holiday).

    This enum provides a standardized way to represent days of the week
    and public holidays in the system.
    """

    PH = "PH"
    SUN = "Sun"
    MON = "Mon"
    TUE = "Tue"
    WED = "Wed"
    THU = "Thu"
    FRI = "Fri"
    SAT = "Sat"

    @classmethod
    def lists(cls) -> list[str]:
        """Return a list of all day names in order."""
        return [member.value for member in cls]


# List of day names
DAY_NAMES: list[str] = DayName.lists()

# List of special days
SPECIAL_DAYS: list[str] = [DayName.PH.value, DayName.SUN.value]


def create_weekly_table(year: Year) -> pl.DataFrame:
    """
    Creates a weekly table for a given year.

    This function takes a Year object and generates a Polars DataFrame containing
    a weekly breakdown for the year. The DataFrame includes columns for:

    - date: The date for each day within the year.
    - day_of_year: The day of the year (cumulative count starting from 1).
    - week_num: The corresponding week number for each date.
    - start_date: The starting date of the week (minimum date within the week).
    - end_date: The ending date of the week (maximum date within the week).

    Args:
        year (Year): The Year object representing the year for which to create the weekly table.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the weekly breakdown information.
    """

    start, end = Year.date_range_for_a_year(year)

    return (
        pl.datetime_range(start=start, end=end, interval="1d", eager=True)
        .dt.date()
        .alias("date")
        .to_frame()
        .with_columns(day_of_year=pl.col("date").cum_count().add(1))
        .with_columns(
            week_num=pl.when(pl.col("date").dt.weekday() == 0)
            .then(pl.col("day_of_year"))
            .otherwise(pl.col("day_of_year").floordiv(7) + 1)
            .cast(pl.Int32)
        )
        .filter(pl.col("date") >= start)
        .group_by("week_num")
        .agg(
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
        )
        .sort(by="week_num")
    )


def month_range(
    month_name: str, year: Year = Year(datetime.now().year)
) -> tuple[date, date]:
    """
    Calculates the start and end date of a given month within a specified year.

    This function takes a MonthName enum member and an optional Year object.
    If no year is provided, the current year is used.

    Args:
        month_number (MonthName): The enum member representing the month (e.g., MonthName.JANUARY).
        year (Year, optional): The Year object representing the year (defaults to current year).

    Returns:
        tuple[date, date]: A tuple containing the start and end date of the month.
    """
    month_number: int = Month.to_number(month_name)

    start_of_month = date(year, month_number, 1)
    end_of_month = start_of_month + relativedelta(months=1, days=-1)
    return (start_of_month, end_of_month)



# To move these functions to the date module
def get_monthly_range(month: str) -> int:
    """Turns a month name to it's corresponding month number"""
    return Month.to_number(month)


def get_2_months_range(month: str) -> tuple[date, date]:
    """Include previous month"""
    start = month_range(month)[0] + relativedelta(months=-1)
    end = month_range(month)[1]
    return start, end





# ---------------------------------------------------------------------------#
#                               DATE FUNCTION                               #
# ---------------------------------------------------------------------------#


def date_string_to_date(date_string: str) -> date:
    """Converts a date string in YYYY-MM-DD format to a date object."""
    return date.fromisoformat(date_string)


def stop_over_date_range(start_date: date, end_date: date) -> tuple[date, date]:
    """
    Makes a tuple of start and end date for a stop over.
    """
    return start_date, end_date


def __get_public_holidays(year: Year | int) -> list[date]:
    """
    Get a list of public holidays for a given year.

    Args:
        year (int): The year for which to get the public holidays.

    Returns:
        list[date]: A list of dates representing public holidays for the given year.
    """
    public_holidays = []

    # Add fixed public holidays
    fixed_holidays = [
        date(year, 1, 1),  # New Year's Day
        date(year, 1, 2),  # New Year's Day
        date(year, 5, 1),  # Labor Day
        date(year, 6, 18),  # Constitutional Day
        date(year, 6, 29),  # Independance Day
        date(year, 8, 15),  # Assumption Day
        date(year, 11, 1),  # All Saints Day
        date(year, 12, 8),  # Imaculate Conception
        date(year, 12, 25),  # Christmas Day
    ]
    public_holidays.extend(fixed_holidays)

    # Calculate Easter Sunday date
    a = year % 19
    b = year // 100
    c = year % 100
    d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
    f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
    month = f // 31
    day = f % 31 + 1

    easter_date = date(year, month, day)
    public_holidays.append(easter_date)  # Easter Sunday
    public_holidays.append(easter_date + timedelta(days=1))  # Easter Monday
    public_holidays.append(easter_date - timedelta(days=1))  # Holy Saturday
    public_holidays.append(easter_date - timedelta(days=2))  # Good Friday

    # Add Corpus Christi
    corpus_christi_date = easter_date + timedelta(days=60)
    public_holidays.append(corpus_christi_date)


    # Check if a fixed holiday falls on a Sunday and add the following Monday
    for holiday in fixed_holidays:
        if holiday.weekday() == 6:  # Sunday
            public_holidays.append(holiday + timedelta(days=1))

    return public_holidays


def public_holiday(year: Year | int = CURRENT_YEAR) -> pl.Series:
    """Gets the public holidays for 3 years"""
    previous_year = year - 1
    current_year = year
    next_year = year + 1

    return (
        pl.Series(__get_public_holidays(previous_year)
        + __get_public_holidays(current_year)
        + __get_public_holidays(next_year)).sort()
    )


@pl.api.register_expr_namespace('days')
class Days:
    """Creates the days name space"""
    def __init__(self,expr:pl.Expr) -> None:
        self._expr = expr

    def add_day_name(self)->pl.Expr:
        """adds a day name to a dataframe"""
        return (
        pl.when(self._expr.is_in(public_holiday()))
        .then(pl.lit("PH"))
        .otherwise(self._expr.dt.to_string(format="%a"))
    ).cast(dtype=pl.Enum(DAY_NAMES)).alias('day_name')

# ---------------------------------------------------------------------------#
#                               TIME FUNCTION                               #
# ---------------------------------------------------------------------------#
