"""
dates.py: Module for handling date and time-related operations and calculations.

This module provides utilities for working with dates, including:
- Custom Year class for year validation
- Enums for months and day names
- Date range calculations for various time periods
- Business day and holiday functionality
- Month name to number conversions
- Polars integration for date manipulations in dataframes
"""

from datetime import date, datetime, timedelta, time
from enum import Enum
from typing import Literal, List, Optional, Union, Tuple
import polars as pl
from dateutil.relativedelta import relativedelta


# Type definitions
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
DateRange = Tuple[date, date]

# Time constants
UPPER_BOUND_SPECIAL_DAY: time = time(16, 0, 0)
UPPER_BOUND: time = time(17, 0, 0)
MORNING_CUTOFF: time = time(7, 59, 0)
NULL_DURATION = pl.duration(hours=0, minutes=0, seconds=0)


def duration_to_hhmm(
    df: Union[pl.DataFrame, pl.LazyFrame],
    duration_columns: Union[str, List[str]] = None,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Convert duration columns to HH:MM string format.

    Args:
        df: The DataFrame or LazyFrame to process
        duration_columns: Column names containing duration data. If None, automatically detects
                          all duration columns in the DataFrame.

    Returns:
        DataFrame or LazyFrame with duration columns converted to HH:MM format
    """
    # Determine if we're working with a DataFrame or LazyFrame
    is_lazy = isinstance(df, pl.LazyFrame)
    schema = df.schema if not is_lazy else df.collect_schema()

    # If no duration columns specified, detect them automatically
    if duration_columns is None:
        duration_columns = [
            col_name
            for col_name, dtype in schema.items()
            if str(dtype).startswith("duration")
        ]
    elif isinstance(duration_columns, str):
        duration_columns = [duration_columns]

    # No duration columns to convert
    if not duration_columns:
        return df

    # Define a function to format durations as HH:MM
    def format_as_hhmm(s: pl.Series) -> pl.Series:
        return s.map_elements(
            lambda d: f"{int(d.total_seconds() // 3600):02d}:{int((d.total_seconds() % 3600) // 60):02d}"
            if d is not None
            else None,
            return_dtype=pl.Utf8,
        )

    # Apply the conversion to each duration column
    for col in duration_columns:
        df = df.with_columns(format_as_hhmm(pl.col(col)).alias(col))

    return df


class Year(int):
    """
    Represents a year with validation for a specific range.

    This class extends the built-in int type to add validation for years
    within a specified range, as well as utility methods for working with year-based
    date ranges.
    """

    min_year: int = 2000
    max_year: int = 3000

    def __new__(cls, value: int) -> "Year":
        """
        Custom constructor to validate the year value.

        Args:
            value (int): The year value to be validated.

        Raises:
            ValueError: If the year is outside the valid range.

        Returns:
            Year: The validated Year instance.
        """
        if not cls.min_year <= value <= cls.max_year:
            raise ValueError(f"Year must be between {cls.min_year} - {cls.max_year}")
        return super().__new__(cls, value)

    @classmethod
    def date_range(cls, year: int) -> DateRange:
        """
        Calculates the start and end date of a given year.

        Args:
            year (int): The year for which to calculate the date range.

        Returns:
            DateRange: A tuple containing the start and end date for the year.
        """
        start_of_year: date = date(year, 1, 1)
        end_of_year: date = date(year, 12, 31)
        return start_of_year, end_of_year

    @classmethod
    def fiscal_year_range(cls, year: int, start_month: int = 4) -> DateRange:
        """
        Calculates the start and end date of a fiscal year.

        Args:
            year (int): The fiscal year
            start_month (int, optional):
                The starting month of the fiscal year. Defaults to 4 (April).

        Returns:
            DateRange: A tuple containing the start and end date for the fiscal year.
        """
        start_of_fiscal_year = date(year, start_month, 1)
        end_of_fiscal_year = date(year + 1, start_month, 1) - timedelta(days=1)
        return start_of_fiscal_year, end_of_fiscal_year

    @classmethod
    def current(cls) -> "Year":
        """
        Returns the current year as a Year object.

        Returns:
            Year: The current year.
        """
        return cls(datetime.now().year)

    def quarter_range(self, quarter: int) -> DateRange:
        """
        Calculates the date range for a specific quarter in this year.

        Args:
            quarter (int): The quarter number (1-4)

        Returns:
            DateRange: A tuple containing the start and end dates for the quarter

        Raises:
            ValueError: If the quarter is not between 1 and 4
        """
        if not 1 <= quarter <= 4:
            raise ValueError("Quarter must be between 1 and 4")

        start_month = (quarter - 1) * 3 + 1
        start_date = date(int(self), start_month, 1)

        if quarter == 4:
            end_date = date(int(self), 12, 31)
        else:
            end_month = quarter * 3
            end_date = date(int(self), end_month + 1, 1) - timedelta(days=1)

        return start_date, end_date


# Date constants
CURRENT_DATE: date = datetime.now().date()
CURRENT_YEAR: Year = Year(CURRENT_DATE.year)
START_OF_YEAR: date = Year.date_range(CURRENT_YEAR)[0]
END_OF_YEAR: date = Year.date_range(CURRENT_YEAR)[1]


class Month(Enum):
    """
    Enum representing months with their corresponding numbers.

    This enum provides a mapping between month names and their numerical representations,
    along with utility methods for conversion and date range calculations.
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
        """
        Convert a month name to a Month enum member.

        Args:
            name (MonthName): The name of the month

        Returns:
            Month: The corresponding Month enum member
        """
        return cls[name.upper()]

    @classmethod
    def from_number(cls, number: int) -> "Month":
        """
        Convert a month number to a Month enum member.

        Args:
            number (int): The month number (1-12)

        Returns:
            Month: The corresponding Month enum member

        Raises:
            ValueError: If the number is not between 1 and 12
        """
        if not 1 <= number <= 12:
            raise ValueError("Month number must be between 1 and 12")
        return cls(number)

    @classmethod
    def to_number(cls, name: MonthName) -> int:
        """
        Convert a month name to its corresponding number.

        Args:
            name (MonthName): The name of the month

        Returns:
            int: The corresponding month number (1-12)
        """
        return cls[name.upper()].value

    @classmethod
    def get_all_names(cls) -> List[str]:
        """
        Get a list of all month names.

        Returns:
            List[str]: List of all month names
        """
        return [member.name for member in cls]

    @classmethod
    def get_all_numbers(cls) -> List[int]:
        """
        Get a list of all month numbers.

        Returns:
            List[int]: List of all month numbers (1-12)
        """
        return [member.value for member in cls]

    def date_range(self, year: Union[Year, int]) -> DateRange:
        """
        Get the date range for this month in the specified year.

        Args:
            year (Union[Year, int]): The year

        Returns:
            DateRange: A tuple containing the start and end dates for the month
        """
        year_int = int(year)
        start_date = date(year_int, self.value, 1)

        if self.value == 12:
            end_date = date(year_int, 12, 31)
        else:
            end_date = date(year_int, self.value + 1, 1) - timedelta(days=1)

        return start_date, end_date

    def next_month(self) -> "Month":
        """
        Get the next month after this one.

        Returns:
            Month: The next month
        """
        return Month.from_number(1 if self.value == 12 else self.value + 1)

    def previous_month(self) -> "Month":
        """
        Get the previous month before this one.

        Returns:
            Month: The previous month
        """
        return Month.from_number(12 if self.value == 1 else self.value - 1)


class DayName(Enum):
    """
    Enum representing day names including PH (Public Holiday).

    This enum provides a standardized way to represent days of the week
    and public holidays in the system, along with utility methods for
    working with day names and holiday calculations.
    """

    PH = "PH"  # Public Holiday
    OT = "OT"  # Overtime :: Only used case is in the Operations Activity
    MON = "Mon"
    TUE = "Tue"
    WED = "Wed"
    THU = "Thu"
    FRI = "Fri"
    SAT = "Sat"
    SUN = "Sun"

    @classmethod
    def get_all(cls) -> List[str]:
        """
        Return a list of all day names including public holiday.

        Returns:
            List[str]: All day names including PH
        """
        return [member.value for member in cls]

    @classmethod
    def get_calendar_days(cls) -> List[str]:
        """
        Return a list of all regular calendar day names (excluding PH).

        Returns:
            List[str]: All regular calendar day names
        """
        return [member.value for member in cls if member != cls.PH]

    @classmethod
    def get_special_days(cls) -> List[str]:
        """
        Return only the special days (PH and Sunday).

        Returns:
            List[str]: Special day names
        """
        return [cls.PH.value, cls.SUN.value]

    @classmethod
    def is_weekend(cls, day: str) -> bool:
        """
        Check if the given day is a weekend day.

        Args:
            day (str): The day name to check

        Returns:
            bool: True if the day is a weekend day, False otherwise
        """
        return day in [cls.SAT.value, cls.SUN.value]

    @classmethod
    def is_special_day(cls, day: str) -> bool:
        """
        Check if the given day is a special day (weekend or holiday).

        Args:
            day (str): The day name to check

        Returns:
            bool: True if the day is a special day, False otherwise
        """
        return day in [cls.PH.value, cls.SUN.value]

    @classmethod
    def get_weekdays(cls) -> List[str]:
        """
        Return a list of weekday names (Monday through Friday).

        Returns:
            List[str]: List of weekday names
        """
        return [
            member.value for member in [cls.MON, cls.TUE, cls.WED, cls.THU, cls.FRI]
        ]

    @staticmethod
    def __get_public_holidays(year: Union[Year, int]) -> List[date]:
        """
        Get a list of public holidays for a given year.

        Args:
            year (Union[Year, int]): The year for which to get the public holidays.

        Returns:
            List[date]: A list of dates representing public holidays for the given year.
        """
        year_int = int(year)
        public_holidays = []

        # Add fixed public holidays
        fixed_holidays = [
            date(year_int, 1, 1),  # New Year's Day
            date(year_int, 1, 2),  # New Year's Day (Second day)
            date(year_int, 5, 1),  # Labor Day
            date(year_int, 6, 18),  # Constitutional Day
            date(year_int, 6, 29),  # Independence Day
            date(year_int, 8, 15),  # Assumption Day
            date(year_int, 11, 1),  # All Saints Day
            date(year_int, 12, 8),  # Immaculate Conception
            date(year_int, 12, 25),  # Christmas Day
        ]
        public_holidays.extend(fixed_holidays)

        # Calculate Easter Sunday date
        a = year_int % 19
        b = year_int // 100
        c = year_int % 100
        d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
        e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
        f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
        month = f // 31
        day = f % 31 + 1

        easter_date = date(year_int, month, day)
        public_holidays.append(easter_date)  # Easter Sunday
        public_holidays.append(easter_date + timedelta(days=1))  # Easter Monday
        public_holidays.append(easter_date - timedelta(days=1))  # Holy Saturday
        public_holidays.append(easter_date - timedelta(days=2))  # Good Friday

        # Add Corpus Christi (60 days after Easter)
        corpus_christi_date = easter_date + timedelta(days=60)
        public_holidays.append(corpus_christi_date)

        # Check if a fixed holiday falls on a Sunday and add the following Monday
        for holiday in fixed_holidays:
            if holiday.weekday() == 6:  # Sunday
                public_holidays.append(holiday + timedelta(days=1))

        return sorted(public_holidays)

    @classmethod
    def get_public_holidays(cls, year: Union[Year, int] = CURRENT_YEAR) -> List[date]:
        """
        Get a list of public holidays for a given year.

        Args:
            year (Union[Year, int], optional): The year. Defaults to current year.

        Returns:
            List[date]: List of public holiday dates for the specified year
        """
        return cls.__get_public_holidays(year)

    @classmethod
    def public_holiday_series(cls, year: Union[Year, int] = CURRENT_YEAR) -> List[date]:
        """
        Gets the public holidays for 3 years (previous, current, and next) as a Polars Series.

        Args:
            year (Union[Year, int], optional): The reference year. Defaults to current year.

        Returns:
            pl.Series: A Polars Series containing all public holiday dates sorted
        """
        year_int = int(year)
        previous_year = year_int - 1
        current_year = year_int
        next_year = year_int + 1

        return (
            pl.Series(
                cls.__get_public_holidays(previous_year)
                + cls.__get_public_holidays(current_year)
                + cls.__get_public_holidays(next_year)
            )
            .sort()
            .to_list()
        )

    @classmethod
    def is_public_holiday(
        cls, date_to_check: date, year: Union[Year, int] = CURRENT_YEAR
    ) -> bool:
        """
        Check if a specific date is a public holiday.

        Args:
            date_to_check (date): The date to check
            year (Union[Year, int], optional): The reference year. Defaults to current year.

        Returns:
            bool: True if the date is a public holiday, False otherwise
        """
        holidays = cls.__get_public_holidays(year)
        return date_to_check in holidays


class DateCalculator:
    """
    Utility class for performing various date calculations.

    This class provides static methods for common date operations such as
    calculating business days, finding next/previous business days, and generating
    date sequences.
    """

    @staticmethod
    def is_business_day(d: date) -> bool:
        """
        Check if a date is a business day (not a weekend or public holiday).

        Args:
            d (date): The date to check

        Returns:
            bool: True if the date is a business day, False otherwise
        """
        # Not a weekend and not a public holiday
        return d.weekday() < 5 and not DayName.is_public_holiday(d)

    @staticmethod
    def next_business_day(d: date) -> date:
        """
        Get the next business day after the given date.

        Args:
            d (date): The reference date

        Returns:
            date: The next business day
        """
        current = d + timedelta(days=1)
        while not DateCalculator.is_business_day(current):
            current += timedelta(days=1)
        return current

    @staticmethod
    def previous_business_day(d: date) -> date:
        """
        Get the previous business day before the given date.

        Args:
            d (date): The reference date

        Returns:
            date: The previous business day
        """
        current = d - timedelta(days=1)
        while not DateCalculator.is_business_day(current):
            current -= timedelta(days=1)
        return current

    @staticmethod
    def add_business_days(d: date, days: int) -> date:
        """
        Add a specified number of business days to a date.

        Args:
            d (date): The starting date
            days (int): The number of business days to add (can be negative)

        Returns:
            date: The resulting date
        """
        if days == 0:
            return d

        current = d
        step = 1 if days > 0 else -1
        days_remaining = abs(days)

        while days_remaining > 0:
            current += timedelta(days=step)
            if DateCalculator.is_business_day(current):
                days_remaining -= 1

        return current

    @staticmethod
    def business_days_between(start_date: date, end_date: date) -> int:
        """
        Calculate the number of business days between two dates.

        Args:
            start_date (date): The start date
            end_date (date): The end date

        Returns:
            int: The number of business days between the dates (inclusive of start_date, exclusive of end_date)
        """
        if start_date > end_date:
            return -DateCalculator.business_days_between(end_date, start_date)

        count = 0
        current = start_date

        while current < end_date:
            if DateCalculator.is_business_day(current):
                count += 1
            current += timedelta(days=1)

        return count

    @staticmethod
    def business_month_end(year: Union[Year, int], month: int) -> date:
        """
        Find the last business day of a month.

        Args:
            year (Union[Year, int]): The year
            month (int): The month (1-12)

        Returns:
            date: The last business day of the month
        """
        year_int = int(year)

        # Get the last day of the month
        if month == 12:
            last_day = date(year_int, month, 31)
        else:
            last_day = date(year_int, month + 1, 1) - timedelta(days=1)

        # If it's a business day, return it
        if DateCalculator.is_business_day(last_day):
            return last_day

        # Otherwise, find the previous business day
        return DateCalculator.previous_business_day(last_day)

    @staticmethod
    def business_quarter_end(year: Union[Year, int], quarter: int) -> date:
        """
        Find the last business day of a quarter.

        Args:
            year (Union[Year, int]): The year
            quarter (int): The quarter (1-4)

        Returns:
            date: The last business day of the quarter
        """
        if not 1 <= quarter <= 4:
            raise ValueError("Quarter must be between 1 and 4")

        year_int = int(year)
        end_month = quarter * 3

        return DateCalculator.business_month_end(year_int, end_month)


class DateRangeGenerator:
    """
    Class for generating various date ranges and sequences.

    This class provides methods for creating date ranges based on different criteria,
    such as weekly, monthly, or custom ranges.
    """

    @staticmethod
    def create_date_sequence(start_date: date, end_date: date) -> List[date]:
        """
        Create a sequence of dates between start_date and end_date (inclusive).

        Args:
            start_date (date): The start date
            end_date (date): The end date

        Returns:
            List[date]: The sequence of dates
        """
        if start_date > end_date:
            return []

        result = []
        current = start_date

        while current <= end_date:
            result.append(current)
            current += timedelta(days=1)

        return result

    @staticmethod
    def create_business_day_sequence(start_date: date, end_date: date) -> List[date]:
        """
        Create a sequence of business days between start_date and end_date (inclusive).

        Args:
            start_date (date): The start date
            end_date (date): The end date

        Returns:
            List[date]: The sequence of business days
        """
        return [
            d
            for d in DateRangeGenerator.create_date_sequence(start_date, end_date)
            if DateCalculator.is_business_day(d)
        ]

    @staticmethod
    def create_weekly_table(year: Union[Year, int]) -> pl.DataFrame:
        """
        Creates a weekly table for a given year.

        This function generates a Polars DataFrame containing a weekly breakdown for the year.
        The DataFrame includes columns for:
        - date: The date for each day within the year.
        - day_of_year: The day of the year (cumulative count starting from 1).
        - week_num: The corresponding week number for each date.
        - start_date: The starting date of the week (minimum date within the week).
        - end_date: The ending date of the week (maximum date within the week).

        Args:
            year (Union[Year, int]): The year for which to create the weekly table.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the weekly breakdown information.
        """
        year_int = int(year)
        start, end = Year.date_range(year_int)

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

    @staticmethod
    def create_monthly_table(year: Union[Year, int]) -> pl.DataFrame:
        """
        Creates a monthly table for a given year.

        This function generates a Polars DataFrame containing a monthly breakdown for the year.

        Args:
            year (Union[Year, int]): The year for which to create the monthly table.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the monthly breakdown information.
        """
        year_int = int(year)
        months = []

        for month in range(1, 13):
            start_date = date(year_int, month, 1)

            if month == 12:
                end_date = date(year_int, 12, 31)
            else:
                end_date = date(year_int, month + 1, 1) - timedelta(days=1)

            business_end = DateCalculator.business_month_end(year_int, month)

            months.append(
                {
                    "month": month,
                    "month_name": Month(month).name.capitalize(),
                    "start_date": start_date,
                    "end_date": end_date,
                    "business_end_date": business_end,
                    "days_in_month": (end_date - start_date).days + 1,
                }
            )

        return pl.DataFrame(months)

    @staticmethod
    def month_range(
        month_name: str, year: Union[Year, int] = CURRENT_YEAR
    ) -> DateRange:
        """
        Calculates the start and end date of a given month within a specified year.

        Args:
            month_name (str): The name of the month
            year (Union[Year, int], optional): The year. Defaults to current year.

        Returns:
            DateRange: A tuple containing the start and end date of the month.
        """
        month_number: int = Month.to_number(month_name)
        return Month(month_number).date_range(year)


# Common date utility functions
def date_string_to_date(date_string: str) -> date:
    """
    Converts a date string in YYYY-MM-DD format to a date object.

    Args:
        date_string (str): The date string in ISO format

    Returns:
        date: The parsed date object
    """
    return date.fromisoformat(date_string)


def date_range(start_date: date, end_date: date) -> DateRange:
    """
    Creates a tuple of start and end date.

    Args:
        start_date (date): The start date
        end_date (date): The end date

    Returns:
        DateRange: A tuple of start and end date
    """
    return start_date, end_date


def get_age(birth_date: date, reference_date: Optional[date] = None) -> int:
    """
    Calculate age in years given a birth date and reference date.

    Args:
        birth_date (date): The birth date
        reference_date (Optional[date], optional): The reference date. Defaults to today.

    Returns:
        int: The age in years
    """
    reference = reference_date or CURRENT_DATE

    years = reference.year - birth_date.year

    # Adjust if birthday hasn't occurred yet this year
    if (reference.month, reference.day) < (birth_date.month, birth_date.day):
        years -= 1

    return years


# Constants for day names
DAY_NAMES: List[str] = DayName.get_all()
CALENDAR_DAY_NAMES: List[str] = DayName.get_calendar_days()
SPECIAL_DAYS: List[str] = DayName.get_special_days()


@pl.api.register_expr_namespace("days")
class Days:
    """
    Creates the days namespace for Polars expressions.

    This class provides methods for working with dates in Polars dataframes,
    including day name operations, weekend detection, business day calculations,
    and holiday handling.
    """

    def __init__(self, expr: pl.Expr) -> None:
        """
        Initialize the Days namespace with a Polars expression.

        Args:
            expr (pl.Expr): The Polars expression representing a date
        """
        self._expr = expr

    def add_day_name(self) -> pl.Expr:
        """
        Adds a day name to a dataframe based on the date.

        Returns:
            pl.Expr: A Polars expression that:
                - Returns "PH" for dates that are public holidays
                - Returns the abbreviated day name (Mon, Tue, etc.) for other dates
                - Casts the result to an Enum type using DAY_NAMES for type safety
        """
        return (
            (
                pl.when(self._expr.is_in(DayName.public_holiday_series()))
                .then(pl.lit(DayName.PH.value))
                .otherwise(self._expr.dt.to_string(format="%a"))
            )
            .cast(dtype=pl.Enum(DAY_NAMES))
            .alias("day_name")
        )

    def is_weekend(self) -> pl.Expr:
        """
        Checks if the date falls on a weekend (Saturday or Sunday).

        Returns:
            pl.Expr: A boolean expression that is True for weekends, False otherwise.
        """
        return self._expr.dt.weekday().is_in([5, 6]).alias("is_weekend")

    def is_public_holiday(self) -> pl.Expr:
        """
        Checks if the date is a public holiday.

        Returns:
            pl.Expr: A boolean expression that is True for public holidays, False otherwise.
        """
        return self._expr.is_in(DayName.public_holiday_series()).alias(
            "is_public_holiday"
        )

    def is_business_day(self) -> pl.Expr:
        """
        Checks if the date is a business day (not a weekend or public holiday).

        Returns:
            pl.Expr: A boolean expression that is True for business days, False otherwise.
        """
        return (~(self.is_weekend() | self.is_public_holiday())).alias(
            "is_business_day"
        )

    def day_type(self) -> pl.Expr:
        """
        Categorizes the date as 'Business Day', 'Weekend', or 'Public Holiday'.

        Returns:
            pl.Expr: String expression with the day type
        """
        return (
            pl.when(self.is_public_holiday())
            .then(pl.lit("Public Holiday"))
            .when(self.is_weekend())
            .then(pl.lit("Weekend"))
            .otherwise(pl.lit("Business Day"))
        ).alias("day_type")

    def next_business_day(self) -> pl.Expr:
        """
        Returns the next business day after the given date.

        Returns:
            pl.Expr: Date expression for the next business day
        """
        return (
            pl.struct([self._expr])
            .map_elements(lambda x: DateCalculator.next_business_day(x[0]))
            .alias("next_business_day")
        )

    def previous_business_day(self) -> pl.Expr:
        """
        Returns the previous business day before the given date.

        Returns:
            pl.Expr: Date expression for the previous business day
        """
        return (
            pl.struct([self._expr])
            .map_elements(lambda x: DateCalculator.previous_business_day(x[0]))
            .alias("previous_business_day")
        )
