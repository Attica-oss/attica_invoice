"""Polars expression used in the dataframe creation"""
import polars as pl

from type_casting.dates import UPPER_BOUND, UPPER_BOUND_SPECIAL_DAY


# Day Exprs
is_special_day = pl.col("Day").is_in(["Sun", "PH"])
is_not_special_day = ~is_special_day

# Overtime Frame Expr
before_cut_off_normal_day = (pl.col("Time Stop") < UPPER_BOUND) & (
    pl.col("Time Start") < UPPER_BOUND
)
end_time_after_cut_off = (pl.col("Time Stop") > UPPER_BOUND) & (
    pl.col("Time Start") < UPPER_BOUND
)  # to rename the variable as it is ambigous

start_after_cut_off_normal_day = (pl.col("Time Stop") > UPPER_BOUND) & (
    pl.col("Time Start") >= UPPER_BOUND
)

stop_after_cut_off_normal_day = (pl.col("Time Stop") > UPPER_BOUND) & (
    pl.col("Time Start") < UPPER_BOUND
)


start_after_cut_off_special_day = (pl.col("Time Stop") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("Time Start") >= UPPER_BOUND_SPECIAL_DAY
)

stop_after_cut_off_special_day = (pl.col("Time Stop") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("Time Start") < UPPER_BOUND_SPECIAL_DAY
)


stop_before_cut_off_special_day = (pl.col("Time Stop") <= UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("Time Start") < UPPER_BOUND_SPECIAL_DAY
)


# Duration bits Expr
hours_after_cut_off_normal_day: pl.Expr = pl.col("Date").dt.combine(
    pl.col("Time Stop")
) - pl.col("Date").dt.combine(UPPER_BOUND)

hours_after_cut_off_special_day: pl.Expr = pl.col("Date").dt.combine(
    pl.col("Time Stop")
) - pl.col("Date").dt.combine(UPPER_BOUND_SPECIAL_DAY)

normal_duration_special_day: pl.Expr = pl.col("Date").dt.combine(
    UPPER_BOUND_SPECIAL_DAY
) - pl.col("Date").dt.combine(pl.col("Time Start"))

normal_duration : pl.Expr= pl.col("Date").dt.combine(UPPER_BOUND) - pl.col("Date").dt.combine(
    pl.col("Time Start")
)


durations: pl.Expr = pl.col("Date").dt.combine(pl.col("Time Stop")) - pl.col("Date").dt.combine(
    pl.col("Time Start")
)
