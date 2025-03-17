"""Shore handling Lazyframe"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import SHORE_HANDLING_ID, salt_sheet, bin_tipping_sheet
from type_casting.customers import ship_owner, purseiner
from type_casting.validations import MOVEMENT_TYPE, OvertimePerc
from type_casting.dates import (
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    DAY_NAMES,
    public_holiday,
)

# from data_source import expressions as exp
from data.price import get_price

BIN_TIPPING_PRICE = (
    get_price(["CCCS Movement in/out"]).select(pl.col("Price")).collect().to_series()[0]
)

SALT_PRICE = (
    get_price(["Loading (Quay to Ship)", "Loading @ Zone 14"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)


ph_list: pl.Series = public_holiday()


is_special_day = pl.col("day_name").is_in(SPECIAL_DAYS)

is_not_special_day = ~is_special_day

# Condition for Duration Calculation
# For Normal Days
before_cut_off_normal_day = (pl.col("end_time") < UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)
end_time_after_cut_off = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)  # rename variable: is_end_time_after_cut_off

start_after_cut_off_normal_day = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") >= UPPER_BOUND
)

stop_after_cut_off_normal_day = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)


# Coditional for Overtime Days

start_after_cut_off_special_day = (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") >= UPPER_BOUND_SPECIAL_DAY
)

stop_after_cut_off_special_day = (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY
)


stop_before_cut_off_special_day = (pl.col("end_time") <= UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY
)

# Durations based on the conditionals

hours_after_cut_off_normal_day = pl.col("date").dt.combine(pl.col("end_time")) - pl.col(
    "date"
).dt.combine(UPPER_BOUND)

hours_after_cut_off_special_day = pl.col("date").dt.combine(
    pl.col("end_time")
) - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)

normal_duration_special_day = pl.col("date").dt.combine(
    UPPER_BOUND_SPECIAL_DAY
) - pl.col("date").dt.combine(pl.col("start_time"))

normal_duration = pl.col("date").dt.combine(UPPER_BOUND) - pl.col("date").dt.combine(
    pl.col("start_time")
)


durations = pl.col("date").dt.combine(pl.col("end_time")) - pl.col("date").dt.combine(
    pl.col("start_time")
)


salt: pl.LazyFrame = (
    load_gsheet_data(sheet_id=SHORE_HANDLING_ID, sheet_name=salt_sheet)
    .select(
        pl.col("day_name").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("date"),
        pl.col("vessel").cast(dtype=pl.Enum(purseiner)),
        pl.col("customer").str.strip_chars().cast(dtype=pl.Enum(ship_owner())),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("duration"),
        pl.col("operation_type"),
        pl.col("tonnage"),
    )
    .with_columns(
        normal=pl.when(is_not_special_day & end_time_after_cut_off)
        .then(normal_duration)
        .when(is_not_special_day & before_cut_off_normal_day)
        .then(durations)
        .otherwise(pl.duration()),
        normal_150=pl.when(is_not_special_day & start_after_cut_off_normal_day)
        .then(durations)
        .when(is_not_special_day & stop_after_cut_off_normal_day)
        .then(hours_after_cut_off_normal_day)
        .otherwise(pl.duration()),
        sun_150=pl.when(is_special_day & stop_after_cut_off_special_day)
        .then(normal_duration_special_day)
        .when(is_special_day & stop_before_cut_off_special_day)
        .then(durations)
        .otherwise(pl.duration()),
        overtime_200=pl.when(is_special_day & start_after_cut_off_special_day)
        .then(durations)
        .when(is_special_day & stop_after_cut_off_special_day)
        .then(hours_after_cut_off_special_day)
        .otherwise(pl.duration()),
    )
    .with_columns(
        normal=(pl.col("normal") / durations) * pl.col("tonnage"),
        overtime_150=(pl.col("normal_150") / durations) * pl.col("tonnage")
        + (pl.col("sun_150") / durations) * pl.col("tonnage"),
        overtime_200=(pl.col("overtime_200") / durations) * pl.col("tonnage"),
    )
    .with_columns(
        price=(pl.col("normal") * SALT_PRICE * OvertimePerc.normal_hour)
        + (pl.col("overtime_150") * SALT_PRICE * OvertimePerc.overtime_150)
        + (pl.col("overtime_200") * SALT_PRICE * OvertimePerc.overtime_200)
    )
    .select(pl.all().exclude(["normal_150", "sun_150"]))
)


add_day_name_col: pl.Expr = (
    pl.when(pl.col("Date").is_in(ph_list))
    .then(pl.lit("PH"))
    .otherwise(pl.col("Date").dt.to_string(format="%a"))
)


bin_tipping: pl.LazyFrame = (
    load_gsheet_data(sheet_id=SHORE_HANDLING_ID, sheet_name=bin_tipping_sheet)
    .filter(pl.col("Tonnage Tipped") > 0)
    .with_columns(day_name=add_day_name_col, Service=pl.lit("IPHS Bin Tipping"))
    .select(
        pl.col("day_name").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("Date"),
        pl.col("Customer"),
        pl.col("movement_type").cast(dtype=pl.Enum(MOVEMENT_TYPE)),
        pl.col("Service"),
        pl.col("IOT Scows (Tipping)").alias("number_of_scows_tipped"),
        pl.col("Tonnage Tipped").cast(pl.Float64),
        pl.col("Overtime"),
    )
    .with_columns(
        price=BIN_TIPPING_PRICE,
        total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(
            (
                BIN_TIPPING_PRICE
                * OvertimePerc.overtime_150
                * (pl.col("Tonnage Tipped") - pl.col("Overtime"))
            )
            + (BIN_TIPPING_PRICE * OvertimePerc.overtime_200 * pl.col("Overtime"))
        )
        .otherwise(
            (
                BIN_TIPPING_PRICE
                * OvertimePerc.normal_hour
                * (pl.col("Tonnage Tipped") - pl.col("Overtime"))
            )
            + (BIN_TIPPING_PRICE * OvertimePerc.overtime_150 * pl.col("Overtime"))
        ),
    )
)
