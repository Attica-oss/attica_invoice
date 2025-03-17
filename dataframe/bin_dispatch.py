"Bin dispatch to and from IOT"
from datetime import date
import polars as pl
from data.price import ALL_PRICE, OVERTIME_150, OVERTIME_200, NORMAL_HOUR

# from dataframe import shore_handling
from dataframe.transport import scow_transfer
from data_source.make_dataset import load_gsheet_data_async
from data_source.sheet_ids import (
    MISC_SHEET_ID,
    ALL_CCCS_DATA_SHEET,
)
from type_casting.dates import (
    # DAY_NAMES,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    public_holiday,
    CURRENT_YEAR,
)
from type_casting.validations import (
    BIN_DISPATCH_SERVICE,
    MOVEMENT_TYPE,
    Status,
    MovementType,
    Overtime,
)

# Prepare the list of Public Holiday dates in the Current Year
ph_list: list[date] = public_holiday()


# Price
SCOW_TRANSFER = ALL_PRICE.filter(
    pl.col("Service").eq(pl.lit("CCCS Movement in/out"))
)


# Full Scows
bin_dispatch: pl.LazyFrame = (
    load_gsheet_data_async(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET)
    .filter(
        pl.col("operation_type").is_in(BIN_DISPATCH_SERVICE),
        pl.col("date").dt.year() >= CURRENT_YEAR - 1,
    )
    .select(
        pl.col("day").alias("day_name"),
        pl.col("date"),
        pl.col("movement_type").cast(dtype=pl.Enum(MOVEMENT_TYPE)),
        pl.col("customer"),
        pl.col("operation_type"),
        pl.col("total_tonnage").abs().cast(pl.Float64).round(3),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float32).round(3),
    )
    .with_columns(normal_tonnage=(pl.col("total_tonnage") - pl.col("overtime_tonnage")))
)

full_scows: pl.LazyFrame = (
    scow_transfer.filter(pl.col("status") == Status.full)
    .with_columns(
        movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
        .then(pl.lit(MovementType.out))
        .otherwise(pl.lit(MovementType.in_))
        .cast(dtype=pl.Enum(MOVEMENT_TYPE)),
        day_name=pl.when(pl.col("date").is_in(ph_list))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a")),
    )
    .with_columns(
        overtime=pl.when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.lit(Overtime.overtime_200_text))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            | (
                (~pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time_out") > UPPER_BOUND)
            )
        )
        .then(pl.lit(Overtime.overtime_150_text))
        .otherwise(pl.lit(Overtime.normal_hour_text))
    )
    .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
    .agg(
        pl.col("time_out").min().alias("start_time"),
        pl.col("time_in").max().alias("end_time"),
        pl.col("num_of_scows").sum(),
    )
    .join(other=bin_dispatch, on=["date", "customer", "movement_type"], how="left")
    .with_columns(
        tonnage=pl.when(
            (pl.col("overtime") == Overtime.normal_hour_text)
            | (
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("overtime") == Overtime.overtime_150_text)
            )
        )
        .then(pl.col("normal_tonnage"))
        .otherwise(pl.col("overtime_tonnage"))
        .cast(pl.Float64)
    )
    .select(
        pl.all().exclude(
            [
                # "day_name",
                "operation_type",
                "total_tonnage",
                "overtime_tonnage",
                "normal_tonnage",
            ]
        )
    )
    .sort(by=pl.col("date"))
    .join_asof(
        SCOW_TRANSFER.lazy(),
        by=None,
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(
        total_price=pl.when(pl.col("overtime") == Overtime.normal_hour_text)
        .then(pl.col("tonnage") * NORMAL_HOUR * pl.col("Price"))
        .when(pl.col("overtime") == Overtime.overtime_150_text)
        .then(pl.col("tonnage") * OVERTIME_150 * pl.col("Price"))
        .otherwise(pl.col("tonnage") * OVERTIME_200 * pl.col("Price")),
        movement_type=pl.when(pl.col("movement_type") == MovementType.out)
        .then(pl.lit("IPHS Delivery of Full Scows to IOT"))
        .when(pl.col("movement_type") == MovementType.in_)
        .then(pl.lit("IPHS Collection of Full Scows from IOT"))
        .otherwise(pl.lit("Err")),
    )
    .select(
        [
            "day_name",
            "date",
            "customer",
            "movement_type",
            "overtime",
            "start_time",
            "end_time",
            "num_of_scows",
            "tonnage",
            "Price",
            "total_price",
        ]
    )
)

# Empty Scows
empty_scows: pl.LazyFrame = (
    scow_transfer.filter(pl.col("status") == Status.empty)
    .with_columns(
        movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
        .then(pl.lit(MovementType.out))
        .otherwise(pl.lit(MovementType.in_))
        .cast(dtype=pl.Enum(MOVEMENT_TYPE)),
        day_name=pl.when(pl.col("date").is_in(ph_list))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a")),
    )
    .with_columns(
        overtime=pl.when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.lit(Overtime.overtime_200_text))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            | (
                (~pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time_out") > UPPER_BOUND)
            )
        )
        .then(pl.lit(Overtime.overtime_150_text))
        .otherwise(pl.lit(Overtime.normal_hour_text))
    )
    .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
    .agg(
        pl.col("time_out").min().alias("start_time"),
        pl.col("time_in").max().alias("end_time"),
        pl.col("num_of_scows").sum(),
    )
    .sort(by="date")
    .join_asof(
        SCOW_TRANSFER.lazy(),
        by=None,
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(
        total_price=pl.when(pl.col("overtime") == Overtime.normal_hour_text)
        .then(pl.col("num_of_scows") * NORMAL_HOUR * pl.col("Price"))
        .when(pl.col("overtime") == Overtime.overtime_150_text)
        .then(pl.col("num_of_scows") * OVERTIME_150 * pl.col("Price"))
        .otherwise(pl.col("num_of_scows") * OVERTIME_200 * pl.col("Price")),
        movement_type=pl.when(pl.col("movement_type") == MovementType.out)
        .then(pl.lit("IPHS Delivery of Empty Scows to IOT"))
        .when(pl.col("movement_type") == MovementType.in_)
        .then(pl.lit("IPHS Collection of Empty Scows from IOT"))
        .otherwise(pl.lit("Err")),
    )
    .select(pl.all().exclude(["Service", "Date"]))
)
