"""Miscellaneous Dataframe"""

import polars as pl

from data_source.all_dataframe import (
    by_catch_transfer,
    miscellaneous,
    cross_stuffing,
    cccs_container_stuffing,
)
from type_casting.dates import SPECIAL_DAYS
from type_casting.customers import get_customer_by_type, client_shore_cost
from type_casting.validations import (
    MOVEMENT_TYPE,
    UNLOADING_SERVICE,
    CARGO_DISPATCH_SERVICE,
    OvertimePerc,
)
from data.price import (
    get_price,
    DARDANEL_DISCOUNT,
)


# Price
TRUCK_PRICE = get_price(["Tipping Truck"])
CARGO_LOADING_PRICE = get_price(["Loading to Cargo"])
CCCS_MOVEMENT_FEE = (
    get_price(["CCCS Movement in/out"]).select(pl.col("Price")).collect().to_series()[0]
)
CROSS_STUFFING_PRICE = get_price(
    [
        "Cross Stuffing",
        "Unstuffing by Hand",
        "Unstuffing to Cargo",
        "Unstuffing to CCCS",
    ]
)
BY_CATCH_PRICE = get_price(["CCCS (By-Catch)","Transfer of by-catch"])

CCCS_STUFFING_PRICE = get_price(
    [
        "Shore Crane & Fishloader",
        "Shore Crane & Fishloader (by catch)",
        "Static Loader",
        "Container Stuffing by Hand",
        "Container Stuffing with Forklift",
    ]
)
STATIC_LOADER = get_price(["Static Loader"])


misc = miscellaneous()

# Static Loader Dataset for IOT Bin Dispatch only
static_loader: pl.LazyFrame = (
    miscellaneous()
    .select(
        pl.col("day"),
        pl.col("date"),
        pl.col("customer"),
        pl.col("operation_type"),
        pl.col("static_loader").str.replace("", "0").cast(pl.Float64),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64),
    )
    .filter(pl.col("static_loader") > 0)
    .with_columns(
        Service=pl.when(pl.col("operation_type").str.contains("Dispatch"))
        .then(pl.lit("Static Loader"))
        .otherwise(pl.lit(""))
    )
    .filter(pl.col("Service") == pl.lit("Static Loader"))
    .sort(by=pl.col("date"))
    .join_asof(
        STATIC_LOADER.lazy(),
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .select(pl.all().exclude(["Date"]))
    .with_columns(
        total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (
                pl.col("Price")
                * (pl.col("static_loader") - pl.col("overtime_tonnage"))
                * OvertimePerc.overtime_150
            )
            + (
                (
                    pl.col("Price")
                    * (pl.col("overtime_tonnage"))
                    * OvertimePerc.overtime_200
                )
            )
        )
        .otherwise(
            (
                pl.col("Price")
                * (pl.col("static_loader") - pl.col("overtime_tonnage"))
                * OvertimePerc.normal_hour
            )
            + (pl.col("Price") * pl.col("overtime_tonnage") * OvertimePerc.overtime_150)
        )
    )
)

# Dispatch to Cargo
dispatch_to_cargo: pl.LazyFrame = (
    miscellaneous()
    .filter(pl.col("operation_type").is_in(CARGO_DISPATCH_SERVICE))
    .select(
        pl.col("day"),
        pl.col("date"),
        pl.col("movement_type"),
        pl.col("customer"),
        pl.col("vessel"),
        pl.col("operation_type"),
        pl.col("total_tonnage").abs(),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64),
    )
    .with_columns(Service=pl.lit("Tipping Truck"))
    .sort(by="date")
    .join_asof(
        TRUCK_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .select(pl.all().exclude(["Service"]))
    .with_columns(
        normal_tonnage=pl.col("total_tonnage") - pl.col("overtime_tonnage"),
        Service=pl.lit("Loading to Cargo"),
        cccs_movement_fee=pl.lit(CCCS_MOVEMENT_FEE),
    )
    .join_asof(
        CARGO_LOADING_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(
        price=pl.when(pl.col("customer").eq("DARDANEL"))
        .then(pl.col("Price") - DARDANEL_DISCOUNT + pl.col("Price_right"))
        .otherwise(
            pl.col("Price") + pl.col("Price_right") + pl.col("cccs_movement_fee")
        )
    )
    .with_columns(
        total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (pl.col("normal_tonnage") * OvertimePerc.overtime_150 * pl.col("price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_200 * pl.col("price"))
        )
        .otherwise(
            (pl.col("normal_tonnage") * OvertimePerc.normal_hour * pl.col("price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_150 * pl.col("price"))
        ),
    )
    .select(pl.all().exclude(["Service", "normal_tonnage", "price"]))
    .with_columns(
        Price=pl.when(pl.col("customer").eq("DARDANEL"))
        .then(pl.col("Price") - 1.0)  # Need a better way to represent this 1.0.
        .otherwise(pl.col("Price")),
        cccs_movement_fee=pl.when(pl.col("customer").eq("DARDANEL"))
        .then(pl.lit(0))
        .otherwise(pl.col("cccs_movement_fee")),
        Price_right=pl.when(pl.col("customer").eq("DARDANEL"))
        .then(pl.col("Price_right") - 3.0)  # Need a better way to represent this 3.0.
        .otherwise(pl.col("Price_right")),
    )
    .select(
        pl.col("day").alias("day_name"),
        pl.col("date"),
        pl.col("movement_type").cast(dtype=pl.Enum(MOVEMENT_TYPE)),
        pl.col("customer"),
        pl.col("vessel"),
        pl.col("operation_type"),
        pl.col("total_tonnage"),
        pl.col("overtime_tonnage"),
        pl.col("Price").alias("truck_price"),
        pl.col("cccs_movement_fee"),
        pl.col("Price_right").alias("stevedores_on_cargo_fee"),
        pl.col("total_price"),
    )
)

# Transfer using IPHS truck for IOT and DARDANEL
truck_to_cccs = (
    miscellaneous()
    .filter(
        pl.col("customer").is_in(client_shore_cost),
        pl.col("operation_type").is_in(UNLOADING_SERVICE),
    )
    .select(
        pl.col("day"),
        pl.col("date"),
        pl.col("customer"),
        pl.col("vessel"),
        pl.col("total_tonnage"),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64),
    )
    .group_by(["day", "date", "customer", "vessel"])
    .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
    .sort(by="date", descending=False)
    .with_columns(Service=pl.lit("Tipping Truck", dtype=pl.Utf8))
    .join_asof(
        TRUCK_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(
        CCCS_incoming_fee=CCCS_MOVEMENT_FEE, operation_type=pl.lit("IPHS Truck to CCCS")
    )
    .with_columns(
        total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("Price")
                    * OvertimePerc.overtime_150
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("Price")
                    * OvertimePerc.overtime_200
                )
            )
            + (
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("CCCS_incoming_fee")
                    * OvertimePerc.overtime_150
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("CCCS_incoming_fee")
                    * OvertimePerc.overtime_200
                )
            )
        )
        .otherwise(
            (
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("Price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("Price")
                    * OvertimePerc.overtime_150
                )
            )
            + (
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("CCCS_incoming_fee")
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("CCCS_incoming_fee")
                    * OvertimePerc.overtime_150
                )
            )
        )
    )
).select(pl.all().exclude(["Service", "Date"]))

# Cross stuffing and Unstuffing dataset
cross_stuffing: pl.LazyFrame = (
    cross_stuffing()
    .join_asof(
        CROSS_STUFFING_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
    .with_columns(
        total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (pl.col("normal_hours") * OvertimePerc.overtime_150 * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_200 * pl.col("Price"))
        )
        .otherwise(
            (pl.col("normal_hours") * OvertimePerc.normal_hour * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_150 * pl.col("Price"))
        )
    )
    .select(pl.all().exclude(["normal_hours", "Date"]))
)

# BY CATCH RECORDS
by_catch_companies = bycatch 
 # To make this workable

# Filter only bycatch services
__by_catch = (
    miscellaneous()
    .filter(
        pl.col("operation_type").is_in(UNLOADING_SERVICE),
        pl.col("customer").is_in(by_catch_companies),
        # pl.col("date").dt.year().eq(CURRENT_YEAR),
    )
    .select(
        pl.col("day"),
        pl.col("date"),
        pl.col("movement_type"),
        pl.col("customer"),
        pl.col("vessel"),
        pl.col("operation_type")
        .str.replace("Sorting from Unloading", "CCCS (By-Catch)")
        .str.replace("Unsorted from Unloading", "CCCS (By-Catch)"),
        pl.col("total_tonnage").cast(pl.Float64).round(3),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64).round(3),
    )
    .group_by(["day", "date", "movement_type", "customer", "vessel", "operation_type"])
    .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
    .sort(by="date")
)

# Records which has both CCCS (by-catch) and transfer of by-catch
__by_catch_with_transfer = (
    __by_catch.join(
        by_catch_transfer(),
        on=[
            "date",
            "customer",
            "vessel",
            "movement_type",
        ],
        how="left",
    )
    .filter(pl.col("total_tonnage_right").is_not_null())
    .with_columns(
        total_tonnage=(pl.col("total_tonnage") - pl.col("total_tonnage_right")).cast(
            pl.Float64
        ),
        overtime_tonnage=(
            pl.col("overtime_tonnage") - pl.col("overtime_tonnage_right")
        ),
    )
    .filter(pl.col("total_tonnage").ne(0))
    .select(
        [
            "day",
            "date",
            "movement_type",
            "customer",
            "vessel",
            "operation_type",
            "total_tonnage",
            "overtime_tonnage",
        ]
    )
)


# Services which has only by-catch
_by_catch_only = (
    __by_catch.join(
        __by_catch_with_transfer,
        on=[
            "date",
            "customer",
            "vessel",
            "movement_type",
        ],
        how="left",
    )
    .filter(pl.col("operation_type_right").is_null())
    .select(
        [
            "day",
            "date",
            "movement_type",
            "customer",
            "vessel",
            "operation_type",
            "total_tonnage",
            "overtime_tonnage",
        ]
    )
    .join(
        by_catch_transfer(),
        on=[
            "date",
            "customer",
            "vessel",
            "movement_type",
        ],
        how="left",
    )
    .filter(pl.col("operation_type_right").is_null())
    .select(
        [
            "day",
            "date",
            "movement_type",
            "customer",
            "vessel",
            "operation_type",
            "total_tonnage",
            "overtime_tonnage",
        ]
    )
)

# Final By Catch Record
by_catch = (
    pl.concat(
        [_by_catch_only, by_catch_transfer(), __by_catch_with_transfer],
        how="vertical",
    )
    .sort(by="date")
    .with_columns(pl.col("operation_type").alias("Service"))
    .sort(by="date")
    .join_asof(
        BY_CATCH_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
    .with_columns(
        total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (pl.col("normal_hours") * OvertimePerc.overtime_150 * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_200 * pl.col("Price"))
        )
        .otherwise(
            (pl.col("normal_hours") * OvertimePerc.normal_hour * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_150 * pl.col("Price"))
        )
    )
    .select(pl.all().exclude(["movement_type", "Date", "Service", "normal_hours"]))
)

# CCCS Container Stuffing dataset
cccs_stuffing: pl.LazyFrame = (
    cccs_container_stuffing()
    .join_asof(
        CCCS_STUFFING_PRICE,
        by="Service",
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
    .with_columns(
        total_price=pl.when(pl.col("Day").is_in(SPECIAL_DAYS))
        .then(
            (pl.col("normal_hours") * OvertimePerc.overtime_150 * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_200 * pl.col("Price"))
        )
        .otherwise(
            (pl.col("normal_hours") * OvertimePerc.normal_hour * pl.col("Price"))
            + (pl.col("overtime_tonnage") * OvertimePerc.overtime_150 * pl.col("Price"))
        )
    )
    .select(pl.all().exclude(["normal_hours"]))
)
