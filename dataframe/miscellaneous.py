"""Miscellaneous Dataframe"""

import polars as pl

from data_source.all_dataframe import (
    by_catch_transfer,
    miscellaneous,
    _cross_stuffing,
    cccs_container_stuffing,
)
from type_casting.dates import SPECIAL_DAYS
from type_casting.customers import client_shore_cost,get_customer_by_type
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

async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    truck_price = await get_price(["Tipping Truck"])

    cargo_loading_price = await get_price(["Loading to Cargo"])

    cccs_movement_fee = (
        await get_price(["CCCS Movement in/out"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    cross_stuffing_price = await get_price(
        [
            "Cross Stuffing",
            "Unstuffing by Hand",
            "Unstuffing to Cargo",
            "Unstuffing to CCCS",
        ]
    )

    by_catch_price = await get_price(["CCCS (By-Catch)", "Transfer of by-catch"])

    cccs_stuffing_price = await get_price(
        [
            "Shore Crane & Fishloader",
            "Shore Crane & Fishloader (by catch)",
            "Static Loader",
            "Container Stuffing by Hand",
            "Container Stuffing with Forklift",
        ]
    )
    static_loader_price = await get_price(["Static Loader"])

    return {
        "truck_price": truck_price,
        "cargo_loading_price": cargo_loading_price,
        "cccs_movement_fee": cccs_movement_fee,
        "cross_stuffing_price": cross_stuffing_price,
        "by_catch_price": by_catch_price,
        "cccs_stuffing_price": cccs_stuffing_price,
        "static_loader_price": static_loader_price,
    }


async def static_loader() -> pl.LazyFrame:
    """Static loader dataset for Bin Dispatch only"""
    return await (
        await miscellaneous()
        .select(
            pl.col("day"),
            pl.col("date"),
            pl.col("customer"),
            pl.col("operation_type"),
            pl.col("static_loader").str.replace("", "0").cast(pl.Float64),
            pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64),
        )
        .filter(pl.col("static_loader").gt(0))
        .with_columns(
            Service=pl.when(
                pl.col("operation_type")
                .str.contains(pl.lit("IOT"))
                .then(pl.lit("Static Loader"))
                .otherwise(pl.lit(""))
            )
        )
        .filter(pl.col("Service").eq(pl.lit("Static Loader")))
        .sort(by=pl.col("date"))
        .join_asof(
            await price_list().get("static_loader_price"),
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
                    pl.col("Price")
                    * (pl.col("overtime_tonnage"))
                    * OvertimePerc.overtime_200
                )
            )
            .otherwise(
                (
                    pl.col("Price")
                    * (pl.col("static_loader") - pl.col("overtime_tonnage"))
                    * OvertimePerc.normal_hour
                )
                + (
                    pl.col("Price")
                    * pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                )
            )
        )
    )


# Dispatch to Cargo
async def dispatch_to_cargo() -> pl.LazyFrame:
    """Dispatch to cargo/from cargo dataset"""
    return await (
        await miscellaneous()
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
            await price_list().get("truck_price"),
            by="Service",
            left_on="date",
            right_on="Date",
            strategy="backward",
        )
        .select(pl.all().exclude(["Service"]))
        .with_columns(
            normal_tonnage=pl.col("total_tonnage") - pl.col("overtime_tonnage"),
            Service=pl.lit("Loading to Cargo"),
            cccs_movement_fee=pl.lit(await price_list().get("cccs_movement_fee")),
        )
        .join_asof(
            await price_list().get("cargo_loading_price"),
            by="Service",
            left_on="date",
            right_on="Date",
            strategy="backward",
        )
        .with_columns(
            price=pl.when(pl.col("customer").eq(pl.lit("DARDANEL")))
            .then((pl.col("Price") - DARDANEL_DISCOUNT) + pl.col("Price_right"))
            .otherwise(
                pl.col("Price") + pl.col("Price_right") + pl.col("cccs_movement_fee")
            )
        )
        .with_columns(
            total_price=pl.when(pl.col("day").is_in(SPECIAL_DAYS))
            .then(
                (pl.col("normal_tonnage") * OvertimePerc.overtime_150 * pl.col("price"))
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("price")
                )
            )
            .otherwise(
                (pl.col("normal_tonnage") * OvertimePerc.normal_hour * pl.col("price"))
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("price")
                )
            ),
        )
        .select(pl.all().exclude(["Service", "normal_tonnage", "price"]))
        .with_columns(
            Price=pl.when(pl.col("customer").eq("DARDANEL"))
            .then(pl.col("Price") - 1.0)  # Need a better way to represent this 1.0.
            .otherwise(pl.col("Price")),
            cccs_movement_fee=pl.when(pl.col("customer").eq(pl.lit("DARDANEL")))
            .then(pl.lit(0))
            .otherwise(pl.col("cccs_movement_fee")),
            Price_right=pl.when(pl.col("customer").eq(pl.lit("DARDANEL")))
            .then(
                pl.col("Price_right") - 3.0
            )  # Need a better way to represent this 3.0.
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
async def truck_to_cccs() -> pl.LazyFrame:
    """Transfer to CCCS using IPHS truck for IOT and Dardanel"""
    return await (
        await miscellaneous()
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
            await price_list().get("truck_price"),
            by="Service",
            left_on="date",
            right_on="Date",
            strategy="backward",
        )
        .with_columns(
            CCCS_incoming_fee=await price_list().get("cccs_movement_fee"),
            operation_type=pl.lit("IPHS Truck to CCCS"),
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
async def cross_stuffing()->pl.LazyFrame:
    """Cross Stuffing and Unstuffing dataset"""
    return await (
    await _cross_stuffing()
    .join_asof(
        await price_list().get("cross_stuffing_price"),
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

# To make this workable

# Filter only bycatch services
async def __by_catch()->pl.LazyFrame:
    return await (
    await miscellaneous()
    .filter(
        pl.col("operation_type").is_in(UNLOADING_SERVICE),
        pl.col("customer").is_in(await get_customer_by_type().get("bycatch")),
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
async def __by_catch_with_transfer()->pl.LazyFrame:
    """Combine both CCCS (bu-catch) and Transfer of Bycatch"""
    return await (
    await __by_catch().join(
        await by_catch_transfer(),
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
async def _by_catch_only()->pl.LazyFrame:
    """Dataset which has only by-catch"""
    return await (
    await __by_catch().join(
        await __by_catch_with_transfer(),
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
        await by_catch_transfer(),
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
async def by_catch()->pl.LazyFrame:

    """Final By catch records"""
    return await (
    pl.concat(
        [await _by_catch_only(), await by_catch_transfer(), await __by_catch_with_transfer()],
        how="vertical",
    )
    .sort(by="date")
    .with_columns(pl.col("operation_type").alias("Service"))
    .sort(by="date")
    .join_asof(
        await price_list().get("by_catch_price"),
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
async def cccs_stuffing()->pl.LazyFrame:
    """"CCCS Container Stuffing dataset"""
    return await (
    await cccs_container_stuffing()
    .join_asof(
        await price_list().get("cccs_stuffing_price"),
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
