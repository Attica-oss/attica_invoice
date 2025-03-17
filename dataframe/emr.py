"""EMR lazyframes"""

import polars as pl

from polars import lit, LazyFrame, when, col, Enum, Utf8

from data.price import OVERTIME_150, FREE, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import EMR_SHEET_ID, shifting_sheet, pti_sheet, washing_sheet
from type_casting.dates import SPECIAL_DAYS, public_holiday
from type_casting.validations import SetPoint, SETPOINTS
from type_casting.containers import containers_enum

# Price


async def price_list() -> dict[str, float]:
    """price dictionary"""
    magnum_pti_electricity = await (
        get_price(["PTI Magnum"]).select(pl.col("Price")).collect().to_series()[0]
    )
    plugin = (
        await get_price(["Plugin"]).select(pl.col("Price")).collect().to_series()[0]
    )
    s_freezer_pti_electricity = await (
        get_price(["PTI S Freezer"]).select(pl.col("Price")).collect().to_series()[0]
    )
    shifting_price = (
        await get_price(["Shifting"]).select(pl.col("Price")).collect().to_series()[0]
    )
    standard_pti_electricity = await (
        get_price(["PTI Standard"]).select(pl.col("Price")).collect().to_series()[0]
    )
    washing_price = await (
        get_price(["Container Cleaning"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    return {
        "washing_price": washing_price,
        "standard_pti_electricity": standard_pti_electricity,
        "s_freezer_pti_electricity": s_freezer_pti_electricity,
        "magnum_pti_electricity": magnum_pti_electricity,
        "shifting": shifting_price,
        "plugin": plugin,
    }




# Shifting Data Set


async def shifting()->LazyFrame:
    """Shifting dataframe"""

    return (
    await load_gsheet_data(sheet_id=EMR_SHEET_ID, sheet_name=shifting_sheet)
    .with_columns(
        day_name=when(col("date").is_in(public_holiday()))
        .then(lit("PH"))
        .otherwise(col("date").dt.strftime("%a"))
    )
    .select(
        col("day_name"),
        col("date"),
        col("container_number").cast(dtype=containers_enum()),
        col("invoice_to"),
        col("service_remarks"),
    )
    .with_columns(
        price=when(col("invoice_to") == "INVALID")
        .then(FREE)
        .when(col("day_name").is_in(SPECIAL_DAYS))
        .then(price_list().get("shifting") * OVERTIME_150)
        .otherwise(price_list().get("shifting"))
    )
)


# PTI records


async def _pti()->LazyFrame:
    """Initital pti dataset"""
    return await (
    load_gsheet_data(EMR_SHEET_ID, pti_sheet)
    .select(
        col("datetime_start"),
        col("container_number").cast(dtype=containers_enum()),
        col("set_point").cast(Utf8).cast(dtype=Enum(SETPOINTS)),
        col("unit_manufacturer"),
        col("datetime_end"),
        col("status").cast(dtype=Enum(["PASSED", "FAILED"])),
        col("invoice_to").cast(dtype=Enum(["MAERSKLINE", "IOT", "INVALID", "CMA CGM"])),
        col("plugged_on").alias("generator"),
    )
    .with_columns(
        hours=(pl.col("datetime_end") - col("datetime_start")).dt.total_minutes() / 60,
        plugin_price=price_list().get("plugin"),
    )
    .with_columns(above_8_hours=when(col("hours").gt(lit(8))).then(2).otherwise(1))
    .with_columns(
        electricity_price=(
            when(col("invoice_to").eq(lit("IOT")))
            .then(
                (col("datetime_end") - col("datetime_start")).dt.total_hours() / 24 + 1
            )
            .when(col("set_point").eq(SetPoint.s_freezer))
            .then(price_list().get("s_freezer_pti_electricity"))
            .when(pl.col("set_point") == SetPoint.magnum)
            .then(price_list().get("magnum_pti_electricity"))
            .when(pl.col("set_point") == SetPoint.standard)
            .then(price_list().get("standard_pti_electricity"))
            .otherwise(FREE)
        )
        * pl.col("above_8_hours")
    )
    .with_columns(
        pl.col("container_number")
        .cum_count()
        .over(pl.col("container_number"))
        .alias("cum_count")
    )
)

async def pti()->LazyFrame:
    """Pre-Trip Inspection dataset"""

    return await (
    _pti().with_columns((pl.col("cum_count") - 1).alias("previous"))
    .join(
        _pti,
        left_on=["container_number", "previous"],
        right_on=["container_number", "cum_count"],
        how="left",
    )
    .with_columns(
        no_shifting=(
            (
                (pl.col("datetime_start") - pl.col("datetime_end_right"))
                > pl.duration(hours=24)
            )
            & (pl.col("generator_right") == pl.col("generator"))
        ).fill_null(True)
    )
    .select(
        pl.col(
            [
                "datetime_start",
                "container_number",
                "set_point",
                "invoice_to",
                "datetime_end",
                "hours",
                "status",
                "plugin_price",
                "electricity_price",
                "no_shifting",
                "generator",
            ]
        )
    )
    .with_columns(
        shifting_price=pl.when(pl.col("no_shifting"))
        .then(price_list().get("shifting"))
        .otherwise(FREE)
    )
    .with_columns(
        (
            pl.col("plugin_price")
            + pl.col("electricity_price")
            + pl.col("shifting_price")
        ).alias("total_price")
    )
)



async def washing()->LazyFrame:
    """Washing Dataset"""
    return await (
    load_gsheet_data(EMR_SHEET_ID, washing_sheet)
    .select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum()),
        pl.col("invoice_to").cast(
            dtype=pl.Enum(
                [
                    "CMA CGM",
                    "ECHEBASTAR",
                    "ATUNSA",
                    "INPESCA",
                    "INVALID",
                    "IPHS",
                    "IOT",
                    "PEVASA",
                    "MAERSKLINE",
                    "SAPMER",
                    "OCEAN BASKET",
                    "IOT EXP",
                    "CCCS",
                ]
            )
        ),
        pl.col("service_remarks"),
    )
    .with_columns(
        price=pl.when(pl.col("invoice_to") != "INVALID")
        .then(price_list().get("washing"))
        .otherwise(FREE)
        .cast(pl.Int64)
    )
)
