"""EMR lazyframes"""

from typing import Final
import polars as pl

from polars import lit, LazyFrame, when, col, Enum, Utf8

from data.price import OVERTIME_150, FREE, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import EMR_SHEET_ID, shifting_sheet, pti_sheet, washing_sheet
from type_casting.dates import SPECIAL_DAYS, DayName
from type_casting.validations import SetPoint, SETPOINTS
from type_casting.containers import containers_enum

# Price


async def price_list() -> dict[str, float]:
    """price dictionary"""

    price = await get_price(
        [
            "PTI Magnum",
            "Plugin",
            "PTI S Freezer",
            "Shifting",
            "PTI Standard",
            "Container Cleaning",
        ]
    )

    magnum_pti_electricity = (
        price.filter(pl.col("Service").eq(pl.lit("PTI Magnum")))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    plugin = (
        price.filter(pl.col("Service").eq(pl.lit("Plugin")))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    s_freezer_pti_electricity = (
        price.filter(pl.col("Service").eq(pl.lit("PTI S Freezer")))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    shifting_price = (
        price.filter(pl.col("Service").eq(pl.lit("Shifting")))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    standard_pti_electricity = (
        price.filter(pl.col("Service").eq(pl.lit("PTI Standard")))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    washing_price = (
        price.filter(pl.col("Service").eq(pl.lit("Container Cleaning")))
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


async def shifting() -> LazyFrame:
    """Shifting dataframe"""

    # First, await the async operations and store the results
    prices = await price_list()
    shifting_price = prices.get("shifting")

    df = await load_gsheet_data(sheet_id=EMR_SHEET_ID, sheet_name=shifting_sheet)
    containers = await containers_enum()

    return (
        df.with_columns(date=pl.col("date").str.to_date(format="%d/%m/%Y"))
        .with_columns(
            day_name=when(col("date").is_in(DayName.public_holiday_series()))
            .then(lit(DayName.PH.value))
            .otherwise(col("date").dt.strftime("%a"))
        )
        .select(
            col("day_name"),
            col("date"),
            col("container_number").cast(dtype=containers),
            col("invoice_to"),
            col("service_remarks"),
        )
        .with_columns(
            price=when(col("invoice_to").eq(pl.lit("INVALID")))
            .then(FREE)
            .when(col("day_name").is_in(SPECIAL_DAYS))
            .then(shifting_price * OVERTIME_150)
            .otherwise(shifting_price)
        )
    )


# PTI records


async def _pti() -> LazyFrame:
    """Initital pti dataset"""

    df = await load_gsheet_data(EMR_SHEET_ID, pti_sheet)
    # container_enum = await containers_enum()

    EIGHT_HOURS: Final[int] = pl.lit(8)  # Duration to invoice electricity
    DOUBLE_PRICE: Final[int] = 2
    NORMAL_PRICE: Final[int] = 1

    price = await price_list()
    plugin = price.get("plugin")
    s_freezer_pti_electricity = price.get("s_freezer_pti_electricity")
    magnum_pti_electricity = price.get("magnum_pti_electricity")
    standard_pti_electricity = price.get("standard_pti_electricity")

    return (
        df.select(
            col("datetime_start").str.to_datetime(format="%d/%m/%Y %H:%M:%S"),
            col("container_number"),
            col("set_point").cast(Utf8).cast(dtype=Enum(SETPOINTS)),
            col("unit_manufacturer"),
            col("datetime_end").str.to_datetime(format="%d/%m/%Y %H:%M:%S"),
            col("status").cast(dtype=Enum(["PASSED", "FAILED"])),
            col("invoice_to").cast(
                dtype=Enum(["MAERSKLINE", "IOT", "INVALID", "CMA CGM"])
            ),
            col("plugged_on").alias("generator"),
        )
        .with_columns(
            hours=(pl.col("datetime_end") - col("datetime_start")).dt.total_minutes()
            / 60,
            plugin_price=plugin,
        )
        .with_columns(
            above_8_hours=when(col("hours").gt(EIGHT_HOURS))
            .then(DOUBLE_PRICE)
            .otherwise(NORMAL_PRICE)
        )
        .with_columns(
            electricity_price=(
                when(col("invoice_to").eq(lit("IOT")))
                .then(
                    (col("datetime_end") - col("datetime_start")).dt.total_hours() / 24
                    + 1
                )
                .when(col("set_point").eq(SetPoint.s_freezer))
                .then(s_freezer_pti_electricity)
                .when(pl.col("set_point").eq(SetPoint.magnum))
                .then(magnum_pti_electricity)
                .when(pl.col("set_point").eq(SetPoint.standard))
                .then(standard_pti_electricity)
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


async def pti() -> LazyFrame:
    """Pre-Trip Inspection dataset"""
    df = await _pti()
    df_2 = await _pti()
    price = await price_list()
    shifting_ = price.get("shifting")

    container_enum = await containers_enum()

    return (
        df.with_columns(
            container_number=pl.col("container_number").cast(dtype=container_enum),
            previous=pl.col("cum_count") - 1,
        )
        .join(
            df_2.with_columns(
                container_number=pl.col("container_number").cast(dtype=container_enum)
            ),
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
            .then(shifting_)
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


async def washing() -> LazyFrame:
    """Washing Dataset"""
    df = await load_gsheet_data(EMR_SHEET_ID, washing_sheet)
    container_enum = await containers_enum()
    price = await price_list()
    washing_ = price.get("washing_price")

    return df.select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=container_enum),
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
                    "RAWANQ",
                    "OMAN PELAGIC",
                    "AMIRANTE",
                ]
            )
        ),
        pl.col("service_remarks"),
    ).with_columns(
        price=pl.when(pl.col("invoice_to").ne(pl.lit("INVALID")))
        .then(washing_)
        .otherwise(FREE)
        .cast(pl.Int64)
    )
