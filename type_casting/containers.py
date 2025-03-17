"""Containers List to create enums for type safety"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import TRANSPORT_SHEET_ID, transfer_sheet


async def containers() -> pl.LazyFrame:
    """LazyFrame of containers"""

    container_lists = await load_gsheet_data(TRANSPORT_SHEET_ID, transfer_sheet).filter(
        pl.col("movement_type") != "Delivery"
    )
    return container_lists


async def containers_enum() -> pl.Enum:
    """All container numbers"""
    return await pl.Enum(
        containers()
        .select(pl.col("container_number").unique())
        .collect()
        .to_series()
        .to_list()
    )


async def iot_soc_enum()-> pl.Enum:
    """IOT SOC containers"""
    return await pl.Enum(
        containers()
        .filter(pl.col("line").eq(pl.lit("IOT")))
        .select(pl.col("container_number").unique())
        .collect()
        .to_series()
        .to_list()
    )
