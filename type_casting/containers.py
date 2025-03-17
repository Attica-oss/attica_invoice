"""Containers List to create enums for type safety"""
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (TRANSPORT_SHEET_ID,
                                   transfer_sheet)


containers : pl.LazyFrame = (load_gsheet_data(
    TRANSPORT_SHEET_ID,
    transfer_sheet)
    .filter(
        pl.col('movement_type') != "Delivery")
        )

container_list:list[str] = (containers
                            .select(
                                pl.col('container_number')
                                .unique()
                                ).collect()
                                .to_series()
                                .to_list())

iot_soc:list[str] = (containers
                     .filter(pl.col('line') == "IOT")
                     .select(pl.col('container_number').unique())
                     .collect()
                     .to_series()
                     .to_list())

# Enums
containers_enum:pl.Enum = pl.Enum(container_list)
iot_soc_enum:pl.Enum = pl.Enum(iot_soc)
