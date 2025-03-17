"""All the dataframe call and clean up"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    MISC_SHEET_ID,
    ALL_CCCS_DATA_SHEET,
    CROSS_STUFFING_SHEET,
    BY_CATCH_SHEET,
    CCCS_STUFFING_SHEET
)
from type_casting.dates import CURRENT_YEAR, DAY_NAMES, public_holiday
from type_casting.validations import FISH_STORAGE
from type_casting.containers import containers_enum


# Miscellaneous Main Sheet clean up
def miscellaneous()->pl.LazyFrame:
    """Miscellaneous main sheet"""
    return (load_gsheet_data(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET).select(
    pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
    pl.col("date"),
    pl.col("movement_type"),
    pl.col("customer"),
    pl.col("origin"),
    pl.col("vessel"),
    pl.col("storage_type").cast(dtype=pl.Enum(FISH_STORAGE)),
    pl.col("operation_type"),
    pl.col("total_tonnage"),
    pl.col("bins_in").str.replace("", "0").cast(pl.Int64),
    pl.col("bins_out").str.strip_chars("-").replace("", "0").cast(pl.Int64) * -1,
    pl.col("static_loader").str.replace("", "0").cast(pl.Float64), # static_loader
    pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64),# overtime_tonnage
))

def cross_stuffing()->pl.LazyFrame:
    """Cross stuffing sheet"""
    return (load_gsheet_data(MISC_SHEET_ID, CROSS_STUFFING_SHEET)
    .filter(pl.col("day").str.replace("", "x").ne("x"))
    .select(
        pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("vessel_client"),
        pl.col("date"),
        pl.col("origin"),
        pl.col("destination"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("total_tonnage").str.replace("", "0").cast(pl.Float64).fill_null(0),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64).fill_null(0),
        pl.col("is_origin_empty"),
        pl.col("service").alias("Service"),
        pl.col("invoiced"),
    ))

def by_catch_transfer()->pl.LazyFrame:
    """by catch transfer sheet"""
    return (
            load_gsheet_data(MISC_SHEET_ID, BY_CATCH_SHEET)
    .with_columns(
        day=pl.when(pl.col("date").is_in(public_holiday(CURRENT_YEAR)))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a")).cast(dtype=pl.Enum(DAY_NAMES))
    )
    .select(
        pl.col("day"),
        pl.col("date"),
        pl.col("movement_type"),
        pl.col("customer"),
        pl.col("vessel"),
        pl.col("service").alias("operation_type"),
        pl.col("total_tonnage").cast(pl.Float64).round(3),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64).round(3),
    )
)

def cccs_container_stuffing()->pl.LazyFrame:
    """CCCS container stuffing dataframe clean up"""
    return (
            load_gsheet_data(MISC_SHEET_ID, CCCS_STUFFING_SHEET)
    .select(
        pl.col("Day").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("customer"),
        pl.col("service").alias("Service"),
        pl.col("total_tonnage"),
        pl.col("overtime_tonnage"),
        pl.col("invoiced"),
    )
    )
