"""Stuffing Lazyframes"""

from datetime import timedelta
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import STUFFING_SHEET_ID, liner_pallet_sheet, plugin_sheet

from data.price import FREE, get_price
from type_casting.validations import PALLET_TYPE
from type_casting.customers import enum_customer, shipping_line, shipper
from type_casting.containers import containers_enum
from type_casting.validations import PLUGGED_STATUS

# from type_casting.dates import DayName,DAY_NAMES

# Price
LINER_PRICE = (
    get_price(["Plastic Liner Installation"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)
MAGNUM_ELECTRICITY = (
    get_price(["Electricity Price Magnum"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)
MONITORING_PRICE = (
    get_price(["Monitoring"]).select(pl.col("Price")).collect().to_series()[0]
)
PALLET_IOT_PRICE = (
    get_price(["Pallets(+ Wedges) Usage"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)
PALLET_PRICE = get_price(["Pallets"]).select(pl.col("Price")).collect().to_series()[0]
PLUGIN_PRICE = get_price(["Plugin"]).select(pl.col("Price")).collect().to_series()[0]
S_FREEZER_ELECTRICITY = (
    get_price(["Electricity Price S Freezer"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)
STANDARD_ELECTRICITY = (
    get_price(["Electricity Price Standard"])
    .select(pl.col("Price"))
    .collect()
    .to_series()[0]
)


# Yard Metrics
transfer_direct: pl.Expr = pl.col("operation_type").str.contains("Direct")
exchange_hands: pl.Expr = pl.col("operation_type").str.contains("Exchange")


on_plug_or_partially_stuffed: pl.Expr = pl.col("location").is_in(
    ["For Completion", "On Plug"]
)
on_plug: pl.Expr = pl.col("location").is_in(["On Plug"])
partially_stuffed: pl.Expr = pl.col("location").is_in(["For Completion"])
plugged_only: pl.Expr = pl.col("location") == "Plugin Only"

# Durations
duration: pl.Expr = (
    (pl.col("date_out") - pl.col("date_plugged")).dt.total_hours() / 24
).cast(pl.Int64)


def load_pallet_dataset() -> pl.LazyFrame:
    """load the pallet and liner datasets"""
    return load_gsheet_data(
        sheet_id=STUFFING_SHEET_ID, sheet_name=liner_pallet_sheet
    ).select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("shipping_line").cast(dtype=pl.Enum(shipping_line + ["SAPMER"])),
        pl.col("assigned_to").str.to_uppercase(),
        pl.col("remarks").cast(dtype=pl.Enum(PALLET_TYPE)),
    )


# Pallet and Liner Dataframe
pallet: pl.LazyFrame = load_pallet_dataset().with_columns(
    pallet_price=pl.when(
        (
            pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Pallet"), strict=True)
        ).and_(pl.col("shipping_line").eq(pl.lit("IOT")))
    )
    .then(PALLET_IOT_PRICE)
    .when((pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Pallet"), strict=True)))
    .then(PALLET_PRICE)
    .otherwise(FREE),
    liner_price=pl.when(
        (
            pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Liner"), strict=True)
        ).and_(pl.col("shipping_line").eq(pl.lit("CMA CGM")))
    )
    .then(LINER_PRICE)
    .otherwise(FREE),
)

coa: pl.LazyFrame = (
    load_gsheet_data(STUFFING_SHEET_ID, plugin_sheet)
    .select(
        pl.col("vessel_client").str.to_uppercase().cast(dtype=enum_customer()),
        pl.col("customer").cast(dtype=pl.Enum(shipper)),
        pl.col("date_plugged"),
        pl.col("time_plugged"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("operation_type"),
        pl.col("shipping_line").cast(dtype=pl.Enum(shipping_line)),
        pl.col("plugged_status").cast(dtype=pl.Enum(PLUGGED_STATUS)),
        pl.col("tonnage"),
        pl.col("set_point"),
        pl.col("date_out"),
        pl.col("location"),
    )
    .with_columns(
        days_on_plug=pl.when(transfer_direct | on_plug | plugged_only)
        .then(timedelta(days=0))
        .when(partially_stuffed)
        .then(duration)
        .otherwise(duration + 1),
        plugin_price=pl.when(transfer_direct | exchange_hands)
        .then(FREE)
        .otherwise(PLUGIN_PRICE),
        monitoring_price=pl.when(
            transfer_direct | on_plug_or_partially_stuffed | plugged_only
        )
        .then(FREE)
        .otherwise(MONITORING_PRICE),
    )
    .with_columns(
        electricity_unit_price=pl.when(plugged_only)
        .then(pl.lit(0))
        .when(pl.col("set_point").eq(-60))
        .then(S_FREEZER_ELECTRICITY)
        .when(pl.col("set_point").eq(-35))
        .then(MAGNUM_ELECTRICITY)
        .otherwise(STANDARD_ELECTRICITY)
    )
    .with_columns(
        total_electricity=pl.col("electricity_unit_price") * pl.col("days_on_plug")
    )
    .with_columns(
        total=pl.col("plugin_price")
        + pl.col("monitoring_price")
        + pl.col("total_electricity")
    )
)
