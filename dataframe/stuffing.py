"""Stuffing Lazyframes"""

from datetime import timedelta
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import STUFFING_SHEET_ID, liner_pallet_sheet, plugin_sheet

from data.price import FREE, get_price
from type_casting.validations import PALLET_TYPE
from type_casting.customers import enum_customer, get_customer_by_type
from type_casting.containers import containers_enum
from type_casting.validations import PLUGGED_STATUS

# from type_casting.dates import DayName,DAY_NAMES

# Price


# Price
async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    liner_price = await (
        get_price(["Plastic Liner Installation"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    magnum_electricity = await (
        get_price(["Electricity Price Magnum"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    monitoring_price = await (
        get_price(["Monitoring"]).select(pl.col("Price")).collect().to_series()[0]
    )
    pallet_iot_price = await (
        get_price(["Pallets(+ Wedges) Usage"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    pallet_price = (
        await get_price(["Pallets"]).select(pl.col("Price")).collect().to_series()[0]
    )
    plugin_price = (
        await get_price(["Plugin"]).select(pl.col("Price")).collect().to_series()[0]
    )
    s_freezer_electricity = await (
        get_price(["Electricity Price S Freezer"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    standard_electricity = await (
        get_price(["Electricity Price Standard"])
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )

    return {
        "liner_price": liner_price,
        "magnum_electricity": magnum_electricity,
        "monitoring_price": monitoring_price,
        "pallet_iot_price": pallet_iot_price,
        "pallet_price": pallet_price,
        "plugin_price": plugin_price,
        "s_freezer_electricity": s_freezer_electricity,
        "standard_electricity": standard_electricity,
    }


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


async def load_pallet_dataset() -> pl.LazyFrame:
    """load the pallet and liner datasets"""
    return await load_gsheet_data(
        sheet_id=STUFFING_SHEET_ID, sheet_name=liner_pallet_sheet
    ).select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("shipping_line").cast(
            dtype=pl.Enum(
                await get_customer_by_type().get("shipping_line") + ["SAPMER"]
            )
        ),
        pl.col("assigned_to").str.to_uppercase(),
        pl.col("remarks").cast(dtype=pl.Enum(PALLET_TYPE)),
    )


# Pallet and Liner Dataframe
async def pallet() -> pl.LazyFrame:
    """Paller and Liner Dataset"""
    return await load_pallet_dataset().with_columns(
        pallet_price=pl.when(
            (
                pl.col("remarks")
                .cast(pl.Utf8)
                .str.contains(pl.lit("Pallet"), strict=True)
            ).and_(pl.col("shipping_line").eq(pl.lit("IOT")))
        )
        .then(await price_list().get("pallet_iot_price"))
        .when(
            (
                pl.col("remarks")
                .cast(pl.Utf8)
                .str.contains(pl.lit("Pallet"), strict=True)
            )
        )
        .then(await price_list().get("pallet_price"))
        .otherwise(FREE),
        liner_price=pl.when(
            (
                pl.col("remarks")
                .cast(pl.Utf8)
                .str.contains(pl.lit("Liner"), strict=True)
            ).and_(pl.col("shipping_line").eq(pl.lit("CMA CGM")))
        )
        .then(await price_list().get("liner_price"))
        .otherwise(FREE),
    )


async def coa() -> pl.LazyFrame:
    """Container Operations Activity"""
    return (
        await load_gsheet_data(STUFFING_SHEET_ID, plugin_sheet)
        .select(
            pl.col("vessel_client")
            .str.to_uppercase()
            .cast(dtype=await enum_customer()),
            pl.col("customer").cast(
                dtype=pl.Enum(await get_customer_by_type().get("shipper"))
            ),
            pl.col("date_plugged"),
            pl.col("time_plugged").str.to_time(format="%H:%M:%S", strict=False),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("operation_type"),
            pl.col("shipping_line").cast(
                dtype=pl.Enum(await get_customer_by_type().get("shipping_line"))
            ),
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
            .otherwise(await price_list().get("plugin_price")),
            monitoring_price=pl.when(
                transfer_direct | on_plug_or_partially_stuffed | plugged_only
            )
            .then(FREE)
            .otherwise(
                await price_list().get("monitoring_price"),
            ),
        )
        .with_columns(
            electricity_unit_price=pl.when(plugged_only)
            .then(pl.lit(0))
            .when(pl.col("set_point").eq(-60))
            .then(await price_list().get("s_freezer_electricity"))
            .when(pl.col("set_point").eq(-35))
            .then(await price_list().get("magnum_electricity"))
            .otherwise(await price_list().get("standard_electricity"))
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
