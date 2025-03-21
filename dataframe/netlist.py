"""NetList Lazyframes"""

from datetime import date
import polars as pl
from data_source.all_dataframe import miscellaneous
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    # MISC_SHEET_ID,
    OPS_SHEET_ID,
    raw_sheet,
    net_list_sheet,
    # by_catch_sheet,
    # all_cccs_data_sheet,
)
from type_casting.dates import (
    SPECIAL_DAYS,
    public_holiday,
)
from type_casting.validations import (
    FISH_STORAGE,
    # MOVEMENT_TYPE,
    UNLOADING_SERVICE,
    Overtime,
    OvertimePerc,
)
from type_casting.containers import iot_soc_enum
from type_casting.customers import get_customer_by_type

from dataframe.stuffing import coa
from data.price import FREE, get_price

ph_list: list[date] = public_holiday()


# Price
async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    service_list: list[str] = [
        "Transhipment - Brine",
        "Transhipment - Dry",
        "Unload to CCCS - Brine",
        "Unload to CCCS - Dry",
        "Unload to Quay - Brine",
        "Unload to Quay - Dry",
        "Container Stuffing - Brine",
        "Container Stuffing - Dry",
        "Full OSS - Brine",
        "Full OSS - Dry",
        "Basic OSS - Brine",
        "Basic OSS - Dry",
    ]

    oss_service_list: list[str] = [
        "Container Stuffing - Brine",
        "Container Stuffing - Dry",
        "Stuffing",
    ]

    stuffing_price = await get_price(["Stuffing"])

    unloading_price: pl.LazyFrame = await get_price(service_list).with_columns(
        date=pl.col("Date")
    )

    oss_stuffing_price: pl.LazyFrame = await get_price(oss_service_list).with_columns(
        date=pl.col("Date")
    )

    return {
        "stuffing_price": stuffing_price,
        "unloading_price": unloading_price,
        "oss_stuffing_price": oss_stuffing_price,
    }


by_catch_companies: list[str] = [
    "AMIRANTE",
    "OCEAN BASKET",
    "ISLAND CATCH",
]  # move this to the customer module


# Container Stuffing Type
async def stuffing_type() -> pl.LazyFrame:
    """Container Stuffing Type"""
    return await (
        await coa()
        .select(
            pl.col(
                [
                    "vessel_client",
                    "customer",
                    "date_plugged",
                    "container_number",
                    "operation_type",
                ]
            )
        )
        .with_columns(
            pl.col("container_number").cast(pl.Utf8),
            pl.col("vessel_client").cast(pl.Utf8),
        )
        .filter(
            (~pl.col("operation_type").str.contains("CCCS"))
            & (pl.col("operation_type").str.contains_any(["Full", "Basic", "Stuffing"]))
        )
        .with_columns(
            stuffing=pl.when(pl.col("operation_type").str.contains("Full"))
            .then(pl.lit("Full OSS"))
            .when(pl.col("operation_type").str.contains("Basic"))
            .then(pl.lit("Basic OSS"))  # Changed to Basic OSS
            .otherwise(pl.lit("Container Stuffing"))
        )
        .select(pl.all().exclude("operation_type"))
    )


# CCCS record from the Miscellaneous Activity
async def cccs_record() -> pl.LazyFrame:
    """CCCS record from the Misc Activity"""
    return await (
        (
            await miscellaneous()
            .filter(
                pl.col("operation_type").is_in(UNLOADING_SERVICE),
                ~pl.col("customer").is_in(by_catch_companies),
            )
            .with_columns(destination="CCCS (" + pl.col("customer").cast(pl.Utf8) + ")")
            .select(
                pl.col("day"),
                pl.col("date"),
                pl.col("movement_type"),
                pl.col("destination"),
                pl.col("vessel"),
                pl.col("operation_type"),
                pl.col("total_tonnage"),
                pl.col("overtime_tonnage"),
                pl.col("storage_type"),
            )
        )
        .group_by(["date", "destination", "vessel", "storage_type"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .sort(by="date")
    )


# CCCS adjusted record in the Genesis Data


async def cccs_adjusted_records() -> pl.LazyFrame:
    """CCCs adjusted records"""

    return await (
        await load_gsheet_data(OPS_SHEET_ID, raw_sheet)
        .filter(pl.col("Container (Destination)").str.contains(pl.lit("CCCS")))
        .select(
            pl.col("Day"),
            pl.col("Date").alias("date"),
            pl.col("Time"),
            pl.col("overtime"),
            pl.col("Storage").cast(dtype=pl.Enum(FISH_STORAGE)).alias("storage_type"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            (
                pl.col("Scale Reading(-Fish Net) (Cal)")
                .str.replace(",", "")
                .cast(pl.Int64)
                * 0.001  # Convert to Tons from Kilos
            )
            .cast(pl.Float64)
            .alias("total_tonnage"),
            pl.col("Container (Destination)").alias("destination"),
            pl.col("Species"),
        )
        .select(
            pl.all(),
            pl.col("total_tonnage")
            .sum()
            .over(["date", "vessel", "destination", "overtime", "storage_type"])
            .alias("tons"),
        )
        .with_columns(
            tonnage_select=pl.when(
                (
                    (pl.col("Day").is_in(SPECIAL_DAYS)).and_(
                        pl.col("overtime").eq(Overtime.overtime_150_text)
                    )
                ).or_(pl.col("overtime").eq(Overtime.normal_hour_text))
            )
            .then(pl.lit("normal"))
            .when(
                (pl.col("overtime").eq(Overtime.overtime_150_text)).or_(
                    pl.col("overtime").eq(Overtime.overtime_200_text)
                )
            )
            .then(pl.lit("overtime"))
            .otherwise(pl.lit("ERR"))  # To modify this for the "Invalid invoice"
        )
        .join(
            cccs_record,
            on=["date", "destination", "vessel", "storage_type"],
            how="left",
        )
        .with_columns(
            normal_tonnage=pl.col("total_tonnage_right") - pl.col("overtime_tonnage")
        )
        .with_columns(
            perc_diff=pl.when(pl.col("tonnage_select").eq(pl.lit("normal")))
            .then(pl.col("normal_tonnage") / pl.col("tons"))
            .otherwise(pl.col("overtime_tonnage") / pl.col("tons"))
        )
        .with_columns(adjusted_tonnage=pl.col("total_tonnage") * pl.col("perc_diff"))
        .group_by(["Day", "date", "overtime", "vessel", "destination", "storage_type"])
        .agg(
            start_time=pl.col("Time").min(),
            end_time=pl.col("Time").max(),
            total_tonnage=pl.col("adjusted_tonnage").sum(),
        )
        .select(
            [
                "date",
                "vessel",
                "start_time",
                "destination",
                "overtime",
                "storage_type",
                "end_time",
                "total_tonnage",
            ]
        )
    )


# The Net List


async def net_list() -> pl.LazyFrame:
    """The net list dataset"""

    return await (
        pl.concat(
            [
                await load_gsheet_data(OPS_SHEET_ID, net_list_sheet)
                .filter(~pl.col("Container (Destination)").str.contains(pl.lit("CCCS")))
                .select(
                    pl.col("Date").alias("date"),
                    pl.col("Vessel").str.to_uppercase().alias("vessel"),
                    pl.col("startTime").alias("start_time"),
                    pl.col("Container (Destination)").alias("destination"),
                    pl.col("overtime"),
                    pl.col("Storage")
                    .cast(dtype=pl.Enum(FISH_STORAGE))
                    .alias("storage_type"),
                    pl.col("endTime").alias("end_time"),
                    pl.col("Total Tonnage").alias("total_tonnage"),
                ),
                await cccs_adjusted_records(),
            ],
            how="vertical",
        )
        .sort(by="date")
        .join(
            other=stuffing_type,
            left_on=["destination", "date", "vessel"],
            right_on=["container_number", "date_plugged", "vessel_client"],
            how="left",
        )
        .with_columns(
            service=pl.when(
                (pl.col("destination").str.contains("IOT"))
                | (pl.col("destination").str.contains("DARDANEL"))
                | (pl.col("destination").str.contains("Unload to Quay"))
                | (
                    pl.col("destination")
                    .is_in(await iot_soc_enum())
                    .and_(pl.col("customer").eq(pl.lit("IOT")))
                )
            )
            .then(pl.lit("Unload to Quay"))
            .when(
                pl.col("destination")
                .str.to_uppercase()
                .is_in(await get_customer_by_type().get("cargo"))
            )
            .then(pl.lit("Transhipment"))
            .when(pl.col("destination").str.contains("CCCS"))
            .then(pl.lit("Unload to CCCS"))
            .otherwise(pl.col("stuffing"))
        )
        .select(
            pl.all().exclude(
                [
                    "operation_type",
                    "stuffing",
                    "operation_type_right",
                    "customer",
                    "stuffing_right",
                ]
            )
        )
        .with_columns(Service=pl.col("service") + " - " + pl.col("storage_type"))
        .join_asof(
            await price_list().get("unloading_price"),
            by="Service",
            on="date",
            strategy="backward",
        )
        .select(pl.all().exclude(["Service", "Date"]))
        .with_columns(
            Price=pl.when(pl.col("overtime").eq(Overtime.overtime_200_text))
            .then(pl.col("Price") * OvertimePerc.overtime_200)
            .when(pl.col("overtime").eq(Overtime.overtime_150_text))
            .then(pl.col("Price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("Price"))
        )
        .with_columns(invoice_value=pl.col("Price") * pl.col("total_tonnage"))
        # .select(pl.all().exclude(["customer"]))
    )


# Maersk OSS stuffing list ; Separated between Full and Basic OSS
async def oss() -> pl.LazyFrame:
    """oss dataset"""
    return await (
        await net_list()
        .select(pl.all().exclude(["Price", "invoice_value"]))
        .filter(pl.col("service").str.contains("OSS"))
        .with_columns(
            Service=pl.when(pl.col("service") == pl.lit("Full OSS"))
            .then(pl.lit("Container Stuffing") + " - " + pl.col("storage_type"))
            .otherwise(pl.lit("Stuffing"))
        )
        .join_asof(
            await price_list().get("oss_stuffing_price"),
            by="Service",
            on="date",
            strategy="backward",
        )
        .select(pl.all().exclude(["Service", "Date"]))
        .with_columns(
            Price=pl.when(pl.col("overtime") == Overtime.overtime_200_text)
            .then(pl.col("Price") * OvertimePerc.overtime_200)
            .when(pl.col("overtime") == Overtime.overtime_150_text)
            .then(pl.col("Price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("Price"))
        )
        .with_columns(invoice_value=pl.col("Price") * pl.col("total_tonnage"))
    )


# Create an IOT list of containers stuffed on IOT account.
async def iot_coa() -> pl.LazyFrame:
    """IOT stuffing and plugin data set"""
    await (
        await coa()
        .with_columns(
            pl.col("vessel_client").cast(pl.Utf8),
            pl.col("container_number").cast(pl.Utf8),
        )
        .select(
            [
                "vessel_client",
                "customer",
                "operation_type",
                "shipping_line",
                "date_plugged",
                "container_number",
            ]
        )
        .filter(
            pl.col("shipping_line").eq(pl.lit("IOT")),
            pl.col("operation_type").str.contains(pl.lit("Stuffing")),
        )
        .select(pl.col("*").exclude(["operation_type", "shipping_line"]))
    )


# IOT SOC Stuffing DataFrame
async def iot_stuffing() -> pl.LazyFrame:
    """IOT SOC dataset"""

    get_iot_containers: pl.Expr = pl.col("container_number").is_in(await iot_soc_enum())

    await (
        await load_gsheet_data(OPS_SHEET_ID, net_list_sheet)
        .select(
            pl.col("Date").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").alias("start_time"),
            pl.col("Container (Destination)").alias("container_number"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage"),
            pl.col("endTime").alias("end_time"),
            pl.col("Total Tonnage").alias("total_tonnage"),
        )
        .filter(get_iot_containers)
        .with_columns(
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
            Service=pl.lit("Stuffing"),
        )
        .join_asof(
            await price_list().get("stuffing_price").lazy(),
            by=None,
            left_on="date",
            right_on="Date",
            strategy="backward",
        )
        .select(pl.all().exclude(["Service"]))
        .with_columns(
            total_price=pl.when(pl.col("overtime") == "normal hours")
            .then(pl.col("total_tonnage") * pl.col("Price") * OvertimePerc.normal_hour)
            .when(pl.col("overtime") == "overtime 150%")
            .then(pl.col("total_tonnage") * pl.col("Price") * OvertimePerc.overtime_150)
            .when(pl.col("overtime") == "overtime 200%")
            .then(pl.col("total_tonnage") * pl.col("Price") * OvertimePerc.overtime_200)
            .otherwise(FREE)
        )
        .select(
            pl.col(
                [
                    "vessel",
                    "day_name",
                    "date",
                    "start_time",
                    "container_number",
                    "end_time",
                    "overtime",
                    "total_tonnage",
                    "storage",
                    "Price",
                    "total_price",
                ]
            )
        )
        .join(
            iot_coa,
            left_on=["date", "vessel", "container_number"],
            right_on=["date_plugged", "vessel_client", "container_number"],
            how="left",
        )
        .filter(pl.col("customer").eq(pl.lit("IOT")))
        .select(pl.col("*").exclude(["customer"]))
    )
