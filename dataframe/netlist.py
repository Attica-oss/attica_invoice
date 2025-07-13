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
    DayName,
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

ph_list: pl.Series = DayName.public_holiday_series()


# Price
async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    price = await get_price()

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

    stuffing_price = price.filter(
        pl.col("Service").eq(pl.lit("Stuffing"))
    ).with_columns(date=pl.col("Date").str.to_date(format="%d/%m/%Y"))

    unloading_price: pl.LazyFrame = price.filter(
        pl.col("Service").is_in(service_list)
    ).with_columns(date=pl.col("Date").str.to_date(format="%d/%m/%Y"))

    oss_stuffing_price: pl.LazyFrame = price.filter(
        pl.col("Service").is_in(oss_service_list)
    ).with_columns(date=pl.col("Date").str.to_date(format="%d/%m/%Y"))

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
    df = await coa()
    return (
        df.select(
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
    df = await miscellaneous()
    return (
        (
            df.filter(
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


# .str.to_date(format="%d/%m/%Y")
async def cccs_adjusted_records() -> pl.LazyFrame:
    """CCCs adjusted records"""

    df = await load_gsheet_data(OPS_SHEET_ID, raw_sheet)
    cccs = await cccs_record()

    return (
        df.filter(pl.col("Container (Destination)").str.contains(pl.lit("CCCS")))
        .select(
            pl.col("Day"),
            pl.col("Date").str.to_date(format="%d/%m/%Y").alias("date"),
            pl.col("Time").str.to_time(format="%H:%M:%S"),
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
            cccs,
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
    df = await load_gsheet_data(OPS_SHEET_ID, net_list_sheet)
    df_cccs_adjusted = await cccs_adjusted_records()
    price = await price_list()
    unloading_price = price.get("unloading_price")
    stuffing_type_df = await stuffing_type()

    iot_enum = await iot_soc_enum()

    customer = await get_customer_by_type()
    cargo = customer.get("cargo")

    return (
        pl.concat(
            [
                df.filter(
                    ~pl.col("Container (Destination)").str.contains(pl.lit("CCCS"))
                ).select(
                    pl.col("Date").str.to_date(format="%d/%m/%Y").alias("date"),
                    pl.col("Vessel").str.to_uppercase().alias("vessel"),
                    pl.col("startTime")
                    .str.to_time(format="%H:%M:%S")
                    .alias("start_time"),
                    pl.col("Container (Destination)").alias("destination"),
                    pl.col("overtime"),
                    pl.col("Storage")
                    .cast(dtype=pl.Enum(FISH_STORAGE))
                    .alias("storage_type"),
                    pl.col("endTime").str.to_time(format="%H:%M:%S").alias("end_time"),
                    pl.col("Total Tonnage").alias("total_tonnage"),
                ),
                df_cccs_adjusted,
            ],
            how="vertical",
        )
        .sort(by="date")
        .join(
            other=stuffing_type_df,
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
                    .is_in(iot_enum)
                    .and_(pl.col("customer").eq(pl.lit("IOT")))
                )
            )
            .then(pl.lit("Unload to Quay"))
            .when(pl.col("destination").str.to_uppercase().is_in(cargo))
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
            unloading_price,
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

    df = await net_list()
    price = await price_list()
    oss_price = price.get("oss_stuffing_price")
    return (
        df.select(pl.all().exclude(["Price", "invoice_value"]))
        .filter(pl.col("service").str.contains("OSS"))
        .with_columns(
            Service=pl.when(pl.col("service").eq(pl.lit("Full OSS")))
            .then(pl.lit("Container Stuffing") + " - " + pl.col("storage_type"))
            .otherwise(pl.lit("Stuffing"))
        )
        .join_asof(
            oss_price,
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

    df = await coa()
    return (
        df.with_columns(
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

    df = await load_gsheet_data(OPS_SHEET_ID, net_list_sheet)

    iot_df = await iot_coa()

    iot_soc = await iot_soc_enum()
    price = await price_list()
    stuffing_price = price.get("stuffing_price")

    get_iot_containers: pl.Expr = pl.col("container_number").is_in(iot_soc)

    return (
        df.select(
            pl.col("Date").str.to_date(format="%d/%m/%Y").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").str.to_time(format="%H:%M:%S").alias("start_time"),
            pl.col("Container (Destination)").alias("container_number"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage"),
            pl.col("endTime").str.to_time(format="%H:%M:%S").alias("end_time"),
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
            stuffing_price,
            by=None,
            left_on="date",
            right_on="date",
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
            iot_df,
            left_on=["date", "vessel", "container_number"],
            right_on=["date_plugged", "vessel_client", "container_number"],
            how="left",
        )
        .filter(pl.col("customer").eq(pl.lit("IOT")))
        .select(pl.col("*").exclude(["customer"]))
    )
