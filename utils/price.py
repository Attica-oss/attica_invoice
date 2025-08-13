"""Pricing Module"""

from typing import Union, Self
import polars as pl
from polars import DataType

from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, price_sheet

from utils.load_google_sheet import load_google_sheet
from utils.save.to_parquet import write_parquet_if_changed
from utils.result import Result

VALIDATION_ID: str = MASTER_ID
PRICE_SHEET_NAME: str = price_sheet


class Price(DataType):
    """Price DataType"""

    service_type: pl.Utf8
    price: pl.Float64
    effective_date: pl.Date
    has_variant: pl.Boolean = True


def load_price() -> "Result[pl.LazyFrame]":
    """load the price dataset into a lazyframe"""
    df = load_google_sheet(config_name="price", parse_dates=True)
    return df


def clean_price() -> pl.LazyFrame:
    """clean the price dataset"""

    df = load_price().data
    return df.select(
        pl.col("Service").cast(dtype=pl.Utf8).alias("service"),
        pl.col("Price").cast(dtype=pl.Float64).alias("price"),
        pl.col("StartingDate").alias("effective_date"),
    )


def save_price() -> None:
    """Saves the price dataset to parquet"""
    return write_parquet_if_changed(clean_price(), verbose=False)


# Function to load unique services
async def __load_unique_services(sheet_id: str, sheet_name: str):
    """Loads unique service name from a specific gsheet"""

    df = await load_gsheet_data(sheet_id, sheet_name)
    return df.select(pl.col("Service").unique()).collect().to_series().to_list()


# Load unique services at runtime


async def services() -> list:
    """Services"""
    service_list = await __load_unique_services(VALIDATION_ID, PRICE_SHEET_NAME)
    return service_list


# Function to get the price based on service class and optional service
async def get_price(service: Union[list[str], None] = None) -> pl.LazyFrame:
    """Gets the price"""

    price_df = await load_gsheet_data(VALIDATION_ID, PRICE_SHEET_NAME)
    service_df = await services()

    df = (
        price_df.with_columns(
            pl.col("StartingDate").alias("Date"),
            pl.col("EndingDate")
            .str.to_date(format="%d/%m/%Y", strict=False)
            .alias("end"),
        )
        .filter(
            pl.col("Service").is_in(service_df)
            if service is None
            else pl.col("Service").is_in(service)
        )
        .select(["Service", "Price", "Date"])
    )
    return df


list_of_services = [
    "CCCS Movement in/out",  # BIN_TIPPING_PRICE, CCCS_MOVEMENT_FEE SCOW_TRANSFER
    "Loading (Quay to Ship)",  # SALT PRICE
    "Loading @ Zone 14",  # SALT PRICE
    "Tipping Truck",  # TRUCK PRICE
    "Loading to Cargo",  # CARGO_LOADING_PRICE
    "Transfer of by-catch",  # CCCS_TRUCK, BY_CATCH_PRICE
    "Cross Stuffing",  # CROSS_STUFFING_PRICE
    "Unstuffing by Hand",  # CROSS_STUFFING_PRICE
    "Unstuffing to Cargo",  # CROSS_STUFFING_PRICE
    "Unstuffing to CCCS",  # CROSS_STUFFING_PRICE
    "CCCS (By-Catch)",  # BY_CATCH_PRICE
    "Shore Crane & Fishloader",  # CCCS_STUFFING_PRICE
    "Shore Crane & Fishloader (by catch)",  # CCCS_STUFFING_PRICE
    "Static Loader",  # CCCS_STUFFING_PRICE STATIC_LOADER
    "Container Stuffing by Hand",  # CCCS_STUFFING_PRICE
    "Shifting",  # SHIFTING SHIFTING_PRICE
    "Plugin",  # PLUGIN PLUGIN_PRICE
    "PTI S Freezer",  # S_FREEZER_PTI_ELECTRICITY
    "PTI Magnum",  # MAGNUM_PTI_ELECTRICITY
    "PTI Standard",  # STANDARD_PTI_ELECTRICITY
    "Electricity Price Standard",  # IOT_ELECTRICITY
    "Container Cleaning",  # WASHING
    "Stuffing",  # STUFFING_PRICE
    "Monitoring",  # MONITORING_PRICE
    "Electricity Price S Freezer",  # S_FREEZER_ELECTRICITY
    "Electricity Price Magnum",  # MAGNUM_ELECTRICITY
    "Electricity Price Standard",  # STANDARD_ELECTRICITY
    "Pallets",  # PALLET_PRICE
    "Plastic Liner Installation",  # LINER_PRICE
    "Pallets(+ Wedges) Usage",  # PALLET_IOT_PRICE
    "Haulage FEU",  # TRANSFER_PRICE
    "Haulage TEU"  # TRANSFER_PRICE
    "Container Stuffing - Brine",  # OSS_PRICE
    "Container Stuffing - Dry",  # OSS_PRICE
    "Forklift Scow Handling",
]

# ALL_PRICE = get_price(list_of_services)

DARDANEL_DISCOUNT: float = 4.0  # Need a better way to represent this

FREE: pl.Expr = pl.lit(0)
