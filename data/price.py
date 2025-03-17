"""Pricing Module"""

from typing import Union
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, price_sheet

# from type_casting.dates import SPECIAL_DAYS

# Overtime %
OVERTIME_150: float = 1.5
OVERTIME_200: float = 2.0
NORMAL_HOUR: float = 1.0


VALIDATION_ID: str = MASTER_ID
PRICE_SHEET_NAME: str = price_sheet


# Function to load unique services
def __load_unique_services(sheet_id: str, sheet_name: str):
    """Loads unique service name from a specific gsheet"""
    return (
        load_gsheet_data(sheet_id, sheet_name)
        .select(pl.col("Service").unique())
        .collect()
        .to_series()
        .to_list()
    )


# Load unique services at runtime
services = __load_unique_services(VALIDATION_ID, PRICE_SHEET_NAME)


# Function to get the price based on service class and optional service
def get_price(service: Union[list[str], None] = None) -> pl.LazyFrame:
    """Gets the price"""
    df = (
        load_gsheet_data(VALIDATION_ID, PRICE_SHEET_NAME)
        .with_columns(
            pl.col("StartingDate").alias("Date"),
            pl.col("EndingDate")
            .str.to_date(format="%d/%m/%Y", strict=False)
            .alias("end"),
        )
        .filter(
            pl.col("Service").is_in(services)
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
]

ALL_PRICE = get_price(list_of_services)

DARDANEL_DISCOUNT: float = 4.0  # Need a better way to represent this

FREE: pl.Expr = pl.lit(0)
