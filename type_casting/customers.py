"""Customer Validations"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, client_sheet


CUSTOMERS: pl.LazyFrame = load_gsheet_data(MASTER_ID, client_sheet)


def enum_customer() -> pl.Enum:
    """Calls the master validation sheet and convert customer to an enum"""
    return pl.Enum(
        CUSTOMERS.select(pl.col("Vessel/Client").str.to_uppercase())
        .collect()
        .to_series()
        .unique()
        .to_list()
    )


def customers(customer_type: str) -> list[str]:
    """Calls the master validation sheet and convert the type of customer to a list"""
    return (
        CUSTOMERS.filter(pl.col("Type") == customer_type)
        .select(pl.col("Vessel/Client"))
        .collect()
        .to_series()
        .to_list()
    )


def ship_owner() -> list[str]:
    """Calls the master validation sheet and get a list of ship owners/operators"""
    return (
        CUSTOMERS.filter(pl.col("Type") == "THONIER")
        .select(pl.col("Customer"))
        .unique()
        .collect()
        .to_series()
        .to_list()
    )


purseiner = customers("THONIER")
longliner = customers("LONGLINER")
cargo = customers("CARGO")
supply_vessel = customers("SUPPLY VESSEL")
military_vessel = customers("MILITARY VESSEL")
tug_boat = customers("TUG BOAT")

vessels: list[str] = (
    purseiner + longliner + cargo + tug_boat + supply_vessel + military_vessel
)
enum_vessel: pl.Enum = pl.Enum(vessels)

factory = customers("FACTORY")
ship_owner_operator = customers("SHIP OWNER")

agent = customers("AGENT")
bycatch = customers("BYCATCH")
various = customers("VARIOUS")

hauler = customers("HAULAGE")

# IOT is both a Factory and a Shipping Line per se.
shipping_line = customers("SHIPPING LINE") + ["IOT"]

# Client which handles shorecost when transporting fish in IPHS truck
# We should move this somewhere else
client_shore_cost = ["DARDANEL", "IOT"]

# List of Customers shipping

shipper = (
    agent
    + ship_owner_operator
    + shipping_line
    + bycatch
    + [
        "IOT EXPORT",
        "CCCS",
        "IPHS",
        # "ALBACORA SA",
        # "INPESCA SA",
    ]
)
