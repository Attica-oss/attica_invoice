"""Customer Validations"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, client_sheet


async def enum_customer() -> pl.Enum:
    """Calls the master validation sheet and convert customer to an enum"""
    return pl.Enum(
        await load_gsheet_data(MASTER_ID, client_sheet)
        .select(pl.col("Vessel/Client").str.to_uppercase())
        .collect()
        .to_series()
        .unique()
        .to_list()
    )


async def customers(customer_type: str) -> list[str]:
    """Calls the master validation sheet and convert the type of customer to a list"""
    return (
        await load_gsheet_data(MASTER_ID, client_sheet)
        .filter(pl.col("Type").eq(customer_type))
        .select(pl.col("Vessel/Client"))
        .collect()
        .to_series()
        .to_list()
    )


async def ship_owner() -> list[str]:
    """Calls the master validation sheet and get a list of ship owners/operators"""
    return (
        await load_gsheet_data(MASTER_ID, client_sheet)
        .filter(pl.col("Type") == "THONIER")
        .select(pl.col("Customer"))
        .unique()
        .collect()
        .to_series()
        .to_list()
    )


async def get_customer_by_type() -> dict[str, list[str]]:
    """Get customer by type"""
    return {
        "purseiner": await customers("THONIER"),
        "longliner": await customers("LONGLINER"),
        "cargo": await customers("CARGO"),
        "supply_vessel": await customers("SUPPLY VESSEL"),
        "military_vessel": await customers("MILITARY VESSEL"),
        "tug_boat": await customers("TUG BOAT"),
        "factory": await customers("FACTORY"),
        "ship_owner_operator": await customers("SHIP OWNER"),
        "agent": await customers("AGENT"),
        "bycatch": await customers("BYCATCH"),
        "various": await customers("VARIOUS"),
        "hauler": await customers("HAULAGE"),
        # IOT is both a Factory and a Shipping Line per se.
        "shipping_line": await customers("SHIPPING LINE") + ["IOT"],
    }


async def enum_vessel() -> pl.Enum:
    """To cast vessel name"""
    df = (
        await get_customer_by_type().get("purseiner")
        + await get_customer_by_type().get("longliner")
        + await get_customer_by_type().get("cargo")
        + await get_customer_by_type().get("tug_boat")
        + await get_customer_by_type().get("supply_vessel")
        + await get_customer_by_type().get("military_vessel")
    )
    return pl.Enum(df)


# Client which handles shorecost when transporting fish in IPHS truck
# We should move this somewhere else
client_shore_cost = ["DARDANEL", "IOT"]

# List of Customers shipping

async def shipper()->list[str]:
    """Shippers"""


    return await (
    get_customer_by_type().get("agent")
        + get_customer_by_type().get("ship_owner_operator")
        + get_customer_by_type().get("shipping_line")
        + get_customer_by_type().get("bycatch")
        + [
            "IOT EXPORT",
            "CCCS",
            "IPHS",
            # "ALBACORA SA",
            # "INPESCA SA",
        ]
    )
