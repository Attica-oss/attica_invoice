"""Customer Validations"""
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MASTER_ID, client_sheet

# Add a cache to avoid repeated data fetching
_customer_cache = {}
_customer_type_cache = {}

async def enum_customer() -> pl.Enum:
    """Calls the master validation sheet and convert customer to an enum"""
    df = await load_gsheet_data(MASTER_ID, client_sheet)
    return pl.Enum(
        df.select(pl.col("Vessel/Client").str.to_uppercase())
        .collect()
        .to_series()
        .unique()
        .to_list()
    )

async def fetch_customers_by_type(customer_type: str) -> list[str]:
    """Base function that fetches customers of a specific type without circular dependencies"""
    # Check cache first
    cache_key = f"customer_type_{customer_type}"
    if cache_key in _customer_cache:
        return _customer_cache[cache_key]
    
    # Fetch data if not in cache
    df = await load_gsheet_data(MASTER_ID, client_sheet)
    result = (
        df.filter(pl.col("Type").eq(pl.lit(customer_type)))
        .select(pl.col("Vessel/Client"))
        .collect()
        .to_series()
        .to_list()
    )
    # Cache the result
    _customer_cache[cache_key] = result
    return result

# Replace the old function with a wrapper around the base function
async def customers(customer_type: str) -> list[str]:
    """Calls the master validation sheet and convert the type of customer to a list"""
    return await fetch_customers_by_type(customer_type)

async def ship_owner() -> list[str]:
    """Calls the master validation sheet and get a list of ship owners/operators"""
    df = await load_gsheet_data(MASTER_ID, client_sheet)
    return (
        df.filter(pl.col("Type").eq(pl.lit("THONIER")))
        .select(pl.col("Customer"))
        .unique()
        .collect()
        .to_series()
        .to_list()
    )

async def get_customer_by_type() -> dict[str, list[str]]:
    """Get customer by type"""
    # Check cache first
    if _customer_type_cache:
        return _customer_type_cache.copy()  # Return a copy to prevent modification
    
    # Build the dictionary using the base function
    result = {
        "purseiner": await fetch_customers_by_type("THONIER"),
        "longliner": await fetch_customers_by_type("LONGLINER"),
        "cargo": await fetch_customers_by_type("CARGO"),
        "supply_vessel": await fetch_customers_by_type("SUPPLY VESSEL"),
        "military_vessel": await fetch_customers_by_type("MILITARY VESSEL"),
        "tug_boat": await fetch_customers_by_type("TUG BOAT"),
        "factory": await fetch_customers_by_type("FACTORY"),
        "ship_owner_operator": await fetch_customers_by_type("SHIP OWNER"),
        "agent": await fetch_customers_by_type("AGENT"),
        "bycatch": await fetch_customers_by_type("BYCATCH"),
        "various": await fetch_customers_by_type("VARIOUS"),
        "hauler": await fetch_customers_by_type("HAULAGE"),
        # IOT is both a Factory and a Shipping Line per se.
        "shipping_line": await fetch_customers_by_type("SHIPPING LINE") + ["IOT"],
    }

    # Cache the result
    _customer_type_cache.update(result)
    return result

async def enum_vessel() -> pl.Enum:
    """To cast vessel name"""
    vessel = await get_customer_by_type()
    df = (
        vessel.get("purseiner")
        + vessel.get("longliner")
        + vessel.get("cargo")
        + vessel.get("tug_boat")
        + vessel.get("supply_vessel")
        + vessel.get("military_vessel")
    )
    return df

# Client which handles shorecost when transporting fish in IPHS truck
# We should move this somewhere else
client_shore_cost: list[str] = ["DARDANEL", "IOT"]

# List of Customers shipping
async def shipper() -> list[str]:
    """Shippers"""
    customer = await get_customer_by_type()
    return (
        customer.get("agent")
        + customer.get("ship_owner_operator")
        + customer.get("shipping_line")
        + customer.get("bycatch")
        + [
            "IOT EXPORT",
            "CCCS",
            "IPHS",
            # "ALBACORA SA",
            # "INPESCA SA",
        ]
    )
