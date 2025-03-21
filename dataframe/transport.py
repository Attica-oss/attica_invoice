"""Transport Lazyframe"""

from datetime import date
import polars as pl
import polars.selectors as cs
from data.price import FREE, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    TRANSPORT_SHEET_ID,
    transfer_sheet,
    shore_crane_sheet,
    forklift_sheet,
    scow_sheet,
)
from type_casting.dates import (
    DAY_NAMES,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    public_holiday,
)
from type_casting.containers import containers_enum
from type_casting.validations import STATUS_TYPE, OvertimePerc, Status

ph_list: list[date] = public_holiday()

# Need to make this as a LazyFrame and do a joinasof incase there is a change in price


async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    shifting_price = (
        await get_price(["Shifting"]).select(pl.col("Price")).collect().to_series()[0]
    )
    transfer_price = await get_price(["Haulage FEU", "Haulage TEU"])

    return {
        "shifting": shifting_price,
        "transfer": transfer_price,
    }


# SHIFTING_PRICE = get_price(["Shifting"]).select(pl.col("Price")).collect().to_series()[0]

# TRANSFER_PRICE = get_price(["Haulage FEU","Haulage TEU"])


async def shore_crane() -> pl.LazyFrame:
    """Load shore crane rental record"""
    return await load_gsheet_data(TRANSPORT_SHEET_ID, shore_crane_sheet).select(
        pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
        cs.contains("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("hours").dt.hour(),
        pl.col("overtime_hours").dt.hour(),
        pl.col("customer").cast(pl.Utf8),
        pl.col("operation_type"),
        pl.col("remarks"),
        pl.col("invoiced_to"),
        pl.col("price").cast(pl.Float64),
        pl.col("total_price").str.replace_many(["$", ","], "").cast(pl.Float64),
    )


async def transfer() -> pl.LazyFrame:
    """Transfer (Haulage) dataset"""

    shifting_price_float:float = await price_list().get("shifting_price")

    return await (
        load_gsheet_data(TRANSPORT_SHEET_ID, transfer_sheet)
        .with_columns(
            pl.col("date"),
            pl.col("container_number").cast(dtype=await containers_enum()),
            pl.col("line"),
            pl.col("movement_type").cast(
                dtype=pl.Enum(["Collection", "Shifting", "Delivery"])
            ),
            pl.col("driver").cast(
                dtype=pl.Enum(["NA", "IPHS", "THIRD PARTY", "IPHS (Third Party)"])
            ),
        )
        .select(pl.all().exclude("invoice_to"))
        .with_columns(
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
            Service=pl.when(pl.col("size") == "40'")
            .then(pl.lit("Haulage FEU"))
            .when(pl.col("size") == "20'")
            .then(pl.lit("Haulage TEU"))
            .otherwise(pl.lit("Err")),
            time=pl.when(pl.col("movement_type") == "Collection")
            .then(pl.col("time_in"))
            .when(pl.col("movement_type") == "Delivery")
            .then(pl.col("time_out"))
            .otherwise(pl.time()),
        )
        .join_asof(
            await price_list().get("transfer_price"),
            by="Service",
            right_on="Date",
            left_on="date",
            strategy="backward",
        )
        .with_columns(
            shifting_price=pl.when(
                (pl.col("type") == "Reefer")
                & (pl.col("remarks") != "IOT")
                & (pl.col("status") == Status.full)
            )
            .then(FREE)
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(shifting_price_float * OvertimePerc.overtime_200)
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (pl.col("time") > UPPER_BOUND)
            )
            .then(shifting_price_float * OvertimePerc.overtime_150)
            .otherwise(shifting_price_float),
            haulage_price=pl.when(
                (~pl.col("driver").cast(pl.Utf8).str.contains("IPHS"))
            )
            .then(pl.lit(0))
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(pl.col("Price") * OvertimePerc.overtime_200)
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (pl.col("time") > UPPER_BOUND)
            )
            .then(pl.col("Price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("Price")),
        )
        .select(
            [
                "day_name",
                "date",
                "container_number",
                "line",
                "movement_type",
                "driver",
                "origin",
                "time_out",
                "destination",
                "time_in",
                "status",
                "type",
                "size",
                "remarks",
                "shifting_price",
                "haulage_price",
            ]
        )
    )


async def scow_transfer()->pl.LazyFrame:
    """Scow Transfer/ Bin Dispatch dataset"""

    return await load_gsheet_data(TRANSPORT_SHEET_ID, scow_sheet).select(
    pl.col("date"),
    pl.col("container_number").cast(dtype=pl.Enum(["STDU6536343", "STDU6536338"])),
    pl.col("customer"),
    pl.col("movement_type"),
    pl.col("driver"),
    pl.col("from"),
    pl.col("time_out"),
    pl.col("destination"),
    pl.col("time_in"),
    pl.col("status").cast(dtype=pl.Enum(STATUS_TYPE)),
    pl.col("remarks"),
    pl.col("num_of_scows").cast(dtype=pl.Int64),
)

async def forklift()->pl.LazyFrame:
    """Forklift dataset"""

    return await (
    load_gsheet_data(TRANSPORT_SHEET_ID, forklift_sheet)
    .filter(
        ~pl.col("service_type").is_in(["Salt Loading", "Gangway"]), pl.col("day") != ""
    )
    .select(
        pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
        cs.contains("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("duration"),
        pl.col("customer").cast(pl.Utf8),
        pl.col("invoiced_in"),
        pl.col("service_type"),
    )
    .with_columns(
        overtime_150=pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS).not_()).and_(
                (pl.col("end_time").gt(UPPER_BOUND)).and_(
                    pl.col("start_time").gt(UPPER_BOUND)
                )
            )
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when((~pl.col("day").is_in(SPECIAL_DAYS)) & (pl.col("end_time") > UPPER_BOUND))
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND)
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") < UPPER_BOUND_SPECIAL_DAY)
            & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
            & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .otherwise(FREE),
        overtime_200=pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (
                (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
                & (pl.col("start_time") > UPPER_BOUND_SPECIAL_DAY)
            )
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            ).dt.total_minutes()
        )
        .otherwise(FREE),
    )
    .with_columns(
        normal_hours=(
            pl.col("date").dt.combine(pl.col("end_time"))
            - pl.col("date").dt.combine(pl.col("start_time"))
        ).dt.total_minutes()
        - (pl.col("overtime_150") + pl.col("overtime_200"))
    )
)
