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
    DayName,
)
from type_casting.containers import containers_enum
from type_casting.validations import STATUS_TYPE, OvertimePerc, Status

ph_list: list[date] = DayName.public_holiday_series()

# Need to make this as a LazyFrame and do a joinasof incase there is a change in price


async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    price = await get_price(["Shifting", "Haulage FEU", "Haulage TEU"])

    shifting_price = (
        price.filter(pl.col("Service").eq("Shifting"))
        .select(pl.col("Price"))
        .collect()
        .to_series()[0]
    )
    transfer_price = price.filter(
        pl.col("Service").is_in(["Haulage FEU", "Haulage TEU"])
    )

    return {
        "shifting": shifting_price,
        "transfer": transfer_price,
    }


async def shore_crane() -> pl.LazyFrame:
    """Load shore crane rental record"""

    operation_type: pl.Enum = pl.Enum(
        [
            "Stuffing",
            "Loading Provision",
            "OSS Stuffing",
            "CCCS Container Stuffing",
            "Loading to CCCS",
            "Cross Stuffing",
            "By Catch",
        ]
    )

    df = await load_gsheet_data(TRANSPORT_SHEET_ID, shore_crane_sheet)
    return df.select(
        pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
        cs.contains("date").str.to_date(format="%d/%m/%Y"),
        pl.col("start_time").str.to_time(format="%H:%M:%S"),
        pl.col("end_time").str.to_time(format="%H:%M:%S"),
        pl.col("hours").str.to_time(format="%H:%M:%S").dt.hour(),
        pl.col("overtime_hours").str.to_time(format="%H:%M").dt.hour(),
        pl.col("customer").cast(pl.Utf8),
        pl.col("operation_type").cast(dtype=operation_type),
        pl.col("remarks"),
        pl.col("invoiced_to"),
        pl.col("price").cast(pl.Float64),
        pl.col("total_price").str.replace_many(["$", ","], "").cast(pl.Float64),
    )


async def transfer() -> pl.LazyFrame:
    """Transfer (Haulage) dataset"""

    df = await load_gsheet_data(TRANSPORT_SHEET_ID, transfer_sheet)

    location: list[str] = [
        "LML",
        "HD YARD",
        "FISHING PORT",
        "CCCS",
        "IPHS",
        "IOT",
        "JHL",
        "Fishing Port",
    ]

    price = await price_list()
    shifting_price_float: float = price.get("shifting")

    transfer_price = price.get("transfer").with_columns(
        Date=pl.col("Date").str.to_date(format="%d/%m/%Y")
    )

    containers = await containers_enum()

    return (
        df.with_columns(
            pl.col("date").str.to_date(format="%d/%m/%Y"),
            pl.col("container_number").cast(dtype=containers),
            pl.col("line").cast(
                dtype=pl.Enum(
                    [
                        "CCCS",
                        "UAFL",
                        "DONGWON",
                        "SAPMER",
                        "CMA CGM",
                        "IPHS",
                        "MAERSK",
                        "IOT",
                        "PEVASA",
                    ]
                )
            ),
            pl.col("movement_type").cast(
                dtype=pl.Enum(["Collection", "Shifting", "Delivery"])
            ),
            pl.col("driver").cast(
                dtype=pl.Enum(["NA", "IPHS", "THIRD PARTY", "IPHS (Third Party)"])
            ),
            pl.col("origin").cast(dtype=pl.Enum(location)),
            pl.col("time_out").str.to_time(format="%H:%M"),
            pl.col("destination").cast(dtype=pl.Enum(location)),
            pl.col("time_in").str.to_time(format="%H:%M"),
            pl.col("status").cast(dtype=pl.Enum(["Full", "Empty"])),
            pl.col("type").cast(dtype=pl.Enum(["Reefer", "Dry"])),
            pl.col("size").cast(dtype=pl.Enum(["20'", "40'"])),
        )
        .select(pl.all().exclude("invoice_to"))
        .with_columns(
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a"))
            .cast(dtype=pl.Enum(DAY_NAMES)),
            Service=pl.when(pl.col("size").eq(pl.lit("40'", dtype=pl.Utf8)))
            .then(pl.lit("Haulage FEU"))
            .when(pl.col("size").eq(pl.lit("20'", dtype=pl.Utf8)))
            .then(pl.lit("Haulage TEU"))
            .otherwise(pl.lit("Err", dtype=pl.Utf8)),
            time=pl.when(pl.col("movement_type") == "Collection")
            .then(pl.col("time_in"))
            .when(pl.col("movement_type") == "Delivery")
            .then(pl.col("time_out"))
            .otherwise(pl.time()),
        )
        .join_asof(
            transfer_price,
            by="Service",
            right_on="Date",
            left_on="date",
            strategy="backward",
        )
        .with_columns(
            shifting_price=pl.when(
                ((pl.col("type") == "Reefer")
                & (pl.col("remarks") != "IOT")
                & (pl.col("status") == Status.full)) | (
                    pl.col("remarks").eq("CCCS").and_(pl.col("driver").eq("NA"))
                )
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


async def scow_transfer() -> pl.LazyFrame:
    """Scow Transfer/ Bin Dispatch dataset"""

    df = await load_gsheet_data(TRANSPORT_SHEET_ID, scow_sheet)

    return df.select(
        pl.col("date").str.to_date(format="%d/%m/%Y"),
        pl.col("container_number").cast(dtype=pl.Enum(["STDU6536343", "STDU6536338"])),
        pl.col("customer").cast(
            pl.Enum(
                [
                    "IOT",
                    "AQUARIUS",
                    "ECHEBASTAR",
                    "SAPMER",
                    "ISLAND CATCH",
                    "INPESCA S.A",
                ]
            )
        ),
        pl.col("movement_type").cast(pl.Enum(["Collection", "Delivery"])),
        pl.col("driver"),
        pl.col("from").cast(pl.Enum(["CCCS", "IOT", "FISHING PORT"])),
        pl.col("time_out").str.to_time(format="%H:%M:%S"),
        pl.col("destination").cast(pl.Enum(["CCCS", "IOT", "FISHING PORT"])),
        pl.col("time_in").str.to_time(format="%H:%M:%S"),
        pl.col("status").cast(dtype=pl.Enum(STATUS_TYPE)),
        pl.col("remarks"),
        pl.col("num_of_scows").cast(dtype=pl.Int64),
    ).with_columns(duration=pl.col("time_in") - pl.col("time_out"))


async def forklift() -> pl.LazyFrame:
    """Forklift dataset"""
    df = await load_gsheet_data(TRANSPORT_SHEET_ID, forklift_sheet)
    return (
        df.filter(
            ~pl.col("service_type").is_in(["Salt Loading", "Gangway"]),
            pl.col("day") != "",
        )
        .select(
            pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
            cs.contains("date").str.to_date(format="%d/%m/%Y"),
            pl.col("start_time").str.to_time(format="%H:%M:%S"),
            pl.col("end_time").str.to_time(format="%H:%M:%S"),
            pl.col("duration"),
            pl.col("customer").cast(pl.Utf8),
            pl.col("invoiced_in"),
            pl.col("service_type"),
        )
        .with_columns(
            overtime_150=pl.when(
                (pl.col("day").is_in(SPECIAL_DAYS).not_())
                .and_(pl.col("end_time").gt(UPPER_BOUND))
                .and_(pl.col("start_time").gt(UPPER_BOUND))
            )
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS).not_())
            .and_((pl.col("end_time").lt(UPPER_BOUND)))
            .then(
                (
                    pl.col("date").dt.combine(pl.col("end_time"))
                    - pl.col("date").dt.combine(UPPER_BOUND)
                ).dt.total_minutes()
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                .and_(pl.col("end_time").gt(UPPER_BOUND_SPECIAL_DAY))
                .and_(pl.col("start_time").gt(UPPER_BOUND_SPECIAL_DAY))
            )
            .then(
                (
                    pl.col("date").dt.combine(pl.col("end_time"))
                    - pl.col("date").dt.combine(pl.col("start_time"))
                ).dt.total_minutes()
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                .and_(pl.col("end_time").lt(UPPER_BOUND_SPECIAL_DAY))
                .and_(pl.col("start_time").gt(UPPER_BOUND_SPECIAL_DAY))
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
                .and_(pl.col("end_time").lt(UPPER_BOUND_SPECIAL_DAY))
                .and_(pl.col("start_time").lt(UPPER_BOUND_SPECIAL_DAY))
            ),
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            .and_(pl.col("end_time").lt(UPPER_BOUND_SPECIAL_DAY))
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
