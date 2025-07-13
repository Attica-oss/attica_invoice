"""Operations Lazyframe for parquet storage only"""

# from pathlib import Path
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import OPS_SHEET_ID, raw_sheet, WELL_TO_WELL
from type_casting.validations import FISH_STORAGE
from type_casting.validations import OvertimePerc

from type_casting.dates import SPECIAL_DAYS, DayName, DAY_NAMES

# from dataframe import invoice

from data.price import get_price


# To move the Price to the Price module


# EXTRAMEN: float = get_price(["Extra Men"]).with_columns(date=pl.col("Date"))
# Price
async def price_list() -> dict[str, float | pl.LazyFrame]:
    """price dictionary"""

    # liner_price = await (
    #     get_price(["Plastic Liner Installation"])
    #     .select(pl.col("Price"))
    #     .collect()
    #     .to_series()[0]
    # )

    price = await get_price([
        "Well to Well Transfer"
    ])

    well_to_well_price: pl.LazyFrame =price.filter(pl.col("Service").eq(
        "Well to Well Transfer"
    )).with_columns(date=pl.col("Date"))

    return {
        "well_to_well_price": well_to_well_price,
    }


# TARE_RATE: pl.LazyFrame = get_price(
#     ["Rental of Calibration", "Tare Calibration"]
# ).with_columns(date=pl.col("Date"))


# ADDITIONAL_OVERTIME: pl.LazyFrame = (
#     get_price(["Additional Overtime"]).with_columns(date=pl.col("Date")).drop("Date")
# )


# OPS_ACTIVITY_PATH = r"C:\Users\gmounac\Dropbox\
# ! OPERATION SUPPORTING DOCUMENTATION\2024\2024 IPHS operation activity.xlsx"


# Operations Activity Unloading Lazyframe


async def ops() -> pl.LazyFrame:
    """Ops Activity"""
    df = await load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=raw_sheet)
    return df.select(
        pl.col("Day"),
        pl.col("Date"),
        pl.col("Time"), # .str.to_time(format="%H:%M:%S")
        pl.col("Vessel").str.to_uppercase(),
        pl.col("Species").str.extract(r"^(.*?)(\s-\s)"),
        pl.col("Details").str.to_uppercase(),
        pl.col("Scale Reading(-Fish Net) (Cal)")
        .str.replace(",", "")
        .cast(pl.Int64)
        .alias("tonnage")
        * 0.001,
        pl.col("Storage").cast(dtype=pl.Enum(FISH_STORAGE)),
        pl.col("Container (Destination)").alias("destination"),
        pl.col("overtime"),
        pl.col("Side Working").alias("side_working"),
    )


def add_day_name_column(date_col: pl.Expr) -> pl.Expr:
    """adds the day name based on the date column name this includes public holiday (PH)"""

    return (
        pl.when(date_col.is_in(DayName.public_holiday_series()))
        .then(pl.lit(DayName.PH.value))
        .otherwise(date_col.dt.to_string(format="%a"))
    ).cast(dtype=pl.Enum(DAY_NAMES))


# main_file = (
#     pl.read_excel(OPS_ACTIVITY_PATH, sheet_name="HANDLING ACTIVITY", engine="calamine")
#     .filter(pl.col("DAY") != "", pl.col("DAY") != "Total")
#     .lazy()
# )

# handling_activity = main_file.select(
#     pl.col("DATE").alias("date"),
#     pl.col("VESSEL NAME").str.to_uppercase().alias("vessel_name"),
#     pl.col("OPERATION TYPE"),
#     pl.col("BRINE (SAUMURE)"),
#     pl.col("DRY (Below -30°C)"),
#     pl.col("TOTAL TONNAGE"),
#     pl.col("Well-to-Well Transfer").fill_null(0),
#     pl.col("Overtime Tonnage"),
#     pl.col("Extra Men").fill_null(0).cast(pl.Int32).alias("extra_men"),
#     pl.col("Number of Stevedores").fill_null(0).cast(pl.Int32),
#     pl.col("OPEX"),
#     pl.col("OPEX %"),
#     pl.col("Comments").alias("remarks"),
# ).with_columns(day_name=add_day_name_column(pl.col("date")))


# Extra men service


# extramen: pl.DataFrame = (
#     handling_activity.select(
#         pl.col("day_name"),
#         pl.col("date"),
#         pl.col("vessel_name"),
#         pl.col("TOTAL TONNAGE").alias("total_tonnage"),
#         pl.col("extra_men"),
#     )
#     .with_columns(Service=pl.lit("Extra Men"))
#     .filter(pl.col("extra_men") > 0)
#     .join_asof(EXTRAMEN, by="Service", on="date", strategy="backward")
#     .with_columns(
#         total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
#         .then(
#             OvertimePerc.overtime_150
#             * pl.col("Price")
#             * pl.col("total_tonnage")
#             * pl.col("extra_men")
#         )
#         .otherwise(
#             OvertimePerc.normal_hour
#             * pl.col("Price")
#             * pl.col("total_tonnage")
#             * pl.col("extra_men")
#         )
#     )
#     .select(pl.all().exclude(["Service", "Date"]))
# )


# Well to well transfer


async def hatch_to_hatch() -> pl.LazyFrame:
    """Well to Well transfer"""
    df = await load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=WELL_TO_WELL)
    price = await price_list()
    well_to_well = price.get("well_to_well_price")
    return (
        df.select(
            pl.col("Day").alias("day_name"),
            pl.col("Date").alias("date"),
            pl.col("Vessel").alias("vessel_name"),
            pl.col("Tonnage").alias("Well-to-Well Transfer"),
        )
        .with_columns(Service=pl.lit("Well to Well Transfer"))
        .sort(by="date")
        .join_asof(well_to_well.sort(by="date"), by="Service", on="date", strategy="backward")
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                OvertimePerc.overtime_150
                * pl.col("Price")
                * pl.col("Well-to-Well Transfer")
            )
            .otherwise(
                OvertimePerc.normal_hour
                * pl.col("Price")
                * pl.col("Well-to-Well Transfer")
            )
        )
        .select(pl.all().exclude(["Service", "Date"]))
    )


# Rental of Calibration Weight Service

# tare: pl.DataFrame = (
#     main_file.filter(pl.col("DAY") != "OT")
#     .select(
#         pl.col("DATE").alias("date"),
#         pl.col("VESSEL NAME").alias("vessel"),
#         pl.col("Comments").str.to_lowercase(),
#     )
#     .with_columns(
#         rental_of_weight=pl.lit(1),
#         sides_working=pl.when(
#             (pl.col("Comments").str.contains("front"))
#             & (pl.col("Comments").str.contains("back"))
#             & (pl.col("Comments").str.contains("middle"))
#         )
#         .then(pl.lit(3))
#         .when(
#             (
#                 (pl.col("Comments").str.contains("front"))
#                 & (pl.col("Comments").str.contains("back"))
#             )
#             | (
#                 (
#                     (pl.col("Comments").str.contains("front"))
#                     & (pl.col("Comments").str.contains("middle"))
#                 )
#             )
#             | (
#                 (
#                     (pl.col("Comments").str.contains("back"))
#                     & (pl.col("Comments").str.contains("middle"))
#                 )
#             )
#             | (pl.col("Comments").str.contains("both sides"))
#         )
#         .then(pl.lit(2))
#         .otherwise(pl.lit(1)),
#     )
#     .with_columns(
#         sides_remarks=pl.when(
#             (pl.col("Comments").str.contains("front"))
#             & (pl.col("Comments").str.contains("back"))
#             & (pl.col("Comments").str.contains("middle"))
#         )
#         .then(pl.lit("Front, Middle and Back"))
#         .when(
#             (
#                 (pl.col("Comments").str.contains("front"))
#                 & (pl.col("Comments").str.contains("back"))
#             )
#             | (pl.col("Comments").str.contains("both sides"))
#         )
#         .then(pl.lit("Front and Back"))
#         .when(
#             (
#                 (pl.col("Comments").str.contains("front"))
#                 & (pl.col("Comments").str.contains("middle"))
#             )
#         )
#         .then(pl.lit("Front and Middle"))
#         .when(
#             (
#                 (pl.col("Comments").str.contains("back"))
#                 & (pl.col("Comments").str.contains("middle"))
#             )
#         )
#         .then(pl.lit("Middle and Back"))
#         .when((pl.col("Comments").str.contains("front")))
#         .then(pl.lit("Front"))
#         .when((pl.col("Comments").str.contains("back")))
#         .then(pl.lit("Back"))
#         .when((pl.col("Comments").str.contains("middle")))
#         .then(pl.lit("Middle"))
#         .otherwise(pl.lit("One Side"))
#     )
#     .with_columns(Service=pl.lit("Rental of Calibration"))
#     .sort(by="date")
#     .join_asof(TARE_RATE, by="Service", on="date", strategy="backward")
#     .select(pl.all().exclude(["Service", "Date"]))
#     .with_columns(
#         Service=pl.lit("Tare Calibration"),
#         rental_price=pl.col("Price").alias("rental_price"),
#     )
#     .drop("Price")
#     .join_asof(TARE_RATE, by="Service", on="date", strategy="backward")
#     .select(pl.all().exclude(["Service", "Date"]))
#     .with_columns(calibration_price=pl.col("Price") * pl.col("sides_working"))
#     .with_columns(total_price=pl.col("rental_price") + pl.col("calibration_price"))
#     .select(
#         pl.col("date"),
#         pl.col("vessel"),
#         pl.col("rental_of_weight"),
#         pl.col("sides_remarks").alias("sides_working"),
#         pl.col("rental_price"),
#         pl.col("calibration_price"),
#         pl.col("total_price"),
#         pl.col("sides_working").alias("number_of_sides"),
#     )
# )
