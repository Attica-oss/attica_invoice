"""Transfer datasets"""

from pathlib import Path
import polars as pl

# Logistics Record
logistics_sheet: tuple[Path, str] = (
    Path.home()
    / r"Dropbox\Container and Transport\Transport Section\Container Movements\Container Transfer.xlsx",
    "Transfer",
)

invoice_sheet: Path = (
    r"""P:\Verification & Invoicing\Validation Report\csv\transfer.csv"""
)

logistics_df: pl.DataFrame = (
    pl.read_excel(
        logistics_sheet[0],
        sheet_name=logistics_sheet[1],
    ).filter(pl.col("Remarks").ne(pl.lit("INVALID")))
    # .with_columns(
    #     pl.col("Client").str.to_uppercase().alias("Client"),
    # )
    .select(
        pl.all().exclude(
            [
                "Check",
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
            ]
        )
    )
)

invoice_df: pl.DataFrame = (
    pl.read_csv(invoice_sheet, try_parse_dates=True)
    .filter(pl.col("movement_type").ne("Shifting"))
    .select(pl.all().exclude(["shifting_price", "haulage_price"]))
)

transfer_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Date", "Container Ref. No.", "Movement Type"],
        right_on=[
            "date",
            "container_number",
            "movement_type",
        ],
        how="full",
    )
    .filter(pl.col("date").is_null())
    .select(
        pl.all().exclude(
            [
                "day_name",
                "Check",
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
            ]
        )
    )
)

transfer_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Date", "Container Ref. No.", "Movement Type"],
        left_on=["date", "container_number", "movement_type"],
        how="full",
    )
    .filter(pl.col("Date").is_null(),pl.col("remarks").ne(pl.lit("CCCS")))
    .select(
        pl.all().exclude(
            [
                "Date",
                "Container Ref. No.",
                "Line/Client",
                "Movement Type",
                "Driver",
                "From",
                "Time out",
                "Destination",
                "Time in",
                "Status",
                "Type",
                "Size",
                "Remarks",
            ]
        )
    )
    .sort(by=["date"])
)
