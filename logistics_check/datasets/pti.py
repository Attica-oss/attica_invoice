"""PTI datasets"""

from pathlib import Path
import polars as pl

# Logistics Record
logistics_sheet: tuple[Path, str] = (
    Path.home()
    / r"Dropbox\Container and Transport\Container Section\Storage, PTI and Container Cleaning\IPHS - PTI Wash Records.xlsx",
    "PTI",
)

invoice_sheet: Path = r"""P:\Verification & Invoicing\Validation Report\csv\pti.csv"""

logistics_df: pl.DataFrame = (
    pl.read_excel(
        logistics_sheet[0],
        sheet_name=logistics_sheet[1],
        engine="openpyxl"
    )
    # .filter(pl.col("Invoiced").ne(pl.lit("Invalid")))
    .with_columns(
        pl.col("Date Plug").dt.date(),
        pl.col("Line/Client").str.to_uppercase().alias("Client"),
    )
    .select(pl.all().exclude(["Invoiced", "#", "Verify"]))
)

invoice_df: pl.DataFrame = (
    pl.read_csv(invoice_sheet, try_parse_dates=True)
    .filter(pl.col("invoice_to").ne(pl.lit("INVALID")))
    .with_columns(pl.col("datetime_start").dt.date().alias("date"))
    .select(pl.all().exclude(["price"]))
)

pti_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Date Plug", "Container Ref. No."],
        right_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("date").is_null())
    .select(
        pl.all().exclude(
            [
                "Verify",
                "date",
                "container_number",
                "invoice_to",
                "date",
                "service_remarks",
                "hours",
                "plugin_price",
                "electricity_price",
                "no_shifting",
                "generator",
                "shifting_price",
                "total_price",

            ]
        )
    )
)

pti_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Date Plug", "Container Ref. No."],
        left_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("Date Plug").is_null())
    .select(
        pl.all().exclude(
            [
                "Date Plug",
                "Container Ref. No.",
                "Unit Manufacturer",
                "Sticker",
                "Invoiced",
                "Hours",
                "Generator"
            ]
        )
    )
    .sort(by=["date"])
)
