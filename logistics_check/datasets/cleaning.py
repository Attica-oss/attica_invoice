"""Cleaning datasets"""

from pathlib import Path
import polars as pl

# Logistics Record
logistics_sheet: tuple[Path, str] = (
    Path.home()
    / r"Dropbox\Container and Transport\Container Section\Storage, PTI and Container Cleaning\IPHS - PTI Wash Records.xlsx",
    "Washing",
)

invoice_sheet: Path = Path(r"P:\Verification & Invoicing\Validation Report\csv\washing.csv")

logistics_df: pl.DataFrame = (
    pl.read_excel(
        logistics_sheet[0],
        sheet_name=logistics_sheet[1],
    )
    .filter(pl.col("Invoiced").ne(pl.lit("Invalid")))
    .with_columns(
        pl.col("Client").str.to_uppercase().alias("Client"),
    )
    .select(pl.all().exclude(["Invoiced", "Check", "Verify"]))
)

invoice_df: pl.DataFrame = (
    pl.read_csv(invoice_sheet, try_parse_dates=True)
    .filter(pl.col("invoice_to").ne(pl.lit("INVALID")))
    .select(pl.all().exclude(["price"]))
)

cleaning_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Cleaning Date", "Container Ref. No."],
        right_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("date").is_null())
    .select(
        pl.all().exclude(
            ["Verify", "date", "container_number", "invoice_to", "service_remarks"]
        )
    )
)

cleaning_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Cleaning Date", "Container Ref. No."],
        left_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("Cleaning Date").is_null())
    .select(
        pl.all().exclude(
            [
                "Cleaning Date",
                "Container Ref. No.",
                "Client",
                "Visual Inspection/Remarks",
            ]
        )
    )
    .sort(by=["date"])
)
