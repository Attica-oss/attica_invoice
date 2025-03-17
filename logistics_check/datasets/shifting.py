"""Shifting datasets"""

from pathlib import Path
import polars as pl

# Logistics Record
logistics_sheet: tuple[Path, str] = (
    Path.home()
    / r"Dropbox\Container and Transport\Transport Section\Container Shifting Records\IPHS Container Shifting Record.xlsx",
    "Container Shifting",
)

invoice_sheet: Path = (
    r"""P:\Verification & Invoicing\Validation Report\csv\shifting.csv"""
)

logistics_df: pl.DataFrame = (
    pl.read_excel(
        logistics_sheet[0],
        sheet_name=logistics_sheet[1],
    )
    .filter(pl.col("Invoiced").ne(pl.lit("INVALID")))
    .with_columns(
        pl.col("Client").str.to_uppercase().alias("Client"),
    )
    .select(pl.all().exclude(["Invoiced"]))
)

invoice_df: pl.DataFrame = pl.read_csv(invoice_sheet, try_parse_dates=True).select(
    pl.all().exclude(["price"])
)

shifting_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Date Shifted", "Container Ref. No."],
        right_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("date").is_null())
    .select(
        pl.all().exclude(
            [
                "Verify",
                "day_name",
                "date",
                "container_number",
                "invoice_to",
                "service_remarks",
            ]
        )
    )
)

shifting_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Date Shifted", "Container Ref. No."],
        left_on=["date", "container_number"],
        how="full",
    )
    .filter(pl.col("Date Shifted").is_null())
    .select(
        pl.all().exclude(
            ["Date Shifted", "Container Ref. No.", "Verify", "Client", "Remarks"]
        )
    )
    .sort(by=["date"])
)
