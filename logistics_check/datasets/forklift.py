"""Forklift datasets"""

from pathlib import Path
import polars as pl

# Logistics Record
logistics_sheet: tuple[Path, str] = (
    Path.home()
    / r"Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx",
    "Forklift_Operation",
)

invoice_sheet: Path = (
    r"""P:\Verification & Invoicing\Validation Report\csv\forklift.csv"""
)

logistics_df: pl.DataFrame = (
    pl.read_excel(
        logistics_sheet[0],
        sheet_name=logistics_sheet[1],
        schema_overrides={"Time Out": pl.Time, "Time In": pl.Time},
    )
    .filter(pl.col("Purpose").str.contains(pl.lit("Salt loading|Load Salt|Salt Loading")).not_())
    .with_columns(
        pl.col("Vessel/Client").str.to_uppercase().alias("Vessel/Client"),
        Duration=(
            pl.col("Date of Service").dt.combine(pl.col("Time In"))
            - pl.col("Date of Service").dt.combine(pl.col("Time Out"))
        ).dt.total_minutes()
        / 60,
    )
    .select(pl.all().exclude(["Invoiced in:"]))
)

invoice_df: pl.DataFrame = pl.read_csv(invoice_sheet, try_parse_dates=True).select(
    pl.all().exclude(["invoiced_in"])
)

forklift_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Date of Service", "Time Out", "Time In", "Vessel/Client"],
        right_on=["date", "start_time", "end_time", "customer"],
        how="full",
    )
    .filter(pl.col("date").is_null())
    .select(
        pl.all().exclude(
            [
                "day",
                "date",
                "start_time",
                "end_time",
                "duration",
                "customer",
                "service_type",
                "overtime_150",
                "overtime_200",
                "normal_hours",
            ]
        )
    )
)

forklift_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Date of Service", "Time Out", "Time In", "Vessel/Client"],
        left_on=["date", "start_time", "end_time", "customer"],
        how="full",
    )
    .filter(pl.col("Date of Service").is_null())
    .select(
        pl.all().exclude(
            [
                "overtime_150",
                "overtime_200",
                "normal_hours",
                "Date of Service",
                "Driver",
                "Forklift No.",
                "Time Out",
                "Time In",
                "Duration",
                "Vessel/Client",
                "Purpose",
            ]
        )
    ).sort(by=["date","start_time"])
)
