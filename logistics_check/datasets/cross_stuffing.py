"""Cross Stuffing datasets"""

from pathlib import Path
import polars as pl

# Logistics Record


logistics_sheet: tuple[Path, str] = (
    Path.home() / r"Dropbox\Container and Transport\Container Section\Container Operations Activity\Container Operation Activity.xlsx",
    "Cross-Stuffing",
)

# inv_dropbox_folder = (
#     rf"""{Path.home()}\Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2024"""
# )

invoice_sheet: Path = (
    r"""P:\Verification & Invoicing\Validation Report\csv\cross_stuffing.csv"""
)

logistics_df: pl.DataFrame = pl.read_excel(
    logistics_sheet[0],
    sheet_name=logistics_sheet[1],
)

invoice_df: pl.DataFrame = (
    pl.read_csv(invoice_sheet, try_parse_dates=True)
    .filter(pl.col("invoiced").ne(pl.lit("INVALID")))
    .select(pl.all().exclude(["Price", "total_price"]))
)

cross_stuffing_log_df = (
    logistics_df.join(
        other=invoice_df,
        left_on=["Date", "From Container Ref . No."],
        right_on=["date", "origin"],
        how="full",
    ).filter(pl.col("date").is_null())
    # .select(
    #     pl.all().exclude(
    #         ["Verify", "date", "container_number", "invoice_to", "service_remarks"]
    #     )
    # )
)

cross_stuffing_inv_df = (
    invoice_df.join(
        other=logistics_df,
        right_on=["Date", "From Container Ref . No."],
        left_on=["date", "origin"],
        how="full",
    ).filter(pl.col("Date").is_null())
    # .select(
    #     pl.all().exclude(
    #         [
    #             "Cleaning Date",
    #             "Container Ref. No.",
    #             "Client",
    #             "Visual Inspection/Remarks",
    #         ]
    #     )
    # )
    # .sort(by=["date"])
)
