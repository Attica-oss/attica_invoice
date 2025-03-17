"""Main entry point"""

import os
from datetime import date
import warnings
import polars as pl

import pretty_errors


from logistics_check.datasets.forklift import forklift_inv_df, forklift_log_df
from logistics_check.datasets.shifting import shifting_inv_df, shifting_log_df
from logistics_check.datasets.transfer import transfer_inv_df, transfer_log_df
from logistics_check.datasets.cleaning import cleaning_inv_df, cleaning_log_df
from logistics_check.datasets.cross_stuffing import cross_stuffing_inv_df, cross_stuffing_log_df
from logistics_check.datasets.pti import pti_inv_df,pti_log_df
from logistics_check.dates.dates import month_number_to_dates
from data.dataframes import print_dataframe



warnings.filterwarnings("ignore")

pretty_errors.activate()

def difference_dfs(
    df1: pl.DataFrame,
    df2: pl.DataFrame,
    df1_date_col: str,
    df2_date_col: str,
    date_tuple: tuple[date, date],
) -> None:
    """Prints the difference rows in two dfs"""
    print("Logistics Data with difference: ")
    print()
    df_log = df1.filter(pl.col(df1_date_col).is_between(*date_tuple))
    df_inv = df2.filter(pl.col(df2_date_col).is_between(*date_tuple))
    if df_log.is_empty():
        print("No issues!")
        print()
    else:
        print_dataframe(df_log)
        # df_log.write_clipboard()
    print("Invoice Data with difference: ")
    if df_inv.is_empty():
        print("No Issues!")
        print()
    else:
        print_dataframe(df_inv)


def check_data():
    """main file"""
    select_year = int(input("Select the year (skip for current year): "))
    select_month = int(input("Select the month number: "))
    data = month_number_to_dates(select_month,select_year)
    os.system("cls")
    msg = "Choose which services to inspect!"
    print(msg)
    print("*" * len(msg))
    msg_1 = """Select: \n1. Forklift\n2. Shifting\n3. Transfer\n4. Cleaning\n5. Cross Stuffing\n6. PTI\n"""
    select_services = int(input(msg_1))
    os.system("cls")
    if select_services == 1:
        print("Forklift Services")
        print("-" * 17)
        difference_dfs(
            forklift_log_df, forklift_inv_df, "Date of Service", "date", data
        )
    elif select_services == 2:
        print("Shifting Services")
        print("-" * 17)
        difference_dfs(shifting_log_df, shifting_inv_df, "Date Shifted", "date", data)
    elif select_services == 3:
        print("Transfer Services")
        print("-" * 17)
        difference_dfs(transfer_log_df, transfer_inv_df, "Date", "date", data)
    elif select_services == 4:
        print("Cleaning Services")
        print("-" * 17)
        difference_dfs(cleaning_log_df, cleaning_inv_df, "Cleaning Date", "date", data)
    elif select_services == 5:
        print("Cross Stuffing / Unstuffing Services")
        print("-" * 27)
        difference_dfs(
            cross_stuffing_log_df, cross_stuffing_inv_df, "Date", "date", data
        )
    elif select_services == 6:
        print("Pre Trip Inspection")
        print("-" * 27)
        difference_dfs(
            pti_log_df, pti_inv_df, "Date Plug", "date", data
        )



