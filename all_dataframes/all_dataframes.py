"""List all dataframes"""

import polars as pl

from dataframe.emr import washing, pti, shifting
from dataframe.miscellaneous import (
    by_catch,
    cccs_stuffing,
    cross_stuffing,
    # full_scow_transfer,
    static_loader,
    truck_to_cccs,
)
from dataframe.netlist import netList,oss,iot_stuffing
from dataframe.stuffing import pallet, coa
from dataframe.transport import scow_transfer, forklift, shore_crane, transfer

# Type for dataframes dictionary
DataFramesDict = dict[str, pl.LazyFrame]

try:
    # EMR LazyFrames
    emr_dataframes: DataFramesDict = {
        "cleaning": washing,
        "pti": pti,
        "shifting": shifting,
    }
    # Transport LazyFrames
    transport_dataframes: DataFramesDict = {
        "shore_crane": shore_crane,
        "transfer": transfer,
        "forklift": forklift,
        "scow_transfer": scow_transfer,
    }

    # Operations LazyFrame
    operations_dataframes: DataFramesDict = {
        "netlist": netList,
        "oss":oss,
        "iot_stuffing": iot_stuffing,
    }

    # Miscellaneous LazyFrames
    miscellaneous_dataframes: DataFramesDict = {
        "static_loader": static_loader,
        "by_catch": by_catch,
        "cross_stuffing": cross_stuffing,
        "cccs_stuffing": cccs_stuffing,
        "scow_transfer": scow_transfer,
        "truck_to_cccs": truck_to_cccs,
    }

    # Stuffing LazyFrames
    stuffing_dataframes: DataFramesDict = {
        "liner_pallet": pallet,
        "plugin": coa
    }

    # All dataframes
    all_dataframes: DataFramesDict = {
        **emr_dataframes,
        **operations_dataframes,
        **transport_dataframes,
        **miscellaneous_dataframes,
        **stuffing_dataframes
    }

except AttributeError as e:
    print(f"Error importing LazyFrames: {e}")
except ImportError as e:
    print(f"Error importing modules: {e}")
except KeyError as e:
    print(f"Key error: {e}")
except ValueError as e:
    print(f"Value error: {e}")
except TypeError as e:
    print(f"Type error: {e}")
