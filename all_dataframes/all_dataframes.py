"""Stores all dataframes as list and dicts"""
from typing import Awaitable
import polars as pl
from dataframe import (
    bin_dispatch,
    emr,
    miscellaneous,
    netlist,
    operations,
    shore_handling,
    stuffing,
    transport
)

DictLazyFrame = dict[str, Awaitable[pl.LazyFrame]]



# EMR Dataframes

emr_dataframes:DictLazyFrame = {
    "shifting": emr.shifting(),
    "washing": emr.washing(),
    "pti": emr.pti(),
}

# Miscellaneous Daframes

bin_dispatch_dataframes: DictLazyFrame = {
    "full_scows_transfer": bin_dispatch.full_scows(),
    "empty_scows_transfer": bin_dispatch.empty_scows(),
}

miscellaneous_dataframes:DictLazyFrame = {
    "static_loader":miscellaneous.static_loader(),
    "dispatch_to_cargo":miscellaneous.dispatch_to_cargo(),
    "truck_to_cccs":miscellaneous.truck_to_cccs(),
    "cross_stuffing":miscellaneous.cross_stuffing(),
    "cccs_stuffing":miscellaneous.cccs_stuffing(),
    "bycatch":miscellaneous.by_catch()
}

netlist_dataframes :DictLazyFrame={
    "net_list":netlist.net_list(),
    "iot_container_stuffing":netlist.iot_stuffing(),
    "oss_stuffing":netlist.oss()
}

operations_dataframes: DictLazyFrame={
    "ops":operations.ops(),
    # "extramen":operations.extramen,
    "hatch_to_hatch":operations.hatch_to_hatch(),
    # "additional_overtime":operations.additional,
    # "tare_calibration":operations.tare
}

shore_handling_dataframes:DictLazyFrame={
    "salt":shore_handling.salt(),
    "bin_tipping":shore_handling.bin_tipping()
}

stuffing_dataframes:DictLazyFrame = {
    "pallet_liner":stuffing.pallet(),
    "container_plugin":stuffing.coa()
}

transport_dataframes:DictLazyFrame={
    "shore_crane":transport.shore_crane(),
    "transfer":transport.transfer(),
    "scow_transfer":transport.scow_transfer(),
    "forklift":transport.forklift()
}
