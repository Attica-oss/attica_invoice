
"""Stores all dataframes as organized collections with improved performance and structure"""

"""Stores all dataframes as list and dicts"""

from typing import Dict, Awaitable
import asyncio
import polars as pl
from dataframe import (
    bin_dispatch,
    emr,
    miscellaneous,
    netlist,
    operations,
    shore_handling,
    stuffing,

    transport,
)

# Type aliases for better readability
LazyFrameDict = Dict[str, pl.LazyFrame]
AsyncLazyFrameDict = Dict[str, Awaitable[pl.LazyFrame]]


class DataFrameCollections:
    """Manages and loads all dataframe collections with concurrent loading support."""

    @staticmethod
    async def load_emr_dataframes() -> LazyFrameDict:
        """Load EMR-related dataframes concurrently."""
        tasks = {
            "shifting": emr.shifting(),
            "washing": emr.washing(),
            "pti": emr.pti(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_bin_dispatch_dataframes() -> LazyFrameDict:
        """Load bin dispatch dataframes concurrently."""
        tasks = {
            "full_scows_transfer": bin_dispatch.full_scows(),
            "empty_scows_transfer": bin_dispatch.empty_scows(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_miscellaneous_dataframes() -> LazyFrameDict:
        """Load miscellaneous dataframes concurrently."""
        tasks = {
            "static_loader": miscellaneous.static_loader(),
            "dispatch_to_cargo": miscellaneous.dispatch_to_cargo(),
            "truck_to_cccs": miscellaneous.truck_to_cccs(),
            "cross_stuffing": miscellaneous.cross_stuffing(),
            "cccs_stuffing": miscellaneous.cccs_stuffing(),
            "bycatch": miscellaneous.by_catch(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_netlist_dataframes() -> LazyFrameDict:
        """Load netlist dataframes concurrently."""
        tasks = {
            "net_list": netlist.net_list(),
            "iot_container_stuffing": netlist.iot_stuffing(),
            "oss_stuffing": netlist.oss(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_operations_dataframes() -> LazyFrameDict:
        """Load operations dataframes concurrently."""
        tasks = {
            "ops": operations.ops(),
            "hatch_to_hatch": operations.hatch_to_hatch(),
            # TODO: Uncomment when ready
            # "extramen": operations.extramen(),
            # "additional_overtime": operations.additional(),
            # "tare_calibration": operations.tare()
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_shore_handling_dataframes() -> LazyFrameDict:
        """Load shore handling dataframes concurrently."""
        tasks = {
            "salt": shore_handling.salt(),
            "forklift_salt": shore_handling.forklift_salt(),
            "bin_tipping": shore_handling.bin_tipping(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_stuffing_dataframes() -> LazyFrameDict:
        """Load stuffing dataframes concurrently."""
        tasks = {"pallet_liner": stuffing.pallet(), "container_plugin": stuffing.coa()}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }

    @staticmethod
    async def load_transport_dataframes() -> LazyFrameDict:
        """Load transport dataframes concurrently."""
        tasks = {
            "shore_crane": transport.shore_crane(),
            "transfer": transport.transfer(),
            "scow_transfer": transport.scow_transfer(),
            "forklift": transport.forklift(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }


async def all_dataframes() -> Dict[str, LazyFrameDict]:
    """
    Load all available dataframes organized by category.

    Returns:
        Dict containing all dataframe collections organized by category.
        Each category contains a dictionary of dataframe name -> LazyFrame.

    Raises:
        Exception: If critical dataframes fail to load (customize as needed).

    Example:
        dfs = await all_dataframes()
        emr_shifting = dfs["emr_dataframes"]["shifting"]
    """
    # Load all dataframe collections concurrently
    collection_tasks = {
        "emr_dataframes": DataFrameCollections.load_emr_dataframes(),
        "bin_dispatch_dataframes": DataFrameCollections.load_bin_dispatch_dataframes(),
        "miscellaneous_dataframes": DataFrameCollections.load_miscellaneous_dataframes(),
        "netlist_dataframes": DataFrameCollections.load_netlist_dataframes(),
        "operations_dataframes": DataFrameCollections.load_operations_dataframes(),
        "shore_handling_dataframes": DataFrameCollections.load_shore_handling_dataframes(),
        "stuffing_dataframes": DataFrameCollections.load_stuffing_dataframes(),
        "transport_dataframes": DataFrameCollections.load_transport_dataframes(),
    }

    # Execute all collection loading concurrently
    results = await asyncio.gather(*collection_tasks.values(), return_exceptions=True)

    # Build final result dictionary
    final_results = {}
    for name, result in zip(collection_tasks.keys(), results):
        if isinstance(result, Exception):
            print(f"Warning: Failed to load {name}: {result}")
            final_results[name] = {}  # Empty dict for failed collections
        else:
            final_results[name] = result

    return final_results


# Alternative: Lazy loading approach for memory efficiency
class LazyDataFrameCollections:
    """Alternative implementation with lazy loading for memory efficiency."""

    def __init__(self):
        self._dataframes: Dict[str, Dict[str, Awaitable[pl.LazyFrame]]] = {}
        self._initialize_collections()

    def _initialize_collections(self):
        """Initialize all dataframe collections without loading them."""
        self._dataframes = {
            "emr_dataframes": {
                "shifting": emr.shifting(),
                "washing": emr.washing(),
                "pti": emr.pti(),
            },
            "bin_dispatch_dataframes": {
                "full_scows_transfer": bin_dispatch.full_scows(),
                "empty_scows_transfer": bin_dispatch.empty_scows(),
            },
            "miscellaneous_dataframes": {
                "static_loader": miscellaneous.static_loader(),
                "dispatch_to_cargo": miscellaneous.dispatch_to_cargo(),
                "truck_to_cccs": miscellaneous.truck_to_cccs(),
                "cross_stuffing": miscellaneous.cross_stuffing(),
                "cccs_stuffing": miscellaneous.cccs_stuffing(),
                "bycatch": miscellaneous.by_catch(),
            },
            "netlist_dataframes": {
                "net_list": netlist.net_list(),
                "iot_container_stuffing": netlist.iot_stuffing(),
                "oss_stuffing": netlist.oss(),
            },
            "operations_dataframes": {
                "ops": operations.ops(),
                # "extramen":operations.extramen,
                "hatch_to_hatch": operations.hatch_to_hatch(),
                # "additional_overtime":operations.additional,
                # "tare_calibration":operations.tare
            },
            "shore_handling_dataframes": {
                "salt": shore_handling.salt(),
                "forklift_salt": shore_handling.forklift_salt(),
                "bin_tipping": shore_handling.bin_tipping(),
            },
            "stuffing_dataframes": {
                "pallet_liner": stuffing.pallet(),
                "container_plugin": stuffing.coa(),
            },
            "transport_dataframes": {
                "shore_crane": transport.shore_crane(),
                "transfer": transport.transfer(),
                "scow_transfer": transport.scow_transfer(),
                "forklift": transport.forklift(),
            }
        }

    async def get_dataframe(self, collection: str, name: str) -> pl.LazyFrame:
        """Get a specific dataframe by collection and name."""
        if collection not in self._dataframes:
            raise ValueError(f"Unknown collection: {collection}")
        if name not in self._dataframes[collection]:
            raise ValueError(f"Unknown dataframe '{name}' in collection '{collection}'")

        return await self._dataframes[collection][name]

    async def get_collection(self, collection: str) -> LazyFrameDict:
        """Get all dataframes in a specific collection."""
        if collection not in self._dataframes:
            raise ValueError(f"Unknown collection: {collection}")

        tasks = self._dataframes[collection]
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: result
            for name, result in zip(tasks.keys(), results)
            if not isinstance(result, Exception)
        }


# """Stores all dataframes as list and dicts"""
# from typing import Awaitable
# import polars as pl
# from dataframe import (
#     bin_dispatch,
#     emr,
#     miscellaneous,
#     netlist,
#     operations,
#     shore_handling,
#     stuffing,
#     transport
# )


# DictLazyFrame = dict[str, Awaitable[pl.LazyFrame]]


# # EMR Dataframes

# async def all_dataframes()->dict[str,DictLazyFrame]:
#     """All avaiable dataframes"""
#     emr_dataframes:DictLazyFrame = {
#     "shifting": await emr.shifting(),
#     "washing": await emr.washing(),
#     "pti": await emr.pti(),}
#     # Miscellaneous Daframes

#     bin_dispatch_dataframes: DictLazyFrame = {
#     "full_scows_transfer": await bin_dispatch.full_scows(),
#     "empty_scows_transfer": await bin_dispatch.empty_scows(),
#     }

#     miscellaneous_dataframes:DictLazyFrame = {
#     "static_loader":await miscellaneous.static_loader(),
#     "dispatch_to_cargo":await miscellaneous.dispatch_to_cargo(),
#     "truck_to_cccs":await miscellaneous.truck_to_cccs(),
#     "cross_stuffing":await miscellaneous.cross_stuffing(),
#     "cccs_stuffing":await miscellaneous.cccs_stuffing(),
#     "bycatch":await miscellaneous.by_catch()
#     }

#     netlist_dataframes :DictLazyFrame={
#     "net_list":await netlist.net_list(),
#     "iot_container_stuffing":await netlist.iot_stuffing(),
#     "oss_stuffing":await netlist.oss()
#     }

#     operations_dataframes: DictLazyFrame={
#     "ops":await operations.ops(),
#     # "extramen":operations.extramen,
#     "hatch_to_hatch":await operations.hatch_to_hatch(),
#     # "additional_overtime":operations.additional,
#     # "tare_calibration":operations.tare
#     }

#     shore_handling_dataframes:DictLazyFrame={
#     "salt":await shore_handling.salt(),
#     "forklift_salt":await shore_handling.forklift_salt(),
#     "bin_tipping":await shore_handling.bin_tipping()
#     }

#     stuffing_dataframes:DictLazyFrame = {
#     "pallet_liner":await stuffing.pallet(),
#     "container_plugin":await stuffing.coa()
#     }

#     transport_dataframes:DictLazyFrame={
#     "shore_crane":await transport.shore_crane(),
#     "transfer":await transport.transfer(),
#     "scow_transfer":await transport.scow_transfer(),
#     "forklift":await transport.forklift()
#     }

#     return {
#         "emr_dataframes":emr_dataframes,
#         "bin_dispatch_dataframes":bin_dispatch_dataframes,
#         "miscellaneous_dataframes":miscellaneous_dataframes,
#         "netlist_dataframes":netlist_dataframes,
#         "operations_dataframes":operations_dataframes,
#         "shore_handling_dataframes":shore_handling_dataframes,
#         "stuffing_dataframes":stuffing_dataframes,
#         "transport_dataframes":transport_dataframes
#     }

    transport
)

# EMR Dataframes

emr_dataframes: dict[str, pl.LazyFrame] = {
    "shifting": emr.shifting,
    "washing": emr.washing,
    "pti": emr.pti,
}

# Miscellaneous Daframes

bin_dispatch_dataframes: dict[str, pl.LazyFrame] = {
    "full_scows_transfer": bin_dispatch.full_scows,
    "empty_scows_transfer": bin_dispatch.empty_scows,
}

miscellaneous_dataframes:dict[str,pl.LazyFrame] = {
    "static_loader":miscellaneous.static_loader,
    "dispatch_to_cargo":miscellaneous.dispatch_to_cargo,
    "truck_to_cccs":miscellaneous.truck_to_cccs,
    "cross_stuffing":miscellaneous.cross_stuffing,
    "cccs_stuffing":miscellaneous.cccs_stuffing,
    "bycatch":miscellaneous.by_catch
}

netlist_dataframes :dict[str,pl.LazyFrame]={
    "net_list":netlist.netList,
    "iot_container_stuffing":netlist.iot_stuffing,
    "oss_stuffing":netlist.oss
}

operations_dataframes: dict[str,pl.LazyFrame]={
    "ops":operations.ops,
    "extramen":operations.extramen,
    "hatch_to_hatch":operations.hatch_to_hatch,
    # "additional_overtime":operations.additional,
    "tare_calibration":operations.tare
}

shore_handling_dataframes:dict[str,pl.LazyFrame]={
    "salt":shore_handling.salt,
    "bin_tipping":shore_handling.bin_tipping
}

stuffing_dataframes:dict[str,pl.LazyFrame] = {
    "pallet_liner":stuffing.pallet,
    "container_plugin":stuffing.coa
}

transport_dataframes:dict[str,pl.LazyFrame]={
    "shore_crane":transport.shore_crane,
    "transfer":transport.transfer,
    "scow_transfer":transport.scow_transfer,
    "forklift":transport.forklift
}

# all_dataframes:dict[str,pl.LazyFrame] = {
#     **emr_dataframes,
#     **netlist_dataframes,
#     **operations_dataframes,
#     **transport_dataframes,
#     **miscellaneous_dataframes,
#     **stuffing_dataframes,
#     **bin_dispatch_dataframes,
#     **shore_handling_dataframes
# }

