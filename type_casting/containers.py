"""
Containers module providing Enum types for container type safety and container list management.

This module handles loading and categorizing shipping container data from Google Sheets,
providing type-safe enumerations and filtering capabilities for container types.
"""

import asyncio
from typing import Optional, List, Dict, Any, Union, cast
from functools import lru_cache
import logging

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import TRANSPORT_SHEET_ID, transfer_sheet

# Set up logging
logger = logging.getLogger(__name__)


class ContainerManager:
    """
    Manages container data and provides various container lists as enums for type safety.
    
    This class handles loading container data from Google Sheets, caching results,
    and providing filtered subsets of containers based on various criteria.
    
    Examples:
        >>> container_mgr = ContainerManager()
        >>> all_containers = await container_mgr.get_all_containers_enum()
        >>> iot_containers = await container_mgr.get_iot_containers()
    """
    
    def __init__(self):
        """Initialize the ContainerManager with empty cache."""
        self._container_data: Optional[pl.LazyFrame] = None
        
    async def _load_container_data(self, force_reload: bool = False) -> pl.LazyFrame:
        """
        Load container data from Google Sheets.
        
        Args:
            force_reload: If True, bypass the cache and reload data
            
        Returns:
            LazyFrame containing container data
            
        Raises:
            ConnectionError: If unable to connect to Google Sheets
            ValueError: If sheet data is missing required columns
        """
        if self._container_data is None or force_reload:
            try:
                logger.info("Loading container data from sheet")
                df = await load_gsheet_data(TRANSPORT_SHEET_ID, transfer_sheet)

                # Validate required columns exist
                required_columns = ["movement_type", "container_number", "line"]
                available_columns = df.collect_schema().names()
                missing_columns = [col for col in required_columns if col not in available_columns]

                if missing_columns:
                    raise ValueError(f"Sheet missing required columns: {missing_columns}")
                
                # Filter out delivery records
                self._container_data = df.filter(pl.col("movement_type") != "Delivery")
                
            except Exception as e:
                logger.error(f"Error loading container data: {str(e)}")
                raise
                
        return self._container_data

    async def get_all_containers(self) -> pl.LazyFrame:
        """
        Get LazyFrame of all containers (excluding delivery).
        
        Returns:
            LazyFrame containing container data
        """
        return await self._load_container_data()
    
    async def get_all_containers_enum(self) -> pl.Enum:
        """
        Get all container numbers as a polars Enum.
        
        Returns:
            Enum containing all unique container numbers
            
        Examples:
            >>> container_enum = await container_mgr.get_all_containers_enum()
            >>> # Use for type checking
            >>> def process_container(container: container_enum):
            >>>     print(f"Processing {container}")
        """
        container_data = await self._load_container_data()
        
        try:
            container_list = (
                container_data
                .select(pl.col("container_number").unique())
                .collect()
                .to_series()
                .to_list()
            )
            
            if not container_list:
                logger.warning("No containers found in data source")
                
            return pl.Enum(container_list)
            
        except Exception as e:
            logger.error(f"Error creating container enum: {str(e)}")
            raise
    
    async def get_containers_by_line(self, line: str) -> List[str]:
        """
        Get container numbers filtered by shipping line.
        
        Args:
            line: Shipping line code to filter by (e.g., "IOT", "MSC")
            
        Returns:
            List of container numbers for the specified line
        """
        container_data = await self._load_container_data()
        
        return (
            container_data
            .filter(pl.col("line").eq(pl.lit(line)))
            .select(pl.col("container_number").unique())
            .collect()
            .to_series()
            .to_list()
        )
    
    async def get_iot_containers(self) -> List[str]:
        """
        Get all IOT SOC container numbers.
        
        Returns:
            List of IOT container numbers
            
        Examples:
            >>> iot_containers = await container_mgr.get_iot_containers()
            >>> print(f"Found {len(iot_containers)} IOT containers")
        """
        return await self.get_containers_by_line("IOT")

    @lru_cache(maxsize=32)
    async def get_container_details(self, container_number: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific container.
        
        Args:
            container_number: The container number to look up
            
        Returns:
            Dictionary of container details
            
        Raises:
            ValueError: If container number is not found
        """
        container_data = await self._load_container_data()

        container_info = (
            container_data
            .filter(pl.col("container_number") == container_number)
            .collect()
        )

        if container_info.height == 0:
            raise ValueError(f"Container {container_number} not found")

        # Convert first row to dict
        return container_info.row(0).to_dict()


# Create singleton instance for easy import
container_manager = ContainerManager()


# Backward compatibility functions
async def containers() -> pl.LazyFrame:
    """
    LazyFrame of containers (excluding delivery).
    
    Deprecated: Use container_manager.get_all_containers() instead.
    
    Returns:
        LazyFrame with container data
    """
    return await container_manager.get_all_containers()


async def containers_enum() -> pl.Enum:
    """
    All container numbers as an enum.
    
    Deprecated: Use container_manager.get_all_containers_enum() instead.
    
    Returns:
        Enum of all container numbers
    """
    return await container_manager.get_all_containers_enum()


async def iot_soc_enum() -> List[str]:
    """
    IOT SOC containers.
    
    Deprecated: Use container_manager.get_iot_containers() instead.
    
    Returns:
        List of IOT container numbers
    """
    return await container_manager.get_iot_containers()