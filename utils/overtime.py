"""Overtime utilities"""

from dataclasses import dataclass
import polars as pl

@dataclass
class OvertimeType:
    """Individual overtime type with text and rate"""
    text: str
    rate: pl.Float32

@dataclass
class Overtime:
    """Overtime configuration"""
    normal_hour: OvertimeType = OvertimeType("normal hours", pl.Float32(1.0))
    overtime_150: OvertimeType = OvertimeType("overtime 150%", pl.Float32(1.5))
    overtime_200: OvertimeType = OvertimeType("overtime 200%", pl.Float32(2.0))


# Overtime %
OVERTIME_150: Overtime = Overtime.overtime_150.rate
OVERTIME_200: Overtime = Overtime.overtime_200.rate
NORMAL_HOUR: Overtime = Overtime.normal_hour.rate
