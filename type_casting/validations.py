"""Validations"""

from dataclasses import dataclass

# Stuffing Metrics
PALLET_TYPE: list[str] = ["Pallet", "Liner & Pallet", "Liner"]


# Miscellaneous / CCCS Metrics

# Services
BIN_DISPATCH_SERVICE: list[str] = ["Bin Dispatch to IOT", "Bin Dispatch from IOT"]
UNLOADING_SERVICE:list[str] = ['Sorting from Unloading','Unsorted from Unloading']
CARGO_DISPATCH_SERVICE:list[str] = ["Dispatch to Cargo Vessel", "Dispatch from Cargo Vessel"]

@dataclass
class MovementType:
    """Classification of Movement"""

    delivery: str = "Delivery"
    collection: str = "Collection"
    shifting: str = "Shifting"  # Not used
    external: str = "External"
    in_: str = "IN"
    out: str = "OUT"
    internal: str = "INTERNAL"


# Movement Type -> CCCS' Perspective

MOVEMENT_TYPE: list[str] = [MovementType.in_, MovementType.out]


@dataclass
class Status:
    """Check if Full or Empty"""

    full: str = "Full"
    empty: str = "Empty"

STATUS_TYPE: list[str] = [Status.full,Status.empty]

@dataclass
class FishStorage:
    """Brine or Dry"""

    brine:str = "Brine"
    dry:str="Dry"

FISH_STORAGE:list[str] =[
    FishStorage.brine,
    FishStorage.dry
]

# Overtime Labels
@dataclass
class Overtime:
    """Overtime Labels"""

    overtime_150_text:str = "overtime 150%"
    overtime_200_text:str = "overtime 200%"
    normal_hour_text:str = "normal hours"

@dataclass
class OvertimePerc:
    """Overtime Percentage rates"""
    overtime_150: float = 1.5
    overtime_200: float = 2.0
    normal_hour: float = 1.0


# Stuffing Validations

@dataclass
class PluggedStatus:
    """Check if the unit is Full, Partial of has been Completed"""

    partial: str = "Partial"
    completed: str = "Completed"
    full: str = "Full"


PLUGGED_STATUS: list[str] = [
    PluggedStatus.full,
    PluggedStatus.partial,
    PluggedStatus.completed,
]


@dataclass
class SetPoint:
    """Classifies the main 3 set point"""
    standard:str="-25"
    magnum:str="-35"
    s_freezer:str="-60"

    @classmethod
    def list_all(cls)->list[str]:
        """list all set point"""
        return [cls.standard,cls.magnum,cls.s_freezer]


SETPOINTS:list[str] = [
    SetPoint.standard,
    SetPoint.magnum,
    SetPoint.s_freezer
]
