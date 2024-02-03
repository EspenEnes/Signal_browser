from enum import Enum, auto


class FileType(Enum):
    """
    FileType is an enumeration class that represents different types of file formats.
    """

    TDM = auto()
    DAT = auto()
    DB = auto()
    PLC_LOG = auto()
    NONE = auto()
