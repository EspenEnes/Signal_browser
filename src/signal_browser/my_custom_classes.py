from dataclasses import dataclass

from PySide6 import QtGui, QtCore
from PySide6.QtGui import QStandardItem


@dataclass
class ItemData:
    """
    The ItemData class represents a data object that holds various properties for an item.
    """
    id: str | int = None
    name: str = None
    node: str = None
    secondary_y: bool = False
    data_type: str | type = None
    b_unit: str = None
    c_unit: str = None
    trace_uid: str = None
    costum_color: str = None

class CustomStandardItem(QStandardItem):
    """
    Subclass of QStandardItem that extends the functionality to store additional data.

    Attributes:
        _item_data (ItemData): The instance of ItemData that stores additional data for the item.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._item_data = ItemData()
        self._item_data.name = self.text()

    def setItemData(self, **kwargs):
        for attr, value in kwargs.items():
            if hasattr(self._item_data, attr):
                setattr(self._item_data, attr, value)

    def getItemData(self) -> ItemData:
        return self._item_data

    @property
    def itemData(self):
        return self._item_data






class CustomStandardItemModel(QtGui.QStandardItemModel):
    """

    CustomStandardItemModel(QtGui.QStandardItemModel)

    A custom implementation of the QtGui.QStandardItemModel class.

    Attributes:
        checkStateChanged (QtCore.Signal): A signal that is emitted when the check state of an item changes.

    Methods:
        __init__(self, parent=None)
            Initializes a new instance of the CustomStandardItemModel class.

        data(self, index, role=QtCore.Qt.ItemDataRole.DisplayRole)
            Returns the data stored under the given role for the item at the specified index.

        setData(self, index, value, role=QtCore.Qt.ItemDataRole.DisplayRole)
            Sets the data for the item at the specified index with the specified value and role.

    """

    checkStateChanged = QtCore.Signal(CustomStandardItem)

    def __init__(self, parent=None):
        super().__init__(parent)

    def data(self, index, role=QtCore.Qt.ItemDataRole.DisplayRole):
        return super().data(index, role)

    def setData(self, index, value, role=QtCore.Qt.ItemDataRole.DisplayRole):
        state = self.data(index, QtCore.Qt.ItemDataRole.CheckStateRole)

        if role == QtCore.Qt.ItemDataRole.CheckStateRole and state != value:
            item = self.itemFromIndex(index)
            result = super().setData(index, value, role)
            self.checkStateChanged.emit(item)
            return result

        return super().setData(index, value, role)

