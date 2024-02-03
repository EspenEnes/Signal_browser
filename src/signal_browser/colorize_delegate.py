from PySide6 import QtWidgets, QtGui


class ColorizeDelegate(QtWidgets.QStyledItemDelegate):
    """
    Class: ColorizeDelegate

        An item delegate class for colorizing the background and text of items in a view.

    Inherits from:
        QtWidgets.QStyledItemDelegate

    Methods:
        initStyleOption(option, index)
            - Initializes the style options for the item at the given index.
    """

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        item = option.widget.model().itemFromIndex(index)

        b_unit = item.itemData.b_unit
        c_unit = item.itemData.c_unit
        name = item.itemData.name

        if b_unit and c_unit and name:
            option.backgroundBrush = QtGui.QColor('Yellow')
            option.text = f'{name} [{b_unit}->{c_unit}]'
