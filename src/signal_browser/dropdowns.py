import sqlite3

import numpy as np
from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import pandas as pd
import pathlib
import plotly.graph_objects as go
import pint
from PySide6.QtGui import QPalette

from .utils import get_darkModePalette
from .colorize_delegate import ColorizeDelegate
from .file_type import FileType
from .my_custom_classes import CustomStandardItemModel, CustomStandardItem
from .novos_processes import NOVOSProcesses
from .mmc_processes import MMCProcesses
from .plclog_reader import PlcLogReader_Async
from .tdmlog_reader import TdmGetGroupsWorker, TdmGetChannelsWorker, TdmGetDataWorker
from .rtilog_reader import RTILogReader, MultiThreaded_RTI_Reader
from .qt_dash import DashThread


class MainWindow(QtWidgets.QMainWindow):
    """
    The MainWindow class represents the main window of the application. It inherits from the QtWidgets.QMainWindow class.
    """

    def __init__(self, parent=None, port=8050, app=None):
        super().__init__(parent)
        self.app = app
        self.resize(800, 600)
        self._host = "127.0.0.1"
        self._port = port
        self.ureg = pint.UnitRegistry()
        self.DASH_URL = f"http://{self._host}:{port}"
        self.init_ui_elements_and_vars()
        self.create_layout()
        self.create_menubar()
        self.connect_signals()
        self.fig = self.qdask.fig
        self.thread_pool = QtCore.QThreadPool()

    def init_ui_elements_and_vars(self):
        """Initializes the main window and UI elements"""
        self.file_type = FileType.NONE
        self.setWindowTitle("Signal Viewer")
        self._standard_model = CustomStandardItemModel(self)
        self._tree_view = QtWidgets.QTreeView(self)
        self._tree_view.setModel(self._standard_model)
        self._load_icon = QtGui.QIcon(str(pathlib.Path(__file__).parent.joinpath("Loading_icon2.png")))
        delegate = ColorizeDelegate(self._tree_view)
        self._tree_view.setItemDelegate(delegate)
        self.qdask = DashThread(host=self._host, port=self._port)
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        self.browser.load(QtCore.QUrl(self.DASH_URL))
        self.qdask.start()
        self._tree_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)

    def create_layout(self):
        """Creates the layout for the main window"""
        self.splitter = QtWidgets.QSplitter(self)
        self.splitter.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.splitter.addWidget(self._tree_view)
        self.splitter.addWidget(self.browser)
        self.setCentralWidget(self.splitter)
        self.splitter.setSizes([200, 400])
        self.splitter.setStretchFactor(1, 1)

    def dark_mode(self, dark):
        if dark:
            self.app.setPalette(get_darkModePalette(self.app))
            self.app.setStyle("fusion")
        else:
            self.app.setPalette(QPalette())

    def connect_signals(self):
        """Connects the signals to the slots"""
        self._tree_view.doubleClicked.connect(self.on_double_clicked)
        self._standard_model.checkStateChanged.connect(self.on_channel_checkbox)
        self._tree_view.customContextMenuRequested.connect(self.open_context_menu)
        self.qdask.theme_manager.is_dark_changed.connect(self.dark_mode)

        self.actionOpenFile.triggered.connect(self.on_actionOpenFile_triggered)
        self.actionShowNovosProcess.triggered.connect(self.show_novos_process)
        self.actionShowSignalBrowser.triggered.connect(self.show_signal_browser)
        self.actionShowMMCProcess.triggered.connect(self.show_mmc_process)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowSignalBrowser.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(False)

    def create_menubar(self):
        """Creates the menu bar and adds the open file action"""
        self.menubar = QtWidgets.QMenuBar(self)

        self.menuFile = QtWidgets.QMenu(self.menubar, title="File")
        self.menuView = QtWidgets.QMenu(self.menubar, title="View")

        self.actionOpenFile = QtGui.QAction(self, text="Open")
        self.actionShowNovosProcess = QtGui.QAction(self, text="Show Novos Process")
        self.actionShowMMCProcess = QtGui.QAction(self, text="Show MMC Process")
        self.actionShowSignalBrowser = QtGui.QAction(self, text="Show Signal Browser")

        self.menuFile.addAction(self.actionOpenFile)
        self.menuView.addAction(self.actionShowSignalBrowser)
        self.menuView.addAction(self.actionShowNovosProcess)
        self.menuView.addAction(self.actionShowMMCProcess)

        self.menubar.addAction(self.menuFile.menuAction())
        self.menubar.addAction(self.menuView.menuAction())
        self.setMenuBar(self.menubar)

    def show_signal_browser(self):
        self.qdask.update_graph(self.fig)
        self.browser.reload()

        if self.file_type == FileType.PLC_LOG:
            self.actionShowMMCProcess.setEnabled(True)
        else:
            self.actionShowNovosProcess.setEnabled(True)

    def show_novos_process(self):
        self.fig2 = NOVOSProcesses.make_plotly_figure(self.filenames)
        if not self.fig2:
            dialog = QtWidgets.QDialog()
            dialog.setWindowTitle("No Data")
            layout = QtWidgets.QVBoxLayout()
            label = QtWidgets.QLabel(dialog)
            label.setText("There is no Novos Process data to show.")
            layout.addWidget(label)
            dialog.setLayout(layout)
            dialog.exec()
            return

        self.qdask.update_graph(self.fig2)
        self.browser.reload()

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowSignalBrowser.setEnabled(True)

    def show_mmc_process(self):
        self.fig2 = MMCProcesses.make_plotly_figure(self.log_file)

        self.qdask.update_graph(self.fig2)
        self.browser.reload()

        self.actionShowMMCProcess.setEnabled(False)
        self.actionShowSignalBrowser.setEnabled(True)

    def on_actionOpenFile_triggered(self):
        """Opens a file and adds the groups to the tree view"""

        self.file_type = FileType.NONE
        self.filenames, _ = QtWidgets.QFileDialog.getOpenFileNames(
            self,
            "Open File",
            "",
            "TDM (*.tdm *.dat *.db *.zip)",
        )
        if len(self.filenames) == 0:
            return

        self.filename = self.filenames[0]

        self._standard_model.clear()
        self.qdask.new_graph()
        self.browser.reload()

        if pathlib.Path(self.filename).suffix.lower() in [".dat", ".db"]:
            self.load_dat_groups(self.filenames)
            self.file_type = FileType.DAT
        elif pathlib.Path(self.filename).suffix.lower() == ".tdm":
            worker = TdmGetGroupsWorker(self.filename)
            worker.signals.Groups_Signal.connect(self.load_tdm_groups)
            self.thread_pool.start(worker)

        elif pathlib.Path(self.filename).suffix.lower() == ".zip":
            self.load_PlcLog_file(self.filenames)
            self.file_type = FileType.PLC_LOG

    def open_context_menu(self, position):
        # Get the index of the item at the position where right-click was performed
        index = self._tree_view.indexAt(position)
        if not index.isValid():
            return

        item = self._tree_view.model().itemFromIndex(index)

        if item.itemData.node != "leaf":
            return

        item = self._tree_view.model().itemFromIndex(index)
        menu = QtWidgets.QMenu()

        action1 = menu.addAction("Select and add to secondary axis")
        action2 = menu.addAction("Unit Conversion")
        action3 = menu.addAction("Pen Color")
        action1.triggered.connect(lambda: self.open_context_menu_secondary_y(item))
        action2.triggered.connect(lambda: self.unit_convertion(item))
        action3.triggered.connect(lambda: self.open_color_picker(item))

        action2.setEnabled(True)
        if item.checkState() == QtCore.Qt.CheckState.Checked:
            action2.setEnabled(False)

        if item.itemData.data_type == str:
            action2.setEnabled(False)

        # Show the context menu
        menu.exec_(self._tree_view.viewport().mapToGlobal(position))

    def open_color_picker(self, item: CustomStandardItem):
        if _color := item.itemData.costum_color:
            color = QtWidgets.QColorDialog.getColor(QtGui.QColor(_color))
        else:
            color = QtWidgets.QColorDialog.getColor()

        if color.isValid():
            item.itemData.costum_color = color.name()

    def unit_convertion(self, item: CustomStandardItem):
        base_unit, conc_unit = self.open_unit_convertion_dialog()
        item.setItemData(b_unit=base_unit, c_unit=conc_unit)

    def open_unit_convertion_dialog(self):
        dialog = QtWidgets.QDialog()
        dialog.setWindowTitle("Unit Conversion")
        layout = QtWidgets.QVBoxLayout()

        input1 = QtWidgets.QLineEdit(dialog)
        input1.setPlaceholderText("Base Unit")

        input2 = QtWidgets.QLineEdit(dialog)
        input2.setPlaceholderText("Conversion Unit")

        buttonBox = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)

        # Connect the signals to the slots
        buttonBox.accepted.connect(dialog.accept)
        buttonBox.rejected.connect(dialog.reject)
        layout.addWidget(input1)
        layout.addWidget(input2)
        layout.addWidget(buttonBox)

        dialog.setLayout(layout)
        if dialog.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            unit1, unit2 = self.validate_unit_convertion(input1.text(), input2.text())
            if (
                    (unit1 is not None) and (unit2 is not None) and (unit1 != "") and (unit2 != "")
            ):  # todo This looks like shit
                return unit1, unit2
            else:
                return None, None
        else:
            return None, None

    def validate_unit_convertion(self, input1, input2):
        msg_box = QtWidgets.QMessageBox()
        msg_box.setIcon(QtWidgets.QMessageBox.Icon.Warning)
        msg_box.setWindowTitle("Alert: Wrong units")
        try:
            base_unit = self.ureg[input1]
        except pint.UndefinedUnitError:
            msg_box.setText(f"Base unit {input1} is not a valid unit of measurement")
            msg_box.exec()
            return None, None

        try:
            conv_unit = self.ureg[input2]
        except pint.UndefinedUnitError:
            msg_box.setText(f"Converted unit {input2} is not a valid unit of measurement")
            msg_box.exec()
            return None, None

        if base_unit.is_compatible_with(conv_unit):
            return input1, input2
        else:
            msg_box.setText(f"{input1} and {input2} is not compatible units")
            msg_box.exec()
            return None, None

    def open_context_menu_secondary_y(self, item: CustomStandardItem):
        item.itemData.secondary_y = True
        item.setCheckState(QtCore.Qt.CheckState.Checked)
        self.on_channel_checkbox(item)

    def on_double_clicked(self, index: QtCore.QModelIndex):
        """Finds the channel names and adds them to the tree view"""
        item = self._tree_view.model().itemFromIndex(index)

        if item.itemData.node != "root":
            return

        if item.rowCount() > 0:
            return

        if self.file_type == FileType.TDM:
            self.set_load_icon(item)
            group = item.itemData.id
            worker = TdmGetChannelsWorker(self.filename, index, group)
            worker.signals.Channels_Signal.connect(self.load_tdm_channels)
            self.thread_pool.start(worker)

        elif self.file_type == FileType.DAT:
            self.load_dat_channels(index)

    def load_PlcLog_file(self, filename):
        self.log_file = PlcLogReader_Async.read_logfile(filename)

        root_node = self._standard_model.invisibleRootItem()
        for channel in self.log_file.columns:
            channel_node = self.create_channel_item(channel, channel)
            root_node.appendRow(channel_node)
        self._standard_model.setHorizontalHeaderLabels(["Signals"])

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(True)
        self._tree_view.setSortingEnabled(True)

    def load_tdm_groups(self, groups):
        root_node = self._standard_model.invisibleRootItem()
        for group in groups:
            group_node = CustomStandardItem(f"{group}")
            group_node.setEditable(False)
            group_node.setItemData(id=group, node="root")
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(False)
        self._tree_view.setSortingEnabled(False)

        self.file_type = FileType.TDM

    def load_dat_groups(self, filenames):
        valid_tables = []

        # read all files and find all tables that contains rti_json_sample that are not none
        # todo Add this to a thread worker
        for filename in filenames:
            with sqlite3.connect(filename) as conn:
                cur = conn.cursor()
                for table in RTILogReader.get_tables_contains(cur, "rti_json_sample"):
                    if RTILogReader._validate_rti_json_sample(cur, table):
                        if f"{table}" not in valid_tables:
                            valid_tables.append(f"{table}")

        root_node = self._standard_model.invisibleRootItem()
        for table in valid_tables:
            group_node = CustomStandardItem(f"{table}")
            group_node.setEditable(False)
            group_node.setItemData(id=table, node="root")
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.actionShowNovosProcess.setEnabled(True)
        self._tree_view.setSortingEnabled(False)

    def load_tdm_channels(self, data):
        index, channels = data
        group_node = self._tree_view.model().itemFromIndex(index)

        for ix, name in channels:
            channel_node = self.create_channel_item(name, ix)
            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.remove_load_icon(group_node)

    def load_dat_channels(self, index: QtCore.QModelIndex):
        """Handles DAT file type"""
        item = self._tree_view.model().itemFromIndex(index)

        if item.itemData.node != "root":
            return
        table = item.itemData.id
        group_node = self._tree_view.model().itemFromIndex(index)
        group_node.setEditable(False)
        channels = {}

        # todo Add this to a thread worker
        self.set_load_icon(group_node)
        for filename in self.filenames:
            with sqlite3.connect(filename) as conn:
                cur = conn.cursor()
                tables = RTILogReader.get_all_tables(cur)
                if table not in tables:
                    continue

                channels_ = RTILogReader.get_channels_from_rti_json_sample(cur, table)
                channels.update((k, v) for k, v in channels_.items() if k not in channels)

        for key, value in channels.items():
            name = key
            channel_node = self.create_channel_item(name, name, data_type=value)
            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.remove_load_icon(group_node)

    def create_channel_item(self, name: str, idx: int | str, data_type=None):
        """Creates a standard QStandardItem"""
        channel_node = CustomStandardItem(name)
        channel_node.setItemData(id=idx, name=name, node="leaf", data_type=data_type)
        channel_node.setCheckable(True)
        channel_node.setEditable(False)
        if data_type in [int, float, bool, str]:
            channel_node.setEnabled(True)
        elif data_type is not None:
            channel_node.setEnabled(False)
        return channel_node

    def on_channel_checkbox(self, item: CustomStandardItem):
        """Adds the traces to the graph if the item is checked"""
        if not item.isCheckable():
            return

        if item.checkState() != QtCore.Qt.CheckState.Checked:
            return self._remove_trace_by_item_name(item)

        if self.file_type == FileType.TDM:
            self.set_load_icon(item)
            group = item.parent().itemData.id
            channel = item.itemData.id
            worker = TdmGetDataWorker(self.filename, group, channel, item)
            worker.signals.Data_Signal.connect(self._get_tdm_channel_data)
            self.thread_pool.start(worker)
        elif self.file_type == FileType.DAT:
            self._get_dat_channel_data(item)
        elif self.file_type == FileType.PLC_LOG:
            self._get_plc_log_channel_data(item)

    def set_load_icon(self, item: CustomStandardItem):
        item.setEnabled(False)
        item.setIcon(self._load_icon)

    def remove_load_icon(self, item: CustomStandardItem):
        item.setEnabled(True)
        item.setIcon(QtGui.QIcon())

    def _get_plc_log_channel_data(self, item):
        self.remove_load_icon(item)
        df = self.log_file[item.text()]
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df, item=item)

    def _get_tdm_channel_data(self, data):
        item, df = data
        self.remove_load_icon(item)
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df, item=item)

    def _unit_convertion(self, item, df):
        b_unit = item.itemData.b_unit
        c_unit = item.itemData.c_unit
        if b_unit and c_unit:
            a = df.to_numpy() * self.ureg[str(b_unit)]
            a = a.to(self.ureg[str(c_unit)])
            df = pd.Series(a, df.index)
        return df

    def _get_dat_channel_data(self, item):
        """Handles changes for DAT items"""
        self.set_load_icon(item)
        rti_get_data_threads = MultiThreaded_RTI_Reader(self.filenames, item)
        rti_get_data_threads.signals.Data_Signal.connect(self._dat_draw_channel_data)
        self.thread_pool.start(rti_get_data_threads)

    def _dat_draw_channel_data(self, data):
        item, df = data
        self.remove_load_icon(item)
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df, item=item)

    def _add_scatter_trace_to_fig(self, df, item):
        color = item.itemData.costum_color if item.itemData.costum_color else None

        # workaround to always have a trace in the fig
        if len(self.fig.data) == 0:
            self.fig.add_trace(go.Scatter(mode='lines'), hf_x=[], hf_y=[])
            self.fig.data[-1].update(yaxis="y")

        if item.itemData.data_type == bool:
            self._add_bool_trace(df, item, color)
        elif item.itemData.data_type == str:
            self._add_str_trace(df, item, color)
        elif item.itemData.secondary_y:
            self._add_secondary_y_trace(df, item, color)
        else:
            self._add_default_trace(df, item, color)

        item.itemData.trace_uid = self.fig.data[-1].uid
        item.itemData.secondary_y = False

        if self.thread_pool.activeThreadCount() == 0:
            self.qdask.update_graph(self.fig)
            self.browser.reload()
            self.actionShowSignalBrowser.setEnabled(False)

    def _add_bool_trace(self, df, item, color):
        self.fig.add_trace(
            go.Scatter(mode='lines', name=item.itemData.name, yaxis="y3", line=dict(color=color)),
            hf_x=df.index,
            hf_y=df)

        self.fig.data[-1].update(yaxis="y3")
        self.fig.update_layout(
            yaxis3=dict(
                range=[-0.1, 1.1],
                overlaying='y',
                side='left',
                fixedrange=True,
                showgrid=False,
                minor_showgrid=False,
                showticklabels=False,
            )
        )

    def _add_str_trace(self, df, item, color):
        table = item.parent().itemData.id
        name = f"{table}-{item.itemData.name}"
        self.fig.add_trace(
            go.Scatter(mode="markers", name=name, yaxis="y4", hovertext=df.values, line=dict(color=color)),
            hf_x=df.index,
            hf_y=np.repeat(name, len(df)),
        )
        self.fig.data[-1].update(yaxis="y4")
        self.fig.update_layout(
            yaxis4=dict(
                overlaying='y',
                side='right',
                showgrid=False,
                minor_showgrid=False,
                anchor="free",
                autoshift=True

            )
        )

    def _add_secondary_y_trace(self, df, item, color):
        self.fig.add_trace(
            go.Scatter(mode='lines', name=item.itemData.name, yaxis="y2", line=dict(color=color)),
            hf_x=df.index,
            hf_y=df)

        self.fig.data[-1].update(yaxis="y2")
        self.fig.update_layout(
            yaxis2=dict(
                side='right',
                overlaying='y',
                showgrid=False,
                minor_showgrid=False,
            )
        )

    def _add_default_trace(self, df, item, color):
        self.fig.add_trace(
            go.Scatter(mode='lines', name=item.itemData.name, yaxis="y", line=dict(color=color)),
            hf_x=df.index,
            hf_y=df,
        )
        self.fig.data[-1].update(yaxis="y")

    def _remove_trace_by_item_name(self, item):
        uid = item.itemData.trace_uid

        for ix, trace in enumerate(self.fig.data):
            if trace.uid == uid:
                self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1:]
                self.qdask.update_graph(self.fig)
                self.browser.reload()
                self.actionShowSignalBrowser.setEnabled(False)


def main():
    """Main function"""
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Run the app with a selectable dash app port")
    parser.add_argument('--port', type=int, help='Port number for the Dash App.', default=8050)

    args = parser.parse_args()
    port = args.port

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(port=port, app=app)
    window.show()
    app.aboutToQuit.connect(window.qdask.stop)
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
