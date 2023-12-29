import sqlite3
from enum import Enum, auto
from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import pandas as pd
import pathlib
import plotly.graph_objects as go
import re
import pint


from .novos_processes import NOVOSProcesses
from .mmc_processes import MMCProcesses
from .plclog_reader import PlcLogReader_Async
from .tdmlog_reader import TdmGetGroupsWorker, TdmGetChannelsWorker, TdmGetDataWorker
from .rtilog_reader import RTILogReader, MultiThreaded_RTI_Reader
from .qt_dash import DashThread


class FileType(Enum):
    TDM = auto()
    DAT = auto()
    DB = auto()
    PLC_LOG = auto()
    NONE = auto()


class ColorizeDelegate(QtWidgets.QStyledItemDelegate):
    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)

        data = index.data(999)
        if not isinstance(data, dict):
            return

        b_unit = data.get('b_unit')
        c_unit = data.get('c_unit')
        name = data.get('name')

        if b_unit and c_unit and name:
            option.backgroundBrush = QtGui.QColor('Yellow')
            option.text = f'{data["name"]} [{data["b_unit"]}->{data["c_unit"]}]'


class CustomStandardItemModel(QtGui.QStandardItemModel):
    """Reimplementation of QStandardItemmodel so it can filer out CheckStateRoles"""

    checkStateChanged = QtCore.Signal(QtGui.QStandardItem)

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


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None, port=8050):
        super().__init__(parent)
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

    def connect_signals(self):
        """Connects the signals to the slots"""
        self._tree_view.doubleClicked.connect(self.on_double_clicked)
        self._standard_model.checkStateChanged.connect(self.on_channel_checkbox)
        self._tree_view.customContextMenuRequested.connect(self.open_context_menu)

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
        self.actionShowNovosProcess.setEnabled(True)

    def show_novos_process(self):
        self.fig2 = NOVOSProcesses.make_plotly_figure(self.filenames)

        self.qdask.update_progress(self.fig2)
        self.browser.reload()

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowSignalBrowser.setEnabled(True)

    def show_mmc_process(self):
        self.fig2 = MMCProcesses.make_plotly_figure(self.log_file)

        self.qdask.update_progress(self.fig2)
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
        self.qdask.update_graph(self.fig)
        self.browser.reload()

        if pathlib.Path(self.filename).suffix.lower() in [".dat", ".db"]:
            self.load_dat_file(self.filename)
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

        # Check if the item is a channel
        if index.data(999)["node"] != "leaf":
            # It's a root item, not a child item
            return

        item = self._tree_view.model().itemFromIndex(index)
        menu = QtWidgets.QMenu()

        action1 = menu.addAction("Select and add to secondary axis")
        action2 = menu.addAction("Unit Conversion")
        action1.triggered.connect(lambda: self.open_context_menu_secondary_y(item))
        action2.triggered.connect(lambda: self.unit_convertion(item))

        action2.setEnabled(True)
        if item.checkState() == QtCore.Qt.CheckState.Checked:
            action2.setEnabled(False)

        # Show the context menu
        menu.exec_(self._tree_view.viewport().mapToGlobal(position))

    def unit_convertion(self, item):
        base_unit, conc_unit = self.open_unit_convertion_dialog()

        data = item.data(999)
        data["b_unit"] = base_unit
        data["c_unit"] = conc_unit
        item.setData(data, 999)

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
        return None, None

    def open_context_menu_secondary_y(self, item: QtGui.QStandardItem):
        data = item.data(999)
        data["secondary_y"] = True
        item.setData(data, 999)
        item.setCheckState(QtCore.Qt.CheckState.Checked)
        self.on_channel_checkbox(item)

    def on_double_clicked(self, index: QtCore.QModelIndex):
        """Finds the channel names and adds them to the tree view"""
        if index.data(999)["node"] != "root":
            return
        item = self._tree_view.model().itemFromIndex(index)
        if item.rowCount() > 0:
            return

        if self.file_type == FileType.TDM:
            self.set_load_icon(item)
            group = index.data(999)["id"]
            worker = TdmGetChannelsWorker(self.filename, index, group)
            worker.signals.Channels_Signal.connect(self.load_tdm_channels)
            self.thread_pool.start(worker)

        elif self.file_type == FileType.DAT:
            self.handle_dat_file(index)

    def load_PlcLog_file(self, filename):
        self.log_file = PlcLogReader_Async.read_logfile(filename)

        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.qdask.update_graph(self.fig)

        root_node = self._standard_model.invisibleRootItem()
        for channel in self.log_file.columns:
            channel_node = self.create_channel_item(channel, channel)
            root_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(True)

    def load_tdm_groups(self, groups):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.qdask.update_graph(self.fig)

        root_node = self._standard_model.invisibleRootItem()
        for group in groups:
            group_node = QtGui.QStandardItem(f"{group}")
            group_node.setEditable(False)
            group_node.setData(dict(id=group, node="root", secondary_y=False), 999)
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(False)

        self.file_type = FileType.TDM

    def load_dat_file(self, filename):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.qdask.update_graph(self.fig)

        with sqlite3.connect(filename) as conn:
            cur = conn.cursor()
            root_node = self._standard_model.invisibleRootItem()
            for table in RTILogReader.get_tables_contains(cur, "rti_json_sample"):
                # if RTILogReader._validate_rti_json_sample(cur, table):
                group_node = QtGui.QStandardItem(f"{table}")
                group_node.setEditable(False)
                group_node.setData(dict(id=table, node="root", secondary_y=False), 999)
                root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.actionShowNovosProcess.setEnabled(True)

    def load_tdm_channels(self, data):
        index, channels = data
        group_node = self._tree_view.model().itemFromIndex(index)


        for ix, name in channels:
            channel_node = self.create_channel_item(name, ix)
            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.remove_load_icon(group_node)

    def handle_dat_file(self, index: QtCore.QModelIndex):
        """Handles DAT file type"""
        if index.data(999)["node"] != "root":
            return
        table = index.data(999)["id"]
        group_node = self._tree_view.model().itemFromIndex(index)
        group_node.setEditable(False)

        with sqlite3.connect(self.filename) as conn:
            cur = conn.cursor()
            channels = RTILogReader.get_channels_from_rti_json_sample(cur, table)

        for key, value in channels.items():
            name = key
            channel_node = self.create_channel_item(name, name, data_type=value)
            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

    def create_channel_item(self, name: str, idx: int | str, data_type=None):
        """Creates a standard QStandardItem"""
        channel_node = QtGui.QStandardItem(name)
        channel_node.setData(dict(id=idx, name=name, node="leaf", secondary_y=False, data_type=data_type), 999)
        channel_node.setCheckable(True)
        channel_node.setEditable(False)
        if data_type in [int, float, bool, str]:
            channel_node.setEnabled(True)
        elif data_type is not None:
            channel_node.setEnabled(False)
        return channel_node

    def on_channel_checkbox(self, item):
        """Adds the traces to the graph if the item is checked"""
        if not item.isCheckable():
            return

        if item.checkState() != QtCore.Qt.CheckState.Checked:
            return self._remove_trace_by_item_name(item)

        if self.file_type == FileType.TDM:
            self.set_load_icon(item)
            group = item.parent().data(999)["id"]
            channel = item.data(999)["id"]
            worker = TdmGetDataWorker(self.filename, group, channel, item)
            worker.signals.Data_Signal.connect(self._get_tdm_channel_data)
            self.thread_pool.start(worker)
        elif self.file_type == FileType.DAT:
            self._get_dat_channel_data(item)
        elif self.file_type == FileType.PLC_LOG:
            self._get_plc_log_channel_data(item)

    def set_load_icon(self, item: QtGui.QStandardItem):
        item.setEnabled(False)
        item.setIcon(self._load_icon)

    def remove_load_icon(self, item: QtGui.QStandardItem):
        item.setEnabled(True)
        item.setIcon(QtGui.QIcon())

    def _get_plc_log_channel_data(self, item):
        self.remove_load_icon(item)
        df = self.log_file[item.text()]
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df.index, df, item.text())

    def _get_tdm_channel_data(self, data):
        item, df = data
        self.remove_load_icon(item)
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df.index, df, item.text())

    def _unit_convertion(self, item, df):
        b_unit, c_unit = None, None
        if "b_unit" in item.data(999):
            b_unit = item.data(999)["b_unit"]
        if "c_unit" in item.data(999):
            c_unit = item.data(999)["c_unit"]
        if b_unit and c_unit:
            a = df.to_numpy() * self.ureg[b_unit]
            a = a.to(self.ureg[c_unit])
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
        # df = self._dat_select_index(df)
        item_name = item.data(999)["id"]
        data_type = item.data(999)["data_type"]
        table = item.parent().data(999)["id"]
        df = self._unit_convertion(item, df)
        is_boolean = self._dat_is_boolean(df, item_name)

        # todo refactor this
        if data_type == str:
            y = [f"{table}-{item_name}" for x in df.values]
            self._add_scatter_trace_to_fig(
                df.index,
                y,
                f"{table}-{item_name}",
                is_str=True,
                hovertext=df.values,
            )
            self.qdask.update_graph(self.fig)
            self.browser.reload()
            self.actionShowSignalBrowser.setEnabled(False)
        else:
            self._add_scatter_trace_to_fig(
                df.index,
                df,
                f"{table}-{item_name}",
                is_boolean=is_boolean,
                secondary_y=item.data(999)["secondary_y"],
            )

        data = item.data(999)
        data["secondary_y"] = False
        item.setData(data, 999)

    def _dat_select_index(self, df):
        if not df[df.columns[0]].isna().all():
            df.drop("SampleInfo_reception_timestamp", axis=1, inplace=True)
            df.set_index("json_extract(rti_json_sample, '$.timestamp')", inplace=True)
        else:
            df.drop("json_extract(rti_json_sample, '$.timestamp')", axis=1, inplace=True)
            df.set_index("SampleInfo_reception_timestamp", inplace=True)
        df = df.squeeze()
        df.sort_index(inplace=True)
        return df

    def _dat_is_boolean(self, df, item_name):
        if df.isin([0, 1]).all():
            is_boolean = True
        elif df.isin([1]).all():
            is_boolean = True
        elif df.isin([0]).all():
            is_boolean = True
        elif item_name == "novosControl":
            is_boolean = True
        else:
            is_boolean = False
        return is_boolean

    def _add_scatter_trace_to_fig(self, x, y, name, is_boolean=False, secondary_y=False, is_str=False, hovertext=None):
        """Adds scatter trace to the fig"""
        if len(self.fig.data) == 0 and not is_boolean and not secondary_y and not is_str:
            self.fig.add_trace(go.Scatter(mode='lines', name=name), hf_x=x, hf_y=y)

        elif secondary_y and not is_boolean and not is_str:
            self.fig.add_trace(go.Scatter(mode='lines', name=name, yaxis="y3"), hf_x=x, hf_y=y)
            self.fig.data[-1].update(yaxis="y3")
            self.fig.update_layout(
                yaxis3=dict(
                    side='right',
                    overlaying='y',
                    showgrid=False,
                    minor_showgrid=False,
                )
            )

        elif is_boolean:
            self.fig.add_trace(go.Scatter(mode='lines', name=name, yaxis="y2"), hf_x=x, hf_y=y)
            self.fig.data[-1].update(yaxis="y2")
            self.fig.update_layout(
                yaxis2=dict(
                    range=[-0.1, 1.1],
                    overlaying='y',
                    side='left',
                    fixedrange=True,
                    showgrid=False,
                    minor_showgrid=False,
                    showticklabels=False,
                )
            )
        elif is_str:
            self.fig.add_trace(
                go.Scatter(
                    hovertext=hovertext,
                    mode="markers",
                    name=name,
                ),
                hf_x=x,
                hf_y=y,
            )
            self.fig.data[-1].update(yaxis="y4")
            self.fig.update_layout(
                yaxis4=dict(
                    side='right',
                    overlaying='y',
                    showgrid=False,
                    minor_showgrid=False,
                )
            )

        else:
            self.fig.add_trace(
                go.Scatter(mode='lines', name=name),
                hf_x=x,
                hf_y=y,
            )

        if self.thread_pool.activeThreadCount() == 0:
            self.qdask.update_graph(self.fig)
            self.browser.reload()
            self.actionShowSignalBrowser.setEnabled(False)

    def _remove_trace_by_item_name(self, item):
        """Removes a trace by given item name"""
        for ix, trace in enumerate(self.fig.data):
            if match := re.findall(r'b>(.+)<i', trace.name):
                name = match[0].rstrip()
                name = name.lstrip()
                if name == item.text():
                    self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1 :]
                    self.qdask.update_graph(self.fig)
                    self.browser.reload()
                    self.actionShowSignalBrowser.setEnabled(False)
                    break

            if trace.name == item.text():
                self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1 :]
                self.qdask.update_graph(self.fig)
                self.browser.reload()
                self.actionShowSignalBrowser.setEnabled(False)
                break

        item_name = item.data(999)["id"]
        table = item.parent().data(999)["id"]
        for ix, trace in enumerate(self.fig.data):
            if match := re.findall(r'b>(.+)<i', trace.name):
                name = match[0].rstrip()
                name = name.lstrip()
                if name == f"{table}-{item_name}":
                    self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1 :]
                    self.qdask.update_graph(self.fig)
                    self.browser.reload()
                    self.actionShowSignalBrowser.setEnabled(False)
                    break


def main():
    """Main function"""
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Run the app with a selectable dash app port")
    parser.add_argument('--port', type=int, help='Port number for the Dash App.', default=8050)

    args = parser.parse_args()
    port = args.port
    print(port)

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(port=port)
    window.show()
    app.aboutToQuit.connect(window.qdask.stop)
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
