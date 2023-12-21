import sqlite3
from enum import Enum, auto
from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import pandas as pd
import pathlib
import plotly.graph_objects as go
import re


from .novos_processes import NOVOSProcesses
from .mmc_processes import MMCProcesses
from .plclog_reader import PlcLogReader
from .tdmlog_reader import TDMLogReader
from .rtilog_reader import RTILogReader
from .qt_dash import DashThread


class FileType(Enum):
    TDM = auto()
    DAT = auto()
    DB = auto()
    PLC_LOG = auto()
    NONE = auto()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None, port=8050):
        super().__init__(parent)
        self.resize(800, 600)
        self._host = "127.0.0.1"
        self._port = port

        self.DASH_URL = f"http://{self._host}:{port}"
        self.init_ui_elements_and_vars()
        self.create_layout()
        self.create_menubar()
        self.connect_signals()
        self.fig = self.qdask.fig

    def init_ui_elements_and_vars(self):
        """Initializes the main window and UI elements"""
        self.file_type = FileType.NONE
        self.setWindowTitle("Signal Viewer")
        self._standard_model = QtGui.QStandardItemModel(self)
        self._tree_view = QtWidgets.QTreeView(self)
        self._tree_view.setModel(self._standard_model)
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
        self._standard_model.itemChanged.connect(self.on_channel_checkbox)
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
        self.filename = self.filenames[0]
        self.qdask.update_graph(self.fig)
        self.browser.reload()

        if pathlib.Path(self.filename).suffix.lower() in [".dat", ".db"]:
            self.load_dat_file(self.filename)
            self.file_type = FileType.DAT
        elif pathlib.Path(self.filename).suffix.lower() == ".tdm":
            self.load_tdm_file(self.filename)
            self.file_type = FileType.TDM
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
        action1.triggered.connect(lambda: self.open_context_menu_secondary_y(item))

        # Show the context menu
        menu.exec_(self._tree_view.viewport().mapToGlobal(position))

    def open_context_menu_secondary_y(self, item: QtGui.QStandardItem):
        data = item.data(999)
        data["secondary_y"] = True

        item.model().blockSignals(True)
        item.setData(data, 999)
        item.model().blockSignals(False)
        item.setCheckState(QtCore.Qt.CheckState.Checked)

    def on_double_clicked(self, index: QtCore.QModelIndex):
        """Finds the channel names and adds them to the tree view"""
        if self.file_type == FileType.TDM:
            self.handle_tdm_file(index)
        elif self.file_type == FileType.DAT:
            self.handle_dat_file(index)

    def load_PlcLog_file(self, filename):
        self.log_file = PlcLogReader.read_logfile(filename)
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

    def load_tdm_file(self, filename):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.qdask.update_graph(self.fig)

        root_node = self._standard_model.invisibleRootItem()

        for group in TDMLogReader.get_groups(filename):
            group_node = QtGui.QStandardItem(f"{group}")
            group_node.setEditable(False)
            group_node.setData(dict(id=group, node="root", secondary_y=False), 999)
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(False)

    def load_dat_file(self, filename):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.qdask.update_graph(self.fig)

        with sqlite3.connect(filename) as conn:
            cur = conn.cursor()
            root_node = self._standard_model.invisibleRootItem()
            for table in RTILogReader.get_tables_contains(cur, "rti_json_sample"):
                if RTILogReader._validate_rti_json_sample(cur, table):
                    group_node = QtGui.QStandardItem(f"{table}")
                    group_node.setEditable(False)
                    group_node.setData(dict(id=table, node="root", secondary_y=False), 999)
                    root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)
        self.actionShowNovosProcess.setEnabled(True)

    def handle_tdm_file(self, index: QtCore.QModelIndex):
        """Handles TDM file type"""
        if index.data(999)["node"] != "root":
            return
        group = index.data(999)["id"]
        group_node = self._tree_view.model().itemFromIndex(index)
        group_node.setEditable(False)
        channels = TDMLogReader.get_channels(self.filename, group)
        for ix, name in channels:
            channel_node = self.create_channel_item(name, ix)

            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

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
        channel_node.setData(dict(id=idx, node="leaf", secondary_y=False, data_type=data_type), 999)
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
            self._get_tdm_channel_data(item)
        elif self.file_type == FileType.DAT:
            self._get_dat_channel_data(item)
        elif self.file_type == FileType.PLC_LOG:
            self._get_plc_log_channel_data(item)

    def _get_plc_log_channel_data(self, item):
        y = self.log_file[item.text()].index
        data = self.log_file[item.text()]
        self._add_scatter_trace_to_fig(y, data, item.text())

    def _get_tdm_channel_data(self, item):
        """Handles changes for TDM items"""
        group = item.parent().data(999)["id"]
        channel = item.data(999)["id"]
        df = TDMLogReader.get_data(self.filename, group, channel)
        self._add_scatter_trace_to_fig(df.index, df, item.text())

    def _get_dat_channel_data(self, item):
        """Handles changes for DAT items"""
        item_name = item.data(999)["id"]
        table = item.parent().data(999)["id"]
        data_type = item.data(999)["data_type"]

        new_list = []
        for filename in self.filenames:
            if pathlib.Path(filename).suffix.lower() in [".dat", ".db"]:
                with sqlite3.connect(filename) as dbcon:
                    df = RTILogReader.get_channel_trace(dbcon, table, item_name)
                    new_list.append(df)
        df = pd.concat(new_list)

        self._dat_select_index(df)
        is_boolean = self._dat_is_boolean(df, item_name)

        # todo refactor this
        if data_type == str:
            y = [f"{table}-{item_name}" for x in df.iterrows()]
            self._add_scatter_trace_to_fig(
                df.index,
                y,
                f"{table}-{item_name}",
                is_str=True,
                hovertext=[x[1][0] for x in df.iterrows()],
            )
            self.qdask.update_graph(self.fig)
            self.browser.reload()
            self.actionShowSignalBrowser.setEnabled(False)
        else:
            self._add_scatter_trace_to_fig(
                df.index,
                df[f"json_extract(rti_json_sample, '$.{item_name}')"],
                f"{table}-{item_name}",
                is_boolean=is_boolean,
                secondary_y=item.data(999)["secondary_y"],
            )

        item.model().blockSignals(True)
        data = item.data(999)
        data["secondary_y"] = False
        item.setData(data, 999)
        item.model().blockSignals(False)

    def _dat_select_index(self, df):
        if not df[df.columns[0]].isna().all():
            df.set_index("json_extract(rti_json_sample, '$.timestamp')", inplace=True)
        else:
            df.set_index("SampleInfo_reception_timestamp", inplace=True)
        df.sort_index(inplace=True)

    def _dat_is_boolean(self, df, item_name):
        if df[f"json_extract(rti_json_sample, '$.{item_name}')"].isin([0, 1]).all():
            is_boolean = True
        elif df[f"json_extract(rti_json_sample, '$.{item_name}')"].isin([1]).all():
            is_boolean = True
        elif df[f"json_extract(rti_json_sample, '$.{item_name}')"].isin([0]).all():
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
