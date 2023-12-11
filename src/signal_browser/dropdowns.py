import json
import math
import sqlite3
from datetime import datetime, timedelta
from enum import Enum, auto

from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import plotly.graph_objects as go
import pandas as pd
import tdm_loader
import pathlib
from PySide6 import QtCore
import plotly.graph_objects as go
from plotly import subplots

from .qt_dash import DashThread


# from signal_browser.qt_dash import DashThread

class FileType(Enum):
    TDM = auto()
    DAT = auto()
    DB = auto()
    NONE = auto()



class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        DASH_URL = "http://127.0.0.1:8050"
        self.file_type = FileType.NONE
        self.setWindowTitle("Signal Viewer")

        self.central_widget = QtWidgets.QWidget()
        self._standard_model = QtGui.QStandardItemModel(self)
        self._tree_view = QtWidgets.QTreeView(self)
        self._tree_view.setModel(self._standard_model)

        self.qdask = DashThread()
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        self.browser.load(QtCore.QUrl(DASH_URL))
        self.qdask.start()

        self.connect_signals()
        self.create_layout()
        self.create_menubar()

    def connect_signals(self):
        """Connects the signals to the slots"""
        self._tree_view.doubleClicked.connect(self.on_double_clicked)
        self._standard_model.itemChanged.connect(self.on_channel_check)

    def create_layout(self):
        """Creates the layout for the main window"""
        self.Hlayout = QtWidgets.QHBoxLayout()
        self.Hlayout.addWidget(self._tree_view)
        self.Hlayout.addWidget(self.browser, 10)
        self.central_widget.setLayout(self.Hlayout)
        self.setCentralWidget(self.central_widget)

    def create_menubar(self):
        """Creates the menu bar and adds the open file action"""

        self.menubar = QtWidgets.QMenuBar(self)
        self.menuFile = QtWidgets.QMenu(self.menubar, title="File")

        self.actionOpenFile = QtGui.QAction(self, text="Open")

        self.menuFile.addAction(self.actionOpenFile)

        self.menubar.addAction(self.menuFile.menuAction())
        self.setMenuBar(self.menubar)
        self.actionOpenFile.triggered.connect(self.on_actionOpenFile_triggered)

    def load_tdm_file(self, filename):
        self.tdms_file = tdm_loader.OpenFile(filename)
        self._standard_model.clear()
        self.fig = go.Figure()
        self.fig.update_xaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray')
        self.fig.update_yaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray')

        root_node = self._standard_model.invisibleRootItem()
        for group in range(0, len(self.tdms_file)):
            group_node = QtGui.QStandardItem(
                f"{self.tdms_file.channel_group_name(group)} - [{self.tdms_file.no_channels(group)}]")
            group_node.setEditable(False)
            group_node.setData(dict(id=group, node="root"), 999)
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

    def load_dat_file(self, filename):
        self._standard_model.clear()
        self.fig = subplots.make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
        self.fig.update_xaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray')
        self.fig.update_yaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray')
        self.fig.update_layout(
            yaxis2=dict(
                range=[-.1, 1.1],
                overlaying='y',
                side='right',
                fixedrange=True,
                showgrid=False,
                minor_showgrid=False,
                tickvals=[0,1],
                ticktext=["False", "True"]
            ),
        )

        # Connect to the SQLite database
        conn = sqlite3.connect(filename)
        # Create a cursor object
        cur = conn.cursor()
        # Execute the SQL query
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # Fetch all rows from the executed SQL query
        rows = cur.fetchall()
        # Close the connection

        # Extract table names from tuples and return as a list
        root_node = self._standard_model.invisibleRootItem()
        for table in [row[0] for row in rows]:
            cur.execute(f"PRAGMA table_info('{table}')")
            columns = cur.fetchall()
            if columns and len(columns) > 0:
                for column in columns:
                    if "rti_json_sample" in column[1]:
                        cur.execute(f"SELECT json_extract(rti_json_sample, '$') FROM '{table}';")
                        channels = cur.fetchone()
                        if channels and len(channels) > 0:
                            group_node = QtGui.QStandardItem(
                            f"{table}")
                            group_node.setEditable(False)
                            group_node.setData(dict(id=table, node="root"), 999)
                            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)


        conn.close()



    def on_actionOpenFile_triggered(self):
        """Opens a file and adds the groups to the tree view"""

        self.file_type = FileType.NONE

        self.filename, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open File", "", "TDM (*.tdm *.dat *.db)")
        match pathlib.Path(self.filename).suffix.lower():
            case ".tdm" :
                self.load_tdm_file(self.filename)
                self.file_type = FileType.TDM
            case ".dat" | ".db":
                self.load_dat_file(self.filename)
                self.file_type = FileType.DAT


    def zeroEpoctimestamp_to_datetime(self, dateval: float) -> datetime:
        """Convert a zero epoch timestamp to a datetime object."""
        basedate = datetime(year=1, month=1, day=1, hour=0, minute=0)
        parts = math.modf(dateval)
        days = timedelta(seconds=parts[1])
        day_frac = timedelta(seconds=parts[0])
        return (basedate + days + day_frac) - timedelta(days=365)

    def on_double_clicked(self, index: QtCore.QModelIndex):
        """Finds the channel names and adds them to the tree view"""
        if self.file_type == FileType.TDM:
            self.handle_tdm_file(index)
        elif self.file_type == FileType.DAT:
            self.handle_dat_file(index)


    def handle_tdm_file(self, index: QtCore.QModelIndex):
        """Handles TDM file type"""
        if index.data(999)["node"] != "root":
            return
        group = index.data(999)["id"]
        group_node = self._tree_view.model().itemFromIndex(index)
        channels = self.get_channels_from_tdm(group)
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
        channels = self.get_channels_from_rti_json_sample(table)
        for key, value in channels.items():
            name = key
            channel_node = self.create_channel_item(name, name, data_type=value)
            group_node.appendRow(channel_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)


    def create_channel_item(self, name: str, idx: int | str, data_type=None):
        """Creates a standard QStandardItem"""
        channel_node = QtGui.QStandardItem(name)
        channel_node.setData(dict(id=idx, node="leaf"), 999)
        channel_node.setCheckable(True)
        if data_type in [int, float, bool]:
            channel_node.setEnabled(True)
        elif data_type is not None:
            channel_node.setEnabled(False)
        return channel_node


    def get_channels_from_rti_json_sample(self, table: str):
        """Connects to the SQLite database and get rti_json_sample,
        only add channels that contains data in the rti_json_sample column"""
        # Connect to the SQLite database
        conn = sqlite3.connect(self.filename)
        # Create a cursor object
        cur = conn.cursor()
        # Execute the SQL query
        cur.execute(f"SELECT rti_json_sample FROM '{table}';")

        # Fetch all rows from the executed SQL query
        row = cur.fetchone()
        # Close the connection
        conn.close()
        channels = {}

        if row[0] is not None:
            json_data = json.loads(row[0])
            for key in json_data.keys():
                if key not in channels:
                    if str(json_data[key]).lower() in ["false", "true"]:
                        channels[key] = bool
                    else:
                        channels[key] = type(json_data[key])





        return channels

    def get_channels_from_tdm(self, group):
        """Fetches channels from the TDM file"""
        return [(ix, channel.findtext("name")) for ix, channel in enumerate(self.tdms_file._channels_xml(group))]



    def get_timestamp_from_json(self, data: dict) -> datetime:
        """convert timestamp from json to datetime.datetime:
        element data timestamp: {sec:, nanosec:}"""
        return datetime.fromtimestamp(data["sec"]) + timedelta(
            microseconds=data["nanosec"] / 1000)

    def get_timestamp_from_ns(self, ns_value: int) -> datetime:
        """convert ns unixtime to datetime.datetime"""
        return datetime.fromtimestamp(ns_value / 1e9)

    def on_channel_check(self, item):
        """Adds the traces to the graph if the item is checked"""
        if not item.isCheckable():
            return

        if item.checkState() != QtCore.Qt.CheckState.Checked:
            return self._remove_trace_by_item_name(item.text())

        if self.file_type == FileType.TDM:
            self._get_tdm_channel_data(item)
        elif self.file_type == FileType.DAT:
            self._get_dat_channel_data(item)


    def _remove_trace_by_item_name(self, item_name):
        """Removes a trace by given item name"""
        for ix, trace in enumerate(self.fig.data):
            if trace.name == item_name:
                self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1:]
                self.qdask.update_graph(self.fig)
                self.browser.reload()
                break


    def _get_tdm_channel_data(self, item):
        """Handles changes for TDM items"""
        y = pd.Series(self.tdms_file.channel(item.parent().data(999)["id"], 0))
        y = y.apply(self.zeroEpoctimestamp_to_datetime)
        data = self.tdms_file.channel(item.parent().data(999)["id"], item.data(999)["id"])
        self._add_scatter_trace_to_fig(y, data, item.text())

    def _get_dat_channel_data(self, item):
        """Handles changes for DAT items"""
        item_name = item.data(999)["id"]

        table = item.parent().data(999)["id"]
        conn = sqlite3.connect(self.filename)
        cur = conn.cursor()
        cur.execute(f"SELECT json_extract(rti_json_sample, '$.timestamp'),"
                    f"       json_extract(rti_json_sample, '$.{item_name}'),"
                    f"       SampleInfo_reception_timestamp "
                    f"FROM '{table}';")
        rows = cur.fetchall()
        conn.close()

        rti_sample = {}
        is_bolean = True

        for timestamp_json, data, timestamp_sql in rows:
            if data not in [1, 0]:
                is_bolean = False


            if timestamp_json is not None:
                timestamp = self.get_timestamp_from_json(json.loads(timestamp_json))
            else:
                timestamp = self.get_timestamp_from_ns(timestamp_sql)

            if type(data) == bool:
                if data:
                    boolean = 1
                else:
                    boolean = 0
                rti_sample[timestamp] = boolean
            else:
                rti_sample[timestamp] = data
        df = pd.DataFrame.from_dict(rti_sample, orient="index", columns=[item.text()])
        self._add_scatter_trace_to_fig(df.index, df.iloc[:, 0], item.text(), secondary_y=is_bolean)


    def _add_scatter_trace_to_fig(self, x, y, text, secondary_y=False):
        """Adds scatter trace to the fig"""
        self.fig.add_trace(go.Scatter(x=x, y=y, mode='lines', name=text),row=1,col=1, secondary_y=secondary_y)
        self.qdask.update_graph(self.fig)
        self.browser.reload()


def main():
    """Main function"""
    import sys

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
