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

from src.signal_browser.qt_dash import DashThread

class FileType(Enum):
    TDM = auto()
    DAT = auto()
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
        self._standard_model.itemChanged.connect(self.on_item_changed)

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

        root_node = self._standard_model.invisibleRootItem()
        for group in range(0, len(self.tdms_file)):
            group_node = QtGui.QStandardItem(
                f"{self.tdms_file.channel_group_name(group)} - [{self.tdms_file.no_channels(group)}]")
            group_node.setEditable(False)
            group_node.setData(dict(id=group, node="root"), 999)
            root_node.appendRow(group_node)

    def load_dat_file(self, filename):
        self._standard_model.clear()
        self.fig = go.Figure()
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
            rows = cur.fetchall()
            if len(rows) > 0:
                if "rti_json_sample" in [row[1] for row in rows]:
                    cur.execute(f"SELECT rti_json_sample FROM '{table}';")
                    rows = cur.fetchall()
                    if len(rows) > 0:
                        group_node = QtGui.QStandardItem(
                            f"{table}")
                        group_node.setEditable(False)
                        group_node.setData(dict(id=table, node="root"), 999)
                        root_node.appendRow(group_node)


        conn.close()



    def on_actionOpenFile_triggered(self):
        """Opens a file and adds the groups to the tree view"""

        self.file_type = FileType.NONE

        self.filename, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open File", "", "TDM (*.tdm *.dat)")
        match pathlib.Path(self.filename).suffix.lower():
            case ".tdm":
                self.load_tdm_file(self.filename)
                self.file_type = FileType.TDM
            case ".dat":
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
            if index.data(999)["node"] != "root":
                return
            group = index.data(999)["id"]
            group_node = self._tree_view.model().itemFromIndex(index)
            for ix, channel in enumerate(self.tdms_file._channels_xml(group)):
                name = channel.findtext("name")
                channel_node = QtGui.QStandardItem(name)
                channel_node.setData(dict(id=ix, node="leaf"), 999)
                channel_node.setCheckable(True)
                group_node.appendRow(channel_node)
        elif self.file_type == FileType.DAT:
            if index.data(999)["node"] != "root":
                return
            table = index.data(999)["id"]
            group_node = self._tree_view.model().itemFromIndex(index)
            # Connect to the SQLite database
            conn = sqlite3.connect(self.filename)
            # Create a cursor object
            cur = conn.cursor()
            # Execute the SQL query
            cur.execute(f"SELECT rti_json_sample FROM '{table}';")
            # Fetch all rows from the executed SQL query
            rows = cur.fetchall()
            # Close the connection
            conn.close()

            channels = {}
            for row in rows:
                if row[0] is not None:
                    json_data = json.loads(row[0])
                    for key in json_data.keys():
                        if key not in channels:
                            channels[key] = type(json_data[key])




            for key, value in channels.items():

                name = key
                channel_node = QtGui.QStandardItem(name)
                channel_node.setData(dict(id=name, node="leaf"), 999)
                channel_node.setCheckable(True)
                if value == int or value == float:
                    channel_node.setEnabled(True)
                else:
                    channel_node.setEnabled(False)
                group_node.appendRow(channel_node)

    def get_timestamp_from_json(self, data: dict) -> datetime:
        """convert timestamp from json to datetime.datetime:
        element data timestamp: {sec:, nanosec:}"""
        return datetime.fromtimestamp(data["sec"]) + timedelta(
            microseconds=data["nanosec"] / 1000)


    def on_item_changed(self, item):
        """Adds the traces to the graph if the item is checked"""

        if self.file_type == FileType.TDM:
            if item.isCheckable():
                if item.checkState() == QtCore.Qt.CheckState.Checked:
                    y = pd.Series(self.tdms_file.channel(item.parent().data(999)["id"], 0))
                    y = y.apply(self.zeroEpoctimestamp_to_datetime)
                    data = self.tdms_file.channel(item.parent().data(999)["id"], item.data(999)["id"])
                    self.fig.add_trace(go.Scatter(x=y, y=data, mode='lines', name=item.text()))

                    self.qdask.update_graph(self.fig)
                    self.browser.reload()

                else:
                    for ix, trace in enumerate(self.fig.data):
                        if trace.name == item.text():
                            self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1:]
                            self.qdask.update_graph(self.fig)
                            self.browser.reload()
                            break
        elif self.file_type == FileType.DAT:
            if item.isCheckable():
                if item.checkState() == QtCore.Qt.CheckState.Checked:
                    table = item.parent().data(999)["id"]

                    # Connect to the SQLite database
                    conn = sqlite3.connect(self.filename)
                    # Create a cursor object
                    cur = conn.cursor()
                    # Execute the SQL query
                    cur.execute(f"SELECT rti_json_sample FROM '{table}';")
                    # Fetch all rows from the executed SQL query
                    rows = cur.fetchall()
                    # Close the connection
                    conn.close()

                    rti_sample = {}
                    for row in rows:
                        json_data = json.loads(row[0])
                        if "timestamp" in json_data:
                            rti_sample[self.get_timestamp_from_json(json_data["timestamp"])] = json_data[item.data(999)["id"]]

                    df = pd.DataFrame.from_dict(rti_sample, orient="index", columns=[item.text()])
                    self.fig.add_trace(go.Scatter(x=df.index, y=df.iloc[:, 0], mode='lines', name=item.text()))
                    self.qdask.update_graph(self.fig)
                    self.browser.reload()
                else:
                    for ix, trace in enumerate(self.fig.data):
                        if trace.name == item.text():
                            self.fig.data = self.fig.data[:ix] + self.fig.data[ix + 1:]
                            self.qdask.update_graph(self.fig)
                            self.browser.reload()
                            break


def main():
    """Main function"""
    import sys

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
