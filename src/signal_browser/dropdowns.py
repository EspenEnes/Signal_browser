import math
from datetime import datetime, timedelta
import dash
from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import plotly.graph_objects as go
import pandas as pd
import tdm_loader

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Signal Viewer")
        self.central_widget = QtWidgets.QWidget()
        self._standard_model = QtGui.QStandardItemModel(self)
        self._tree_view = QtWidgets.QTreeView(self)
        self.qdask = WorkerThread()
        self.qdask.start()
        self.browser = QtWebEngineWidgets.QWebEngineView(self)
        self.fig = go.Figure()

        self.create_menubar()

        self.qdask.update_graph(self.fig)
        self.browser.load(QtCore.QUrl("http://127.0.0.1:8050"))


        self.Hlayout = QtWidgets.QHBoxLayout()
        self.Hlayout.addWidget(self._tree_view)
        self.Hlayout.addWidget(self.browser, 10)

        self.central_widget.setLayout(self.Hlayout)

        self.setCentralWidget(self.central_widget)

        self._tree_view.setModel(self._standard_model)
        self._tree_view.doubleClicked.connect(self.on_double_clicked)
        self._standard_model.itemChanged.connect(self.on_item_changed)

    def create_menubar(self):

        self.menubar = QtWidgets.QMenuBar(self)
        self.menuFile = QtWidgets.QMenu(self.menubar, title="File")

        self.actionOpenFile = QtGui.QAction(self, text="Open")

        self.menuFile.addAction(self.actionOpenFile)

        self.menubar.addAction(self.menuFile.menuAction())
        self.setMenuBar(self.menubar)
        self.actionOpenFile.triggered.connect(self.on_actionOpenFile_triggered)

    def on_actionOpenFile_triggered(self):
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open File", "", "TDM Files (*.tdm)")
        if filename:
            self.tdms_file = tdm_loader.OpenFile(filename)
            self._standard_model.clear()
            self.fig = go.Figure()

            root_node = self._standard_model.invisibleRootItem()
            for group in range(0, len(self.tdms_file)):
                group_node = QtGui.QStandardItem(
                    f"{self.tdms_file.channel_group_name(group)} - [{self.tdms_file.no_channels(group)}]")
                group_node.setEditable(False)
                group_node.setData(group, 999)
                root_node.appendRow(group_node)

    def zeroEpoctimestamp_to_datetime(self, dateval: float):
        basedate = datetime(year=1, month=1, day=1, hour=0, minute=0)
        parts = math.modf(dateval)
        days = timedelta(seconds=parts[1])
        day_frac = timedelta(seconds=parts[0])
        return (basedate + days + day_frac) - timedelta(days=365)

    def on_double_clicked(self, index):
        group = index.data(999)
        group_node = self._tree_view.model().itemFromIndex(index)
        for ix, channel in enumerate(self.tdms_file._channels_xml(group)):
            name = channel.findtext("name")
            channel_node = QtGui.QStandardItem(name)
            channel_node.setData(ix, 999)
            channel_node.setCheckable(True)
            group_node.appendRow(channel_node)

    def on_item_changed(self, item):
        if item.isCheckable():
            if item.checkState() == QtCore.Qt.CheckState.Checked:
                y = pd.Series(self.tdms_file.channel(item.parent().data(999), 0))
                y = y.apply(self.zeroEpoctimestamp_to_datetime)
                data = self.tdms_file.channel(item.parent().data(999), item.data(999))
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


class WorkerThread(QtCore.QThread):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._app = dash.Dash()
        self._app.layout = dash.html.Div()

    def update_graph(self, fig):
        self._app.layout = dash.html.Div(children=[
            dash.dcc.Graph(
                id='fig',
                figure={
                    'data': fig.data,
                    'layout': fig.layout
                },  style={'height': '100vh'}),
        ], style={'height': '100vh'})

    def run(self):
        self._app.run_server()


def main():
    import sys

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()

    window.show()
    sys.exit(app.exec())



if __name__ == '__main__':
    main()
