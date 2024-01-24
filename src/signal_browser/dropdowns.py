import sqlite3
from enum import Enum, auto
from PySide6 import QtCore, QtWidgets, QtWebEngineWidgets, QtGui
import pandas as pd
import pathlib
import plotly.graph_objects as go
import pint

from .my_custom_classes import CustomStandardItemModel, CustomStandardItem
from .novos_processes import NOVOSProcesses
from .mmc_processes import MMCProcesses
from .plclog_reader import PlcLogReader_Async
from .tdmlog_reader import TdmGetGroupsWorker, TdmGetChannelsWorker, TdmGetDataWorker
from .rtilog_reader import RTILogReader, MultiThreaded_RTI_Reader
from .qt_dash import DashThread


class FileType(Enum):
    """
    FileType is an enumeration class that represents different types of file formats.
    """

    TDM = auto()
    DAT = auto()
    DB = auto()
    PLC_LOG = auto()
    NONE = auto()


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

        # if item.itemData.costum_color:
        #     option.backgroundBrush = QtGui.QColor(item.itemData.costum_color)


class MainWindow(QtWidgets.QMainWindow):
    """
    The MainWindow class represents the main window of the application. It inherits from the QtWidgets.QMainWindow class.
    """

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
        if _color :=  item.itemData.costum_color:
            color = QtWidgets.QColorDialog.getColor(QtGui.QColor(_color))
        else:
            color = QtWidgets.QColorDialog.getColor()


        if color.isValid():
            item.itemData.costum_color = color.name()
            print(color.name())

    def unit_convertion(self, item: CustomStandardItem):
        print(item.itemData.data_type)
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

        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.fig.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
        self.qdask.update_graph(self.fig)

        root_node = self._standard_model.invisibleRootItem()
        for channel in self.log_file.columns:
            channel_node = self.create_channel_item(channel, channel)
            root_node.appendRow(channel_node)
        self._standard_model.setHorizontalHeaderLabels(["Signals"])

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(True)
        self._tree_view.setSortingEnabled(True)

    def load_tdm_groups(self, groups):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.fig.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
        self.qdask.update_graph(self.fig)

        root_node = self._standard_model.invisibleRootItem()
        for group in groups:
            group_node = CustomStandardItem(f"{group}")
            group_node.setEditable(False)
            group_node.setItemData(id=group, node="root", secondary_y=False)
            root_node.appendRow(group_node)
        self._standard_model.sort(0, QtCore.Qt.AscendingOrder)

        self.actionShowNovosProcess.setEnabled(False)
        self.actionShowMMCProcess.setEnabled(False)
        self._tree_view.setSortingEnabled(False)

        self.file_type = FileType.TDM

    def load_dat_groups(self, filenames):
        self._standard_model.clear()
        self.fig.replace(go.Figure())
        self.fig.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
        self.qdask.update_graph(self.fig)
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
            group_node.setItemData(id=table, node="root", secondary_y=False)
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
        channel_node.setItemData(id=idx, name=name, node="leaf", secondary_y=False, data_type=data_type)
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
        self._add_scatter_trace_to_fig(df.index, df, item.text(), item=item, secondary_y=item.itemData.secondary_y)
        item.itemData.secondary_y = False

    def _get_tdm_channel_data(self, data):
        item, df = data
        self.remove_load_icon(item)
        df = self._unit_convertion(item, df)
        self._add_scatter_trace_to_fig(df.index, df, item.text(), item=item, secondary_y=item.itemData.secondary_y)

    def _unit_convertion(self, item, df):
        b_unit = item.itemData.b_unit
        c_unit = item.itemData.c_unit
        print(b_unit, c_unit)
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
        item_name = item.itemData.id
        data_type = item.itemData.data_type
        table = item.parent().itemData.id
        df = self._unit_convertion(item, df)
        is_boolean = self._dat_is_boolean(df, item_name)

        # todo refactor this
        if data_type == str:
            y = [f"{table}-{item_name}" for x in df.values]
            self._add_scatter_trace_to_fig(
                df.index, y, f"{table}-{item_name}", is_str=True, hovertext=df.values, item=item
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
                secondary_y=item.itemData.secondary_y,
                item=item,
            )

        item.itemData.secondary_y = False

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

    def _add_scatter_trace_to_fig(
            self, x, y, name, is_boolean=False, secondary_y=False, is_str=False, hovertext=None, item=None
    ):

        if item.itemData.costum_color:
            color = item.itemData.costum_color
        else:
            color = None
        """Adds scatter trace to the fig"""
        if len(self.fig.data) == 0 and not is_boolean and not secondary_y and not is_str:
            self.fig.add_trace(go.Scatter(mode='lines', name=name, line=dict(color=color)), hf_x=x, hf_y=y)

        elif secondary_y and not is_boolean and not is_str:
            self.fig.add_trace(go.Scatter(mode='lines', name=name, yaxis="y3", line=dict(color=color)), hf_x=x, hf_y=y)
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
            self.fig.add_trace(go.Scatter(mode='lines', name=name, yaxis="y2", line=dict(color=color)), hf_x=x, hf_y=y)
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
                    line=dict(color=color)
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
                go.Scatter(mode='lines', name=name, line=dict(color=color)),
                hf_x=x,
                hf_y=y,
            )

        item.itemData.trace_uid = self.fig.data[-1].uid

        if self.thread_pool.activeThreadCount() == 0:
            self.qdask.update_graph(self.fig)
            self.browser.reload()
            self.actionShowSignalBrowser.setEnabled(False)

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
    print(port)

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(port=port)
    window.show()
    app.aboutToQuit.connect(window.qdask.stop)
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
