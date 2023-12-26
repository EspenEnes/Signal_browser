import pandas as pd
import tdm_loader
import numpy as np
from PySide6 import QtGui
from PySide6.QtCore import QObject, Signal, QRunnable

from .utils import TimeConversionUtils


class TDMLogReader:
    @staticmethod
    def get_groups(file):
        groups = []
        tdm_file = tdm_loader.OpenFile(file)
        for ix, group in enumerate(range(0, len(tdm_file))):
            groups.append(f"{tdm_file.channel_group_name(group)}")
        return groups

    @staticmethod
    def get_channels(file, group):
        tdm_file = tdm_loader.OpenFile(file)
        return [(ix, channel.findtext("name")) for ix, channel in enumerate(tdm_file._channels_xml(group))]

    @staticmethod
    def get_data(file, group, channel):
        tdm_file = tdm_loader.OpenFile(file)
        timestamp = list(map(TimeConversionUtils.epoch_timestamp_to_datetime, tdm_file.channel(group, 0)))
        data = tdm_file.channel(group, channel)
        df = pd.Series(data, timestamp, name=tdm_file.channel_name(group, channel))
        df.sort_index(inplace=True)
        return df


class TDM_WorkerSignals(QObject):
    Groups_Signal = Signal(list)
    Channels_Signal = Signal(list)
    Data_Signal = Signal(pd.Series)


class TdmGetGroupsWorker(QRunnable):
    def __init__(self, filename: str):
        super().__init__()
        self.filename = filename
        self.signals = TDM_WorkerSignals()

    def run(self):
        groups = TDMLogReader.get_groups(self.filename)
        self.signals.Groups_Signal.emit(groups)


class TdmGetChannelsWorker(QRunnable):
    def __init__(self, filename: str, index, group):
        super().__init__()
        self.signals = TDM_WorkerSignals()

        self.filename = filename
        self.index = index
        self.group = group

    def run(self):
        channels = TDMLogReader.get_channels(self.filename, self.group)
        self.signals.Channels_Signal.emit((self.index, channels))


class TdmGetDataWorker(QRunnable):
    def __init__(self, filename, group, channel, item):
        super().__init__()
        self.signals = TDM_WorkerSignals()

        self.filename = filename
        self.group = group
        self.channel = channel
        self.item = item

    def run(self):
        data = TDMLogReader.get_data(self.filename, self.group, self.channel)
        self.signals.Data_Signal.emit((self.item, data))

