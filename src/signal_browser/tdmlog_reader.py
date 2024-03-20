import pandas as pd
import tdm_loader
import re
import numpy as np
from PySide6 import QtGui
from PySide6.QtCore import QObject, Signal, QRunnable

from .utils import TimeConversionUtils


class TDMLogReader:
    @staticmethod
    def get_groups(file):

        groups = []
        group_names = {}
        tdm_file = tdm_loader.OpenFile(file)
        for ix, group in enumerate(range(0, len(tdm_file))):
            name = tdm_file.channel_group_name(group)

            if name not in group_names:
                group_names[name] = 0
                groups.append(f"{name}")
            else:
                group_names[name] += 1
                groups.append(f"{name} [{group_names[name]}]")
        return groups

    @staticmethod
    def get_channels(file, group):
        tdm_file = tdm_loader.OpenFile(file)

        match = re.search(r'(.*?)\s*\[(\d+)\]', group)
        if match:
            group_name = match.group(1)
            occurrence = int(match.group(2))
        else:
            group_name = group
            occurrence = 0

        return [(ix, f'{channel.findtext("name")} ({channel.findtext("description")})') for ix, channel in
                enumerate(tdm_file._channels_xml(group_name, occurrence=occurrence))]

    @staticmethod
    def get_channel_description(file, group, channel):
        tdm_file = tdm_loader.OpenFile(file)

        match = re.search(r'(.*?)\s*\[(\d+)\]', group)
        if match:
            group_name = match.group(1)
            occurrence = int(match.group(2))
        else:
            group_name = group
            occurrence = 0


        return tdm_file.channel_description(group_name, channel, occurrence=occurrence)

    @staticmethod
    def get_data(file, group, channel):
        match = re.search(r'(.*?)\s*\[(\d+)\]', group)
        if match:
            group_name = match.group(1)
            occurrence = int(match.group(2))
        else:
            group_name = group
            occurrence = 0

        tdm_file = tdm_loader.OpenFile(file)
        timestamp = list(map(TimeConversionUtils.epoch_timestamp_to_datetime, tdm_file.channel(group_name, channel=0, occurrence=occurrence)))
        data = tdm_file.channel(group_name, channel, occurrence=occurrence)
        df = pd.Series(data, timestamp, name=tdm_file.channel_name(group_name, channel, occurrence=occurrence))
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

