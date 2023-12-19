import pandas as pd
import tdm_loader
import numpy as np

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
        return df
