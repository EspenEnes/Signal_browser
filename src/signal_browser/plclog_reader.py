import io
import zipfile


import numpy as np
import pandas as pd
from .utils import TimeConversionUtils, read_struct_from_binary
import pathlib

import asyncio
class PlcLogReader_Async:
    @classmethod
    async def read_file_async(cls, file_path: str):
        f = await cls.open_file(file_path)
        new = await cls.proccess_file(f)
        f.close()
        return new

    @classmethod
    async def open_file(cls, source):
        if isinstance(source, zipfile.ZipFile):
            return source.open(source.filelist[0].filename, "r")
        elif isinstance(source, str):
            if zipfile.is_zipfile(source):
                with zipfile.ZipFile(source) as zfile:
                    return zfile.open(zfile.filelist[0].filename, "r")
            else:
                return open(source, "rb")

    @classmethod
    async def read_multiple_files_async(cls, file_paths: list):
        tasks = [cls.read_file_async(file_path) for file_path in file_paths]
        return await asyncio.gather(*tasks)

    @classmethod
    def read_logfile(cls, files: list[str]):
        if isinstance(files, list):
            new = asyncio.run(cls.read_multiple_files_async(files))
            df = pd.concat(new)
            df = df[~df.index.duplicated(keep='first')]
            df.sort_index(inplace=True)
            return df
        return None

    @classmethod
    async def proccess_file(cls, file: io.BytesIO, use_timestamp=False, last_timestamp=0):
        """
        Processes the given file and returns the data, previous file name, and next file name.

        Parameters:
            file (io.BytesIO): The file to be processed.
            use_timestamp (bool, optional): Indicates whether to use timestamps or relative time for sample_dt calculation. Default is False.

        Returns:
            tuple: A tuple containing the following elements:
                - data (list of pd.Series): The data extracted from the file.
                - prev_fileName (str): The name of the previous file.
                - next_fileName (str): The name of the next file.
        """
        sample_cnt = int.from_bytes(file.read(4), "little")
        VersionOfData = int.from_bytes(file.read(4), "little")
        sample_start = TimeConversionUtils.oledatetime_to_datetime(np.frombuffer(file.read(8), np.float64))

        if use_timestamp:
            sample_dt = np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32) + last_timestamp
        else:
            sample_dt = (
                pd.TimedeltaIndex(np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32), unit="s") + sample_start
            )

        channel_cnt = int.from_bytes(file.read(4), "little")
        prev_fileName = read_struct_from_binary(file)
        next_fileName = read_struct_from_binary(file)

        data = []
        for i in range(0, channel_cnt):
            name = read_struct_from_binary(file)

            new = pd.Series(np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32), sample_dt, name=name)

            data.append(new)
        df = pd.concat(data, axis=1)
        return df
        # return df, prev_fileName, next_fileName, sample_dt[-1]


class PlcLogReader:
    @classmethod
    def read_logfile(
        cls,
        file: str | zipfile.ZipFile | io.BytesIO | list[str],
        use_timestamp: bool = False,
        read_series: bool = False,
        last_timestamp: int = 0,
    ):
        if isinstance(file, list):
            df = pd.DataFrame()
            for f in file:
                new = cls.read_logfile(
                    f, use_timestamp=use_timestamp, read_series=read_series, last_timestamp=last_timestamp
                )
                df = pd.concat([df, new])
                df = df[~df.index.duplicated(keep='first')]
            df.sort_index(inplace=True)
            return df

        f = cls.open_file(file)
        df, prev_fileName, next_fileName, last_timestamp = cls.proccess_file(
            f, use_timestamp=use_timestamp, last_timestamp=last_timestamp
        )
        f.close()

        if read_series:
            path = pathlib.Path(file)
            next_path = path.parent.joinpath(next_fileName)
            if next_path.is_file():
                if prev_fileName != next_fileName:
                    new = cls.read_logfile(
                        str(next_path), read_series=True, use_timestamp=use_timestamp, last_timestamp=last_timestamp
                    )
                    df = pd.concat([df, new])
                    df = df[~df.index.duplicated(keep='first')]

        return df

    @classmethod
    def proccess_file(cls, file: io.BytesIO, use_timestamp=False, last_timestamp=0):
        """
        Processes the given file and returns the data, previous file name, and next file name.

        Parameters:
            file (io.BytesIO): The file to be processed.
            use_timestamp (bool, optional): Indicates whether to use timestamps or relative time for sample_dt calculation. Default is False.

        Returns:
            tuple: A tuple containing the following elements:
                - data (list of pd.Series): The data extracted from the file.
                - prev_fileName (str): The name of the previous file.
                - next_fileName (str): The name of the next file.
        """
        sample_cnt = int.from_bytes(file.read(4), "little")
        VersionOfData = int.from_bytes(file.read(4), "little")
        sample_start = TimeConversionUtils.oledatetime_to_datetime(np.frombuffer(file.read(8), np.float64))

        if use_timestamp:
            sample_dt = np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32) + last_timestamp
        else:
            sample_dt = (
                pd.TimedeltaIndex(np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32), unit="s") + sample_start
            )

        channel_cnt = int.from_bytes(file.read(4), "little")
        prev_fileName = read_struct_from_binary(file)
        next_fileName = read_struct_from_binary(file)

        data = []
        for i in range(0, channel_cnt):
            name = read_struct_from_binary(file)

            new = pd.Series(np.frombuffer(file.read(sample_cnt * 4), dtype=np.float32), sample_dt, name=name)

            data.append(new)
        df = pd.concat(data, axis=1)
        return df, prev_fileName, next_fileName, sample_dt[-1]

    @classmethod
    def open_file(cls, source):
        if isinstance(source, zipfile.ZipFile):
            return source.open(source.filelist[0].filename, "r")
        elif isinstance(source, str):
            if zipfile.is_zipfile(source):
                with zipfile.ZipFile(source) as zfile:
                    return zfile.open(zfile.filelist[0].filename, "r")
            else:
                return open(source, "rb")
