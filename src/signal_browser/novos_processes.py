import json
import sqlite3
from datetime import timedelta
import plotly.express as px
import pandas as pd
from .utils import TimeConversionUtils


class NOVOSProcesses:
    @classmethod
    def make_plotly_figure(cls, filenames: list[str]):
        """
        Generates a Plotly figure object based on the given list of filenames.

        Parameters:
        filenames (list[str]): A list of filenames containing the data for plotting.

        Returns:
        go.Figure: A Plotly figure object representing the generated timeline chart.

        Example:
        >>> filenames = ["data_file1.db", "data_file2.db"]
        >>> figure = make_plotly_figure(filenames)
        """
        timeline_data = cls.make_timeline_data(filenames)

        if not timeline_data:
            return None

        fig = px.timeline(
            timeline_data, x_start="Start", x_end="Finish", y="Task", color="Resource", custom_data="custom"
        )

        for bar in fig.data:
            bar.width = bar.customdata[0][0][0]
            if bar.customdata[0][0][1] == 1:
                bar.showlegend = False
        return fig

    @classmethod
    def make_timeline_data(cls, filenames: list[str]):
        query = f"""SELECT json_extract(rti_json_sample, '$.timestamp'),
        json_extract(rti_json_sample, '$.phase'),
        json_extract(rti_json_sample, '$.subPhase'),
        SampleInfo_reception_timestamp
        FROM 'ProcessPhase@0' WHERE json_extract(rti_json_sample, '$.timestamp') IS NOT NULL;"""

        list_df = [cls._process_db_query_to_df(filename, query) for filename in filenames]
        df = pd.concat(list_df)
        processes = cls._process_df_rows_to_processes(df)
        gannt = cls._process_data_to_gannt(processes)

        if len(gannt) == 0:
            return None

        return gannt

    @classmethod
    def _process_db_query_to_df(cls, filename, query):
        with sqlite3.connect(filename) as dbcon:
            df = pd.read_sql_query(query, dbcon, parse_dates={"SampleInfo_reception_timestamp": "ns"})
            timestamp_column = "json_extract(rti_json_sample, '$.timestamp')"
            df[timestamp_column] = df[timestamp_column].apply(json.loads)
            df[timestamp_column] = df[timestamp_column].apply(TimeConversionUtils.json_to_datetime)
        return df

    @classmethod
    def _process_df_rows_to_processes(cls, df):
        processes = {}
        for row in df.itertuples():
            timestamp = row[1]
            phase = row[2]
            subphase = row[3]
            processes.setdefault(phase, {}).setdefault(subphase, []).append(timestamp)
        return processes

    @classmethod
    def _process_data_to_gannt(cls, processes):
        gannt = []
        for process in processes:
            start, end = None, None
            for subprocess, timestamp in processes[process].items():
                if not subprocess.endswith("End"):
                    gannt.append(
                        dict(
                            Task=process,
                            Start=timestamp[0],
                            Finish=timestamp[0] + timedelta(seconds=0.2),
                            custom=[1, 1],
                            Resource=subprocess,
                        )
                    )
                if subprocess.endswith("End"):
                    end = timestamp[0]
                    start = end - timedelta(seconds=1) if start is None else start
                else:
                    start = timestamp[0]
                if start and end:
                    gannt.append(dict(Task=process, Start=start, Finish=end, Resource=process, custom=[0.25, 0.25]))
                    start = None
                    end = None

            if start and not end:
                end = processes[process][subprocess][-1] + timedelta(seconds=0.2)
                gannt.append(dict(Task=process, Start=start, Finish=end, Resource=process, custom=[0.25, 0.25]))

        return gannt
