import json
import sqlite3

import pandas as pd

from .utils import TimeConversionUtils


class RTILogReader:
    @classmethod
    def get_all_tables(cls, cur: sqlite3.Cursor):
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        return [table[0] for table in tables]

    @classmethod
    def get_tables_contains(cls, cur: sqlite3.Cursor, filter: str):
        tables = []
        for table in cls.get_all_tables(cur):
            cur.execute(f"PRAGMA table_info('{table}')")
            columns = cur.fetchall()
            for column in columns:
                if column[1] == filter:
                    tables.append(table)
        return tables

    @classmethod
    def get_channels_from_rti_json_sample(cls, cur: sqlite3.Cursor, table: str):
        query = f"SELECT rti_json_sample FROM '{table}';"
        cur.execute(query)
        data = cur.fetchone()
        if data is not None:
            channels = json.loads(data[0])
            for key, value in channels.items():
                channels[key] = type(value)
            return channels
        else:
            return {}

    @classmethod
    def get_channel_trace(cls, dbcon: sqlite3.Connection, table: str, channel: str):
        query = f"""SELECT json_extract(rti_json_sample, '$.timestamp'),
                json_extract(rti_json_sample, '$.{channel}'),
                SampleInfo_reception_timestamp
                FROM '{table}' WHERE json_extract(rti_json_sample, '$.{channel}') IS NOT NULL;"""

        df = pd.read_sql_query(query, dbcon, parse_dates={"SampleInfo_reception_timestamp": "ns"})

        if not df[df.columns[0]].isna().all():
            df["json_extract(rti_json_sample, '$.timestamp')"] = df[
                "json_extract(rti_json_sample, '$.timestamp')"
            ].apply(json.loads)
            df["json_extract(rti_json_sample, '$.timestamp')"] = df[
                "json_extract(rti_json_sample, '$.timestamp')"
            ].apply(TimeConversionUtils.json_to_datetime)

        return df

    @staticmethod
    def _validate_rti_json_sample(cur: sqlite3.Cursor, table: str):
        query = f"SELECT json_extract(rti_json_sample, '$') FROM '{table}';"
        cur.execute(query)
        channel = cur.fetchone()

        if channel is not None and len(channel) > 0:
            return True
        else:
            return False
