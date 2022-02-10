import os

import pandas as pd

from pp_exec_env import config
from pp_exec_env.base_command import BaseCommand, Syntax, Rule
from pp_exec_env.schema import (
    read_parquet_with_schema,
    read_jsonl_with_schema,
    write_parquet_with_schema,
    write_jsonl_with_schema
)

LPP = config["system_commands"]["local_storage_alias"]
SPP = config["system_commands"]["shared_storage_alias"]
IPS = config["system_commands"]["interproc_storage_alias"]
DEFAULT_DATA_PATH = config["system_commands"]["data_file_name"]
DEFAULT_SCHEMA_PATH = config["system_commands"]["schema_file_name"]


class SysReadInterProcCommand(BaseCommand):
    """
    An implementation of `ReadIPS` system command,
    which reads parquet and jsonl result files with schema from the InterProcessing Storage.
    """
    syntax = Syntax([Rule(name='path', type='kwarg', key='path', required=True),
                     Rule(name='storage_type', type='kwarg', key='storage_type', required=True)], use_timewindow=False)

    ips_path = ""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result_path = self.get_arg("path").value
        full_parquet_path = os.path.join(self.ips_path, result_path, 'parquet')
        full_jsonl_path = os.path.join(self.ips_path, result_path, 'jsonl')

        if os.path.exists(full_parquet_path):
            df = read_parquet_with_schema(os.path.join(full_parquet_path, DEFAULT_SCHEMA_PATH),
                                          os.path.join(full_parquet_path, DEFAULT_DATA_PATH))
        elif os.path.exists(full_jsonl_path):
            df = read_jsonl_with_schema(os.path.join(full_jsonl_path, DEFAULT_SCHEMA_PATH),
                                        os.path.join(full_jsonl_path, DEFAULT_DATA_PATH))
        else:
            raise ValueError(f"No parquet or jsonl folder found there: {os.path.join(self.ips_path, result_path)}")

        return df


class SysWriteResultCommand(BaseCommand):
    """
    An implementation of `WriteResult` system command,
    which writes jsonl result files with schema in the desired storage.

    Available storage options: Local Postprocessing and Shared Postprocessing
    """
    syntax = Syntax([Rule(name='path', type='kwarg', key='path', required=True),
                     Rule(name='storage_type', type='kwarg', key='storage_type', required=True)], use_timewindow=False)

    ips_path = ""
    local_storage_path = ""
    shared_storage_path = ""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result_path = self.get_arg("path").value
        stype = self.get_arg("storage_type").value

        if stype == LPP:
            base_path = self.local_storage_path
        elif stype == SPP:
            base_path = self.shared_storage_path
        else:
            raise ValueError(f"Unknown storage_type \"{stype}\"")

        jsonl_path = os.path.join(base_path, result_path, "jsonl")
        os.makedirs(jsonl_path, exist_ok=True)

        full_data_path = os.path.join(jsonl_path, DEFAULT_DATA_PATH)
        full_schema_path = os.path.join(jsonl_path, DEFAULT_SCHEMA_PATH)

        write_jsonl_with_schema(df, full_schema_path, full_data_path)
        return df


class SysWriteInterProcCommand(BaseCommand):
    """
    An implementation of `WriteIPS` system command,
    which writes parquet result files with schema in the InterProcessing Storage.
    """
    syntax = Syntax([Rule(name='path', type='kwarg', key='path', required=True),
                     Rule(name='storage_type', type='kwarg', key='storage_type', required=True)], use_timewindow=False)

    ips_path = ""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result_path = self.get_arg("path").value

        parquet_path = os.path.join(self.ips_path, result_path, "parquet")
        os.makedirs(parquet_path, exist_ok=True)

        full_data_path = os.path.join(parquet_path, DEFAULT_DATA_PATH)
        full_schema_path = os.path.join(parquet_path, DEFAULT_SCHEMA_PATH)

        write_parquet_with_schema(df, full_schema_path, full_data_path)
        return df
