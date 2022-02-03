import os

from pp_exec_env.base_command import BaseCommand, Syntax, Rule
from pp_exec_env.schema import read_parquet_with_schema, read_jsonl_with_schema
import pandas as pd


SPP = "SHARED_POST_PROCESSING"
LPP = "LOCAL_POST_PROCESSING"
IPS = "INTERPROCESSING"
DEFAULT_DATA_PATH = "data"
DEFAULT_SCHEMA_PATH = "_SCHEMA"


class SysReadInterProcCommand(BaseCommand):
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
            raise ValueError(f"Path not found: {os.path.join(self.ips_path, result_path)}")

        return df


class SysWriteResultCommand(BaseCommand):
    syntax = Syntax([Rule(name='path', type='kwarg', key='path', required=True),
                     Rule(name='storage_type', type='kwarg', key='storage_type', required=True)], use_timewindow=False)

    ips_path = ""
    local_storage_path = ""
    shared_storage_path = ""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result_path = self.get_arg("path").value
        stype = self.get_arg("storage_type").value
        base_path = ""

        if stype == LPP:
            base_path = self.local_storage_path
        elif stype == SPP:
            base_path = self.shared_storage_path
        else:
            raise ValueError(f"Unknown storage_type \"{stype}\"")

        full_data_path = os.path.join(base_path, result_path, "jsonl", DEFAULT_DATA_PATH)
        full_schema_path = os.path.join(base_path, result_path, "jsonl", DEFAULT_SCHEMA_PATH)

        with open(full_schema_path, 'w') as file:
            file.write(df.schema.ddl)
        df.to_json(full_data_path, lines=True, orient='records')
        return df


class SysWriteInterProcCommand(BaseCommand):
    syntax = Syntax([Rule(name='path', type='kwarg', key='path', required=True),
                     Rule(name='storage_type', type='kwarg', key='storage_type', required=True)], use_timewindow=False)

    ips_path = ""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result_path = self.get_arg("path").value
        full_data_path = os.path.join(self.ips_path, result_path, "parquet", DEFAULT_DATA_PATH)
        full_schema_path = os.path.join(self.ips_path, result_path, "parquet", DEFAULT_SCHEMA_PATH)

        with open(full_schema_path, 'w') as file:
            file.write(df.schema.ddl)
        df.to_parquet(full_data_path, compression="snappy")
        return df
