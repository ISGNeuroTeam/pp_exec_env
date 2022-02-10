import unittest
import os
import shutil
import logging

import pandas as pd

from execution_environment.command_executor import GetArg
from pp_exec_env.schema import (
    read_jsonl_with_schema,
    read_parquet_with_schema,
    write_jsonl_with_schema,
    write_parquet_with_schema
)
from pp_exec_env.sys_commands import (
    SysWriteResultCommand,
    SysWriteInterProcCommand,
    SysReadInterProcCommand,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_DATA_PATH,
    IPS, LPP, SPP
)


class TestCommands(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.join(os.path.curdir, "tmp")
        self.resources = os.path.join(os.path.curdir, "resources")
        self.ips = os.path.join(self.tmp, "ips")  # InterProcessing Storage
        self.lpp = os.path.join(self.tmp, "lpp")  # Local PostProcessing Storage
        self.spp = os.path.join(self.tmp, "spp")  # Shared PostProcessing Storage

        SysWriteResultCommand.ips_path = self.ips
        SysWriteResultCommand.local_storage_path = self.lpp
        SysWriteResultCommand.shared_storage_path = self.spp

        SysReadInterProcCommand.ips_path = self.ips
        SysWriteInterProcCommand.ips_path = self.ips

        shutil.rmtree(self.tmp, ignore_errors=True)
        os.makedirs(self.ips, exist_ok=True)
        os.makedirs(self.spp, exist_ok=True)
        os.makedirs(self.lpp, exist_ok=True)

        try:

            self.df = read_jsonl_with_schema(os.path.join(self.resources, "data", "simple_jsonl", DEFAULT_SCHEMA_PATH),
                                             os.path.join(self.resources, "data", "simple_jsonl", DEFAULT_DATA_PATH))

            os.makedirs(os.path.join(self.ips, "read_input_jsonl", "jsonl"))
            os.makedirs(os.path.join(self.ips, "read_input_parquet", "parquet"))

            write_jsonl_with_schema(self.df,
                                    os.path.join(self.ips, "read_input_jsonl", "jsonl", DEFAULT_SCHEMA_PATH),
                                    os.path.join(self.ips, "read_input_jsonl", "jsonl", DEFAULT_DATA_PATH))
            write_parquet_with_schema(self.df,
                                      os.path.join(self.ips, "read_input_parquet", "parquet", DEFAULT_SCHEMA_PATH),
                                      os.path.join(self.ips, "read_input_parquet", "parquet", DEFAULT_DATA_PATH))
        except Exception as e:
            shutil.rmtree(self.tmp, ignore_errors=False)
            raise e

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=False)
        self.df = None

        SysWriteResultCommand.ips_path = None
        SysWriteResultCommand.local_storage_path = None
        SysWriteResultCommand.shared_storage_path = None

        SysReadInterProcCommand.ips_path = None
        SysWriteInterProcCommand.ips_path = None

    def test_sys_write_commands(self):
        args = {'path':
                    [{'value': 'output_data', 'key': 'path', 'type': 'term', 'named_as': '', 'group_by': [],
                      'arg_type': 'arg'}],
                'storage_type':
                    [{'value': '', 'key': 'storage_type', 'type': 'term', 'named_as': '', 'group_by': [],
                      'arg_type': 'arg'}]
                }
        logger = logging.getLogger("Testing.SysWrite*")

        for cls, storage_type, storage_path, file_format in [
            (SysWriteInterProcCommand, IPS, self.ips, "parquet"),
            (SysWriteResultCommand, LPP, self.lpp, "jsonl"),
            (SysWriteResultCommand, SPP, self.spp, "jsonl")
        ]:
            with self.subTest(msg=f"Storage Type: {storage_type}",
                              storage_type=storage_type,
                              storage_path=storage_path,
                              file_format=file_format):
                args["storage_type"][0]["value"] = storage_type
                get_arg = GetArg(None, args)

                cmd = cls(get_arg, None)
                cmd.transform(self.df)

                self.assertTrue(os.path.exists(os.path.join(storage_path, "output_data")))
                self.assertTrue(os.path.exists(os.path.join(storage_path, "output_data", file_format)))
                self.assertTrue(
                    os.path.exists(os.path.join(storage_path, "output_data", file_format, DEFAULT_DATA_PATH)))
                self.assertTrue(
                    os.path.exists(os.path.join(storage_path, "output_data", file_format, DEFAULT_SCHEMA_PATH)))

                func = read_jsonl_with_schema if file_format == "jsonl" else read_parquet_with_schema
                ndf = func(os.path.join(storage_path, "output_data", file_format, DEFAULT_SCHEMA_PATH),
                           os.path.join(storage_path, "output_data", file_format, DEFAULT_DATA_PATH))

                self.assertTrue(ndf.equals(self.df))
                self.assertEqual(self.df.schema.ddl, ndf.schema.ddl)

    def test_sys_read_interproc(self):
        args = {
            'path': [{'value': '', 'key': 'path', 'type': 'term', 'named_as': '', 'group_by': [], 'arg_type': 'arg'}]
        }
        logger = logging.getLogger("Testing.SysReadInterProc")

        for file_format in ["parquet", "jsonl"]:
            with self.subTest(file_format=file_format):
                args["path"][0]["value"] = f"read_input_{file_format}"
                get_arg = GetArg(None, args)

                cmd = SysReadInterProcCommand(get_arg, None)
                ndf = cmd.transform(pd.DataFrame())

                self.assertTrue(ndf.equals(self.df))
                self.assertEqual(self.df.schema.ddl, ndf.schema.ddl)


if __name__ == '__main__':
    unittest.main()
