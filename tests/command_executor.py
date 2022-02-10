import json
import os
import shutil
import unittest

import pandas as pd

from pp_exec_env.command_executor import CommandExecutor
from pp_exec_env.sys_commands import (
    SysWriteResultCommand,
    SysWriteInterProcCommand,
    SysReadInterProcCommand
)


class TestCommandExecutor(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.join(os.path.curdir, "tmp")
        self.resources = os.path.join(os.path.curdir, "resources")
        self.commands = os.path.join(self.resources, "commands")
        self.ips = os.path.join(self.tmp, "ips")  # InterProcessing Storage
        self.lpp = os.path.join(self.tmp, "lpp")  # Local PostProcessing Storage
        self.spp = os.path.join(self.tmp, "spp")  # Shared PostProcessing Storage

        shutil.rmtree(self.tmp, ignore_errors=True)
        os.makedirs(self.ips, exist_ok=True)
        os.makedirs(self.spp, exist_ok=True)
        os.makedirs(self.lpp, exist_ok=True)

        shutil.copytree(os.path.join(self.resources, "data", "input_data"), os.path.join(self.ips, "input_data"))
        shutil.copytree(os.path.join(self.resources, "data", "join_data"), os.path.join(self.ips, "join_data"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=False)
        self.df = None

    def test_sys_commands_import(self):
        commands = CommandExecutor._import_sys_commands(self.spp, self.lpp, self.ips)
        expected = {
            "sys_read_interproc": SysReadInterProcCommand,
            "sys_write_interproc": SysWriteInterProcCommand,
            "sys_write_result": SysWriteResultCommand
        }

        self.assertEqual(commands, expected)
        self.assertEqual(self.ips, commands["sys_read_interproc"].ips_path)
        self.assertEqual(self.ips, commands["sys_write_interproc"].ips_path)
        self.assertEqual(self.ips, commands["sys_write_result"].ips_path)
        self.assertEqual(self.lpp, commands["sys_write_result"].local_storage_path)
        self.assertEqual(self.spp, commands["sys_write_result"].shared_storage_path)

    def test_user_commands_import(self):
        commands = CommandExecutor._import_user_commands(self.commands, follow_links=False)
        expected = "{'sum': <class 'plugin.sum.SumCommand'>, 'join': <class 'plugin.myjoin.JoinCommand'>}"

        self.assertEqual(expected, commands.__str__())

    def test_execute(self):
        ce = CommandExecutor({"INTERPROCESSING": self.ips,
                              "LOCAL_POST_PROCESSING": self.lpp,
                              "SHARED_POST_PROCESSING": self.spp},
                             self.commands)

        self.assertLessEqual(5, len(ce.command_classes))

        with open(os.path.join(self.resources, "misc", "ce_otl.json")) as file:
            code = json.load(file)

        expected = pd.DataFrame([[1, 2, "a", 2.20], [2, 3, "b", 3.14], [3, 4, "c", 15.60]],
                                columns=["a", "b", "c", "d"])
        expected.index.name = "Index"
        expected["c"] = expected["c"].astype(pd.StringDtype())

        df = ce.execute(code)

        self.assertTrue(expected.equals(df))
        self.assertTrue(os.path.exists(os.path.join(self.ips, "output_data", "parquet")))
        self.assertTrue(os.path.exists(os.path.join(self.lpp, "output_data", "jsonl")))

    def test_full_pipeline(self):
        from otlang.otl import OTL

        code = f"""
        | sys_read_interproc path=input_data, storage_type=whatever
        | join 'a' [| sys_read_interproc path=join_data, storage_type=whatever]
        | sys_write_result path=output_data, storage_type=LOCAL_POST_PROCESSING
        | sys_write_interproc path=output_data, storage_type=whatever
        | sys_read_interproc path=output_data, storage_type=whatever
        """

        ce = CommandExecutor({"INTERPROCESSING": self.ips,
                              "LOCAL_POST_PROCESSING": self.lpp,
                              "SHARED_POST_PROCESSING": self.spp},
                             self.commands)
        syntax = ce.get_command_syntax()
        translator = OTL(syntax)
        job = [c.to_dict() for c in translator.translate(code)]

        expected = pd.DataFrame([[1, 2, "a", 2.20], [2, 3, "b", 3.14], [3, 4, "c", 15.60]],
                                columns=["a", "b", "c", "d"])
        expected.index.name = "Index"
        expected["c"] = expected["c"].astype(pd.StringDtype())

        df = ce.execute(job)

        self.assertTrue(expected.equals(df))
        self.assertTrue(os.path.exists(os.path.join(self.ips, "output_data", "parquet")))
        self.assertTrue(os.path.exists(os.path.join(self.lpp, "output_data", "jsonl")))


if __name__ == '__main__':
    unittest.main()
