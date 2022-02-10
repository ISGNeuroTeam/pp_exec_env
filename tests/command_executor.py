import json
import os
import shutil
import unittest

import pandas as pd

from pp_exec_env.command_executor import CommandExecutor, SYS_WRITE_RESULT, SYS_WRITE_IPS, SYS_READ_IPS
from pp_exec_env.sys_commands import (
    SysWriteResultCommand,
    SysWriteInterProcCommand,
    SysReadInterProcCommand,
    LPP, SPP, IPS
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
            SYS_READ_IPS: SysReadInterProcCommand,
            SYS_WRITE_IPS: SysWriteInterProcCommand,
            SYS_WRITE_RESULT: SysWriteResultCommand
        }

        self.assertEqual(commands, expected)
        self.assertEqual(self.ips, commands[SYS_READ_IPS].ips_path)
        self.assertEqual(self.ips, commands[SYS_WRITE_IPS].ips_path)
        self.assertEqual(self.ips, commands[SYS_WRITE_RESULT].ips_path)
        self.assertEqual(self.lpp, commands[SYS_WRITE_RESULT].local_storage_path)
        self.assertEqual(self.spp, commands[SYS_WRITE_RESULT].shared_storage_path)

    def test_user_commands_import(self):
        commands = CommandExecutor._import_user_commands(self.commands, follow_links=False)
        expected = "{'sum': <class 'plugin.sum.SumCommand'>, 'join': <class 'plugin.myjoin.JoinCommand'>}"

        self.assertEqual(expected, commands.__str__())

    def test_execute(self):
        ce = CommandExecutor({IPS: self.ips,
                              LPP: self.lpp,
                              SPP: self.spp},
                             self.commands)

        self.assertLessEqual(5, len(ce.command_classes))

        with open(os.path.join(self.resources, "misc", "ce_otl.json")) as file:
            job = json.load(file)

        job[0]["name"] = SYS_READ_IPS
        job[1]["arguments"]["jdf"][0]["value"][0]["name"] = SYS_READ_IPS
        job[2]["name"] = SYS_WRITE_RESULT
        job[2]["arguments"]["storage_type"][0]["value"] = LPP
        job[3]["name"] = SYS_WRITE_IPS
        job[4]["name"] = SYS_READ_IPS

        expected = pd.DataFrame([[1, 2, "a", 2.20], [2, 3, "b", 3.14], [3, 4, "c", 15.60]],
                                columns=["a", "b", "c", "d"])
        expected.index.name = "Index"
        expected["c"] = expected["c"].astype(pd.StringDtype())

        df = ce.execute(job)

        self.assertTrue(expected.equals(df))
        self.assertTrue(os.path.exists(os.path.join(self.ips, "output_data", "parquet")))
        self.assertTrue(os.path.exists(os.path.join(self.lpp, "output_data", "jsonl")))

    def test_full_pipeline(self):
        from otlang.otl import OTL

        code = f"""
        | {SYS_READ_IPS} path=input_data, storage_type=whatever
        | join 'a' [| {SYS_READ_IPS} path=join_data, storage_type=whatever]
        | {SYS_WRITE_RESULT} path=output_data, storage_type={LPP}
        | {SYS_WRITE_IPS} path=output_data, storage_type=whatever
        | {SYS_READ_IPS} path=output_data, storage_type=whatever
        """

        ce = CommandExecutor({IPS: self.ips,
                              LPP: self.lpp,
                              SPP: self.spp},
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
