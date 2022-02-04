from typing import Dict, List

import execution_environment.command_executor as eece
from pp_exec_env.sys_commands import SysWriteResultCommand, SysWriteInterProcCommand, SysReadInterProcCommand, LPP, SPP, IPS
import pandas as pd


class CommandExecutor(eece.CommandExecutor):
    """
    Abstract class that manages command imports and OTL execution for python_computing_node
    """

    def __init__(self, storages: dict[str, str], commands_directory: str):
        self.local_storage = storages[LPP]
        self.shared_storage = storages[SPP]
        self.ips = storages[IPS]

        self.command_classes = {}
        self.commands_directory = commands_directory

        self._import_sys_commands()
        self._import_user_commands()

    def _import_sys_commands(self):
        """
        Initialize system commands in the CommandExecutor.
        Reference implementation provided.
        """
        SysWriteResultCommand.shared_storage_path = self.shared_storage
        SysWriteResultCommand.local_storage_path = self.local_storage

        SysWriteResultCommand.ips_path = self.ips
        SysReadInterProcCommand.ips_path = self.ips
        SysWriteInterProcCommand.ips_path = self.ips

        self.command_classes["sys_read_interproc"] = SysReadInterProcCommand
        self.command_classes["sys_write_interproc"] = SysWriteInterProcCommand

        self.command_classes["sys_write_result"] = SysWriteResultCommand

    def _import_user_commands(self):
        """
        Initialize custom commands in the CommandExecutor.
        """
        import os
        import importlib.util
        import sys

        for name in os.listdir(self.commands_directory):
            path = os.path.join(self.commands_directory, name)
            if os.path.isdir(path) and os.path.exists(init_path := os.path.join(path, '__init__.py')):
                spec = importlib.util.spec_from_file_location("plugin", init_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                sys.modules.pop(spec.name)

                if module.__dict__.get('__all__', None) is None or not module.__all__:  # Existence and emptiness
                    continue

                cls_name = module.__all__[0]
                cls = module.__getattribute__(cls_name)
                if cls not in ['sys_read_interproc', 'sys_write_interproc', 'sys_write_result']:
                    self.command_classes[name] = cls

    def execute(self, commands: List[Dict]):
        """
        Execute program provided in `commands`.
        Reference implementation provided.
        """
        df = None
        for command in commands:
            arguments = command['arguments']
            command_name = command['name']
            command_cls = self.command_classes[command_name]
            get_arg = eece.GetArg(self, arguments)
            command = command_cls(get_arg)
            df = command.transform(df)
            if not isinstance(df, pd.DataFrame):
                raise ValueError("You're doing something spooky, command must return a DataFrame")
        return df
