import importlib.util
import logging
import os
import sys
from typing import Dict, List, Type

import execution_environment.command_executor as eece
import pandas as pd

from pp_exec_env.base_command import BaseCommand
from pp_exec_env.sys_commands import (
    SysWriteResultCommand,
    SysWriteInterProcCommand,
    SysReadInterProcCommand,
    LPP, SPP, IPS
)


class CommandExecutor(eece.CommandExecutor):
    """
    Implementation of execution_environment.CommandExecutor.

    Attributes:
        commands_directory: path to the folder with commands packages
        command_classes: a dictionary of command name and their classes
        logger: logging.Logger instance
        local_storage: path to mounted PostProcessing Local Storage
        shared_storage: path to mounted PostProcessing Shared Storage
        ips: path to mounted InterProcessing Storage
    """

    def __init__(self, storages: dict[str, str], commands_directory: str):
        self.logger = logging.getLogger("PostProcessing")
        self.logger.info("Initialization started")
        self.local_storage = storages[LPP]
        self.shared_storage = storages[SPP]
        self.ips = storages[IPS]

        self.command_classes: Dict[str, Type[BaseCommand]] = {}
        self.commands_directory = commands_directory

        self.logger.info("Importing commands")
        self._import_sys_commands()
        self._import_user_commands()
        self.logger.info("Initialization finished")

    def _import_sys_commands(self):
        """
        Initialize system commands in the CommandExecutor.
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

        for name in os.listdir(self.commands_directory):
            path = os.path.join(self.commands_directory, name)
            if os.path.isdir(path) and os.path.exists(init_path := os.path.join(path, '__init__.py')):
                spec = importlib.util.spec_from_file_location("plugin", init_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                sys.modules.pop(spec.name)

                if module.__dict__.get('__all__', None) is None or not module.__all__:  # Existence and emptiness
                    self.logger.warning(f"Plugin {name} ignored, __all__ is empty or not found")
                    continue

                cls_name = module.__all__[0]
                cls = module.__getattribute__(cls_name)
                if cls not in ['sys_read_interproc', 'sys_write_interproc', 'sys_write_result']:
                    self.command_classes[name] = cls
                else:
                    self.logger.warning(f"Plugin {name} ignored, cannot redefine system command")
            else:
                self.logger.warning(f"Plugin {name} ignored, either not a folder or no __init__.py")

    def execute(self, commands: List[Dict]):
        """
        Execute OTLTree provided in `commands`

        Args:
            commands: a list of all commands with their arguments to run
        Raises:
            Pretty much anything that and be raised
        """
        self.logger.info("Execution started")
        df = None
        for command in commands:
            arguments = command['arguments']
            command_name = command['name']
            self.logger.info(f"Command {command_name} in progress...")

            command_cls = self.command_classes[command_name]
            get_arg = eece.GetArg(self, arguments)
            command = command_cls(get_arg, self.logger.getChild(f"command.{command_name}"))

            df = command.transform(df)

            if not isinstance(df, pd.DataFrame):
                raise ValueError("You're doing something spooky, command must return a DataFrame")
        return df
