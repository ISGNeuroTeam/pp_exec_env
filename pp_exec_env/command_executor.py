import configparser
import importlib.util
import logging
import os
import sys
from typing import Dict, List, Type, Callable

import execution_environment.command_executor as eece
import pandas as pd
from threadpoolctl import threadpool_limits

from pp_exec_env import config
from pp_exec_env.base_command import BaseCommand
from pp_exec_env.sys_commands import (
    SysWriteResultCommand,
    SysWriteInterProcCommand,
    SysReadInterProcCommand,
    LPP, SPP, IPS
)

FOLLOW_LINKS = config["plugins"]["follow_symlinks"]
SYS_WRITE_RESULT = config["system_commands"]["sys_write_result_name"]
SYS_WRITE_IPS = config["system_commands"]["sys_write_interproc_name"]
SYS_READ_IPS = config["system_commands"]["sys_read_interproc_name"]


class CommandExecutor(eece.CommandExecutor):
    """
    Implementation of execution_environment.CommandExecutor.
    This should be used by a Worker of Python Computing Node in order to execute PostProcessing commands.

    Attributes:
        command_classes: a dictionary of command name and their classes
        progress_message: Function for Worker-Server IPC logging
        current_depth: Subsearch depth in the current state of CommandExecutor
    """

    logger = logging.getLogger(config["logging"]["base_logger"])

    def __init__(self, storages: dict[str, str], commands_directory: str, progress_message: Callable):
        self.logger.info("Initialization started")
        self.logger.info("Importing system commands")
        self.command_classes = self._import_sys_commands(local_storage=storages[LPP],
                                                         shared_storage=storages[SPP],
                                                         ips=storages[IPS])
        self.progress_message = progress_message
        self.current_depth = 0  # Initial Subsearch depth.

        self.logger.info("Importing user commands")
        self.command_classes.update(self._import_user_commands(commands_directory))

        self.logger.info("Initialization finished")

    @staticmethod
    def _import_sys_commands(shared_storage: str, local_storage: str, ips: str) -> Dict[str, Type[BaseCommand]]:
        """
        Import system commands and set paths to storages for each of them.

        Args:
            shared_storage: Path to PostProcessing Shared Storage
            local_storage: Path to PostProcessing Local Storage
            ips: Path to InterProcessing Storage
        Returns:
            A dictionary with command names as keys and command classes as values

        Example Usage:

        >>> from pp_exec_env.command_executor import CommandExecutor
        >>> CommandExecutor._import_sys_commands("/tmp", "/tmp", "/tmp")
        {'sys_read_interproc': <class '...'>, 'sys_write_interproc': <class '...'>, 'sys_write_result': <class '...'>}
        """
        SysWriteResultCommand.shared_storage_path = shared_storage
        SysWriteResultCommand.local_storage_path = local_storage

        SysWriteResultCommand.ips_path = ips
        SysReadInterProcCommand.ips_path = ips
        SysWriteInterProcCommand.ips_path = ips

        command_classes = {
            SYS_READ_IPS: SysReadInterProcCommand,
            SYS_WRITE_IPS: SysWriteInterProcCommand,
            SYS_WRITE_RESULT: SysWriteResultCommand
        }
        return command_classes

    @staticmethod
    def _import_user_commands(commands_directory: str, follow_links: bool = FOLLOW_LINKS) -> Dict[str, Type[BaseCommand]]:
        """
        Import user-defined commands from the given folder.
        The following rules are applied to the plugins:

        - Each command should be present in commands_directory as a separate folder.
        - Command name is defined by its folder name.
        - Each command should contain __init__.py in its root.
        - __init__.py should define __all__.
        - Command class should be the 1st element in the __all__ list.
        - Plugin must support the same version of Python as the one in PostProcessing container image.
        - Site-packages are expected to be installed in the image *for now*.

        Args:
            commands_directory: Path to the folder that contains plugins.
            follow_links: If True, symbolic links will be treated as plugins as well.
                          Can be useful for testing.
        Returns:
            A dictionary with command names as keys and command classes as values

        Example Usage:

        >>> from pp_exec_env.command_executor import CommandExecutor
        >>> import os
        >>> CommandExecutor._import_user_commands(os.path.join(os.curdir, "tests", "resources", "commands"),
        ...                                                    follow_links=False)
        {'join': <class 'join.myjoin.JoinCommand'>, 'sum': <class 'sum.sum.SumCommand'>}
        """
        command_classes = {}

        for name in sorted(os.listdir(commands_directory)):
            path = os.path.join(commands_directory, name)

            link_bool = (not os.path.islink(path)) or follow_links  # Either not a link or links are allowed
            if os.path.isdir(path) and link_bool and os.path.exists(init_path := os.path.join(path, '__init__.py')):
                spec = importlib.util.spec_from_file_location(name, init_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                sys.modules.pop(spec.name)

                if module.__dict__.get('__all__', None) is None or not module.__all__:  # Existence and emptiness
                    CommandExecutor.logger.warning(f"Plugin {name} ignored, __all__ is empty or not found")
                    continue

                cls_name = module.__all__[0]
                cls: Type[BaseCommand] = module.__getattribute__(cls_name)
                if cls in [SYS_READ_IPS, SYS_WRITE_IPS, SYS_WRITE_RESULT]:
                    CommandExecutor.logger.warning(f"Plugin {name} ignored, cannot redefine system command")
                    continue

                config_path = os.path.join(path, "config.ini")
                files = None
                try:
                    files = cls.config.read(config_path)
                except configparser.ParsingError as e:
                    CommandExecutor.logger.warning(f"Ignoring config file of {cls_name} plugin ({config_path})")
                    CommandExecutor.logger.warning(e.message)
                    files = None
                finally:
                    if files:
                        CommandExecutor.logger.warning(f"Loaded config file for {cls_name}")

                command_classes[name] = cls
                CommandExecutor.logger.info(f"Added command {cls_name} with name `{name}`")
            else:
                CommandExecutor.logger.warning(f"Plugin {name} ignored, either not a folder or no __init__.py")

        return command_classes

    def execute(self, commands: List[Dict]) -> pd.DataFrame:
        """
        Execute a list of serialized OTL commands.

        Args:
            commands: List of dictionaries each containing serialized OTL commands.
        Returns:
            A pd.DataFrame that was created by given transformations.

        For example usage consider looking at tests.
        """
        self.logger.info("Execution started")
        df = None
        pipeline_len = len(commands)

        with threadpool_limits(limits=config.getint("threadpoolctl", "thread_limit"),
                               user_api=config["threadpoolctl"]["user_api"]):
            for idx, command in enumerate(commands):
                arguments = command['arguments']
                command_name = command['name']
                self.logger.info(f"Command {command_name} in progress...")

                command_cls = self.command_classes[command_name]
                get_arg = eece.GetArg(self, arguments)
                log_progress = self.get_command_progress_logger(command_name, idx, pipeline_len)

                command = command_cls(get_arg, log_progress)
                command.logger = self.logger.getChild(f"command.{command_name}")  # Not a part of the interface

                df = command.transform(df)

                if not isinstance(df, pd.DataFrame):
                    raise ValueError("You're doing something spooky, command must return a DataFrame")
        return df


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
