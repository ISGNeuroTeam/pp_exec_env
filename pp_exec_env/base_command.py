from abc import abstractmethod
from typing import List

import execution_environment.base_command as eebc
import pandas as pd
from otlang.sdk.syntax import APIRule

Rule = eebc.Rule


class Syntax(eebc.Syntax):
    """
    Syntax object should be used to define `syntax` attribute of a command that inherits `BaseCommand`.

    Attributes:
        argument_rules: A list of `APIRule` instances that will be used during syntax analysis.
                        See `Rule` class documentation.
        use_timewindow: If set to True, command will be provided with additional Arguments
                        that give information about desired time window.
                        # TODO: Implement
    """
    def __init__(self, argument_rules: List[APIRule], use_timewindow: bool):
        self._argument_rules = argument_rules
        self.use_timewindow = use_timewindow

    @property
    def argument_rules(self) -> List[APIRule]:
        return self._argument_rules

    @argument_rules.setter
    def argument_rules(self, argument_rules: List[APIRule]):
        self._argument_rules = argument_rules

    def to_dict(self):
        return {"rules": [r.to_dict() for r in self.argument_rules], "use_timewindow": self.use_timewindow}


class BaseCommand(eebc.BaseCommand):
    """
    This abstract class should be used by developers of PostProcessing commands
    in order to create the commands.

    Each Command should define a `syntax` attribute as a class variable
    In other words, it should be accessible without instance initialization.

    Each Command should also define a `transform` method that takes
    a pd.DataFrame as its only argument and returns a pd.DataFrame.
    Inside transform developer is free to define any transformations with the given DataFrame.
    """
    @property
    @abstractmethod
    def syntax(self) -> Syntax:
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
