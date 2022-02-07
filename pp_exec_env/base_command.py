from abc import abstractmethod
from typing import List

import execution_environment.base_command as eebc
import pandas as pd

from pp_exec_env.dataframe import SchemaAccessor

_ = SchemaAccessor  # Remove warning for unused import
Rule = eebc.Rule


class Syntax(eebc.Syntax):
    def __init__(self, argument_rules: List[Rule], use_timewindow: bool):
        self._argument_rules = argument_rules
        self.use_timewindow = use_timewindow

    @property
    def argument_rules(self) -> List[Rule]:
        return self._argument_rules

    @argument_rules.setter
    def argument_rules(self, argument_rules: List[Rule]):
        self._argument_rules = argument_rules

    def to_dict(self):
        return {"rules": [r.to_dict() for r in self.argument_rules], "use_timewindow": self.use_timewindow}


class BaseCommand(eebc.BaseCommand):
    @property
    @abstractmethod
    def syntax(self) -> Syntax:
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
