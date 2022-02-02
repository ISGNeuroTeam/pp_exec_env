from abc import abstractmethod
from typing import List

import execution_environment.base_command as eebc
import pandas as pd


Rule = eebc.base_command.Rule


class Syntax(eebc.Syntax):
    def __init__(self, argument_rules: List[Rule], use_timewindow: bool):
        self.argument_rules = argument_rules
        self.use_timewindow = use_timewindow

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
