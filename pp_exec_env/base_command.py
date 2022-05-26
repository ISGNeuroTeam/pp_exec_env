from abc import abstractmethod

import execution_environment.base_command as eebc
import pandas as pd


Syntax = eebc.Syntax


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
