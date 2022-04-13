import pandas as pd

from otlang.sdk.syntax import Positional, Keyword, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class SumCommand(BaseCommand):
    syntax = Syntax([Positional(name="col", otl_type=OTLType.TEXT, required=True),
                     Positional(name="cols", otl_type=OTLType.TEXT, inf=True),
                     Keyword(name="field_name", otl_type=OTLType.TEXT, inf=True)],
                    use_timewindow=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = [self.get_arg('col').value] + [v.value for v in self.get_iter('cols')]
        field_name = self.get_arg('field_name').value

        df[field_name] = df[cols].sum(axis=1)

        return df
