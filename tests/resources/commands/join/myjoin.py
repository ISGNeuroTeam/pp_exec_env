import pandas as pd
from otlang.sdk.syntax import Positional, Keyword, Subsearch, OTLType

from pp_exec_env.base_command import BaseCommand, Syntax


class JoinCommand(BaseCommand):
    syntax = Syntax([Positional(name='field', required=True, otl_type=OTLType.TEXT),
                     Positional(name='fields', inf=True, otl_type=OTLType.TEXT),
                     Keyword(name='type', required=False, otl_type=OTLType.TEXT),
                     Subsearch(name='jdf', required=True)])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        fields = [self.get_arg('field').value] + [v.value for v in self.get_iter('fields')]
        join_type = self.get_arg('type').value or 'left'
        jdf = self.get_arg('jdf').value
        return pd.merge(left=df, right=jdf, on=fields, how=join_type)
