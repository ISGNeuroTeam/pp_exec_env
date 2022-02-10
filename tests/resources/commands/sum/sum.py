from pp_exec_env.base_command import BaseCommand, Syntax, Rule
import pandas as pd


class SumCommand(BaseCommand):
    syntax = Syntax([Rule(name="col", type="arg", input_types=['string', 'term'], required=True),
                     Rule(name="cols", type="arg", input_types=['string', 'term'], inf=True),
                     Rule(name="field_name", type="kwarg", key="name", input_types=["string", "term"], inf=True),],
                    use_timewindow=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = [self.get_arg('col').value] + [v.value for v in self.get_arg('cols', all=True)]
        field_name = self.get_arg('field_name').value

        df[field_name] = df[cols].sum(axis=1)

        return df
