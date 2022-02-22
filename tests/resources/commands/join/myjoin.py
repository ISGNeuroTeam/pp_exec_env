from pp_exec_env.base_command import BaseCommand, Syntax, Rule
import pandas as pd


class JoinCommand(BaseCommand):
    syntax = Syntax([Rule(name='field', type='arg', required=True, input_types=['string', 'term']),
                     Rule(name='fields', type='arg', inf=True, input_types=['string', 'term']),
                     Rule(name='type', type='kwarg', required=False, input_types=['string', 'term']),
                     Rule(name='jdf', type='subsearch', required=True)],
                    use_timewindow=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        fields = [self.get_arg('field').value] + [v.value for v in self.get_iter('fields')]
        join_type = self.get_arg('type').value or 'left'
        jdf = self.get_arg('jdf').value

        return pd.merge(left=df, right=jdf, on=fields, how=join_type)
