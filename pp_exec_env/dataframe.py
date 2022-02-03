import pandas as pd
import numpy as np
from pp_exec_env.schema import PANDAS_TO_DDL, OBJ_TYPE, PYTHON_TO_DDL, DDL_TO_PANDAS


@pd.api.extensions.register_dataframe_accessor("schema")
class SchemaAccessor:
    def __init__(self, pandas_obj):
        self._obj: pd.DataFrame = pandas_obj
        self._specials = {}
        self._initial_schema = pandas_obj.schema

    @property
    def specials(self):
        return self._specials

    def add_special_ddl(self, field, ddl_type):
        self._specials[field] = ddl_type

    def get_dll_type(self, field, dtype):
        if field is None:
            raise ValueError("Field had name \"None\", probably index is unnamed")
        if dtype != OBJ_TYPE:
            if isinstance(dtype, np.dtype):
                dtype = dtype.type
            ddl_type = PANDAS_TO_DDL.get(dtype, None)
            if not ddl_type:
                raise TypeError(f"Unsupported Pandas type \"{dtype}\" at column \"{field}\"")
            return ddl_type
        else:
            # The first not NaN in the DataFrame
            # It's done through a mask so must be fast
            idx = self._obj[field].first_valid_index()
            if idx is None:  # None of the rows were not NaN
                initial_ddl = self._initial_schema.get(field, None)
                if initial_ddl is not None:
                    return initial_ddl
                return "NULL"
            value = self._obj[field].loc[idx]  # Value of the first not NaN
            if isinstance(value, str):
                return "STRING"
            elif isinstance(value, list):
                try:
                    sub_value = value[0]
                except IndexError:
                    # We will not retry this for performance reasons
                    # So if the first array is empty, then sad
                    raise TypeError(f"Could not determine array type at column \"{field}\" from the first attempt, "
                                    "you may try to resort it so that the first array is not empty")
                # Get type of the value in the Array
                sub_value_type = type(sub_value)
                sub_value_ddl_type = PYTHON_TO_DDL.get(sub_value_type, None) or PANDAS_TO_DDL.get(sub_value_type, None)
                if not sub_value_ddl_type:
                    raise TypeError(f"Unsupported type \"{sub_value_type}\" in an Array at column \"{field}\"")
                return f"ARRAY<{sub_value_ddl_type}>"
            else:
                # Some unknown Object
                raise TypeError(f"Could not determine type of the column \"{field}\"")

    @property
    def schema(self):
        schema = {**self._specials}  # Fancy way to copy, to avoid dealing with references
        fields = [(self._obj.index.name, self._obj.index.dtype),
                  *(filter(lambda x: x[0] not in self._specials.keys(), self._obj.dtypes.to_dict().items()))]
        for field, dtype in fields:
            schema[field] = self.get_dll_type(field, dtype)
            if isinstance(dtype, np.dtype):
                initial_ddl = self._initial_schema.get(field, None)
                if initial_ddl is not None and DDL_TO_PANDAS[initial_ddl] == dtype.name:
                    schema[field] = initial_ddl
        return schema

    @property
    def ddl(self):
        ddl = []
        for field, ddl_type in self.schema.items():
            field = field.replace('`', '``')  # escape backtick
            ddl.append(f"`{field}` {ddl_type}")
        return ','.join(ddl)
