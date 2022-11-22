from typing import Dict

import numpy as np
import pandas as pd

from pp_exec_env.schema import PANDAS_TO_DDL, OBJ_TYPE, PYTHON_TO_DDL, DDL_TO_PANDAS


@pd.api.extensions.register_dataframe_accessor("schema")
class SchemaAccessor:
    """
    An extension to Pandas API that provides `schema` attribute for all Pandas DataFrames.
    The extension is automatically registered when importing anything from `pp_exec_env`.
    Thus, importing it alongside with BaseCommand or anything else is redundant.

    `Schema` attribute provides access to the current state of DDL Schema that
    will be written in _SCHEMA file when system command is executed.

    Additionally, it allows to modify the automatically generated DDL.

    Note:\n
    When the DataFrame object is first created, SchemaAccessor saves its initial schema
    and then uses it in order to reduce the amount of casting in DDL.
    In other words, if possible, types that were given to the DataFrame initially
    will be preserved.
    """
    def __init__(self, pandas_obj):
        self._obj: pd.DataFrame = pandas_obj
        self._specials = {}
        self._initial_schema = {}  # So that the next line works correctly
        self._initial_schema = self.schema

    @property
    def specials(self):
        """
        Dictionary of all fields that were manually modified
        """
        return self._specials

    def add_special_ddl(self, field: str, ddl_type: str):
        """
        Add custom ddl_type to the field. This will affect the DDL Schema of the DataFrame and _SCHEMA file.

        Args:
            field: Target field to set custom type for.
            ddl_type: String that will be used instead of automatically set DDL type.

        Example Usage:

        >>> import pandas as pd
        >>> from pp_exec_env.dataframe import SchemaAccessor
        >>> df = pd.DataFrame([[1,2,3]], columns=["a", "b", "c"])
        >>> df.schema.ddl
        '`a` LONG,`b` LONG,`c` LONG'
        >>> df.schema.add_special_ddl("a", "INT")
        >>> df.schema.ddl
        '`a` INT,`b` LONG,`c` LONG'
        """
        self._specials[field] = ddl_type

    def get_dll_type(self, field: str, dtype: str) -> str:
        """
        Get Spark DDL type from field and its dtype. (Redundant on purpose).
        The function tries to reduce the amount of upcasting and downcasting by using `_initial_schema`.

        Args:
            field: Target field name.
            dtype: Target field dtype.
        Returns:
            Spark DDL type string.

        Example Usage:

        >>> import pandas as pd
        >>> from pp_exec_env.dataframe import SchemaAccessor
        >>> df = pd.DataFrame([[1,2,3]], columns=["a", "b", "c"])
        >>> df.schema.get_dll_type("a", "int64")
        'LONG'
        >>> df.schema.get_dll_type("a", "int32")
        'INTEGER'
        """
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
    def schema(self) -> Dict[str, str]:
        """
        Get Spark DDL schema for pd.DataFrame.
        The function tries to reduce the amount of upcasting and downcasting by using `_initial_schema`.
        Special DDL types added through `add_special_ddl` function have higher priority.
        Additionally, they prevent field from being processed at all.

        Returns:
            A dictionary with field names as keys and Spark DDL types as values.

        Example Usage:

        >>> import pandas as pd
        >>> from pp_exec_env.dataframe import SchemaAccessor
        >>> df = pd.DataFrame([[1,2,3]], columns=["a", "b", "c"])
        >>> df.schema.schema
        {'a': 'LONG', 'b': 'LONG', 'c': 'LONG'}
        """
        schema = {**self._specials}  # Fancy way to copy, to avoid dealing with references
        fields = (filter(lambda x: x[0] not in self._specials.keys(), self._obj.dtypes.to_dict().items()))
        for field, dtype in fields:
            schema[field] = self.get_dll_type(field, dtype)
            # Now look for downcast on numpy types and try to remove as much of it, as possible
            if isinstance(dtype, np.dtype):
                initial_ddl = self._initial_schema.get(field, None)
                if initial_ddl is not None and DDL_TO_PANDAS.get(initial_ddl, None) == dtype.name:
                    schema[field] = initial_ddl
        return schema

    @property
    def ddl(self):
        """
        Get full DDL schema string from the DataFrame. Prepared to be written to _SCHEMA file.

        Returns:
            A Spark DDL schema string.

        Example Usage:

        >>> import pandas as pd
        >>> from pp_exec_env.dataframe import SchemaAccessor
        >>> df = pd.DataFrame([[1,2,3]], columns=["a", "b", "c"])
        >>> df.schema.ddl
        '`a` LONG,`b` LONG,`c` LONG'
        """
        ddl = []
        for field, ddl_type in self.schema.items():
            if not isinstance(field, str):
                field = str(field)
            field = field.replace('`', '``')  # escape backtick
            ddl.append(f"`{field}` {ddl_type}")
        return ','.join(ddl)


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
