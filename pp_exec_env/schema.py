import re
import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd

# This is not technically correct, as BIGINT in Scala can go from LONG to BIGDECIMAL when needed
# BIGINT is set to pd.Int64Dtype for _time to be nullable (experimental)
DDL_TO_PANDAS = {
    "STRING": pd.StringDtype(),
    "FLOAT": "float32",
    "DOUBLE": "float64",
    "INTEGER": "int32",
    "INT": "int32",
    "LONG": "int64",
    "BIGINT": pd.Int64Dtype(),
    "NULL": "int64",
    "TIMESTAMP": "datetime64[ns]",
    "BOOLEAN": pd.BooleanDtype()
}

PANDAS_TO_DDL = {
    "int64": "LONG",
    np.int64: "LONG",
    pd.Int64Dtype(): "BIGINT",

    "int32": "INTEGER",
    pd.Int32Dtype(): "INTEGER",
    np.int32: "INTEGER",

    "int16": "INTEGER",
    np.int16: "INTEGER",
    pd.Int16Dtype(): "INTEGER",

    "int8": "INTEGER",
    pd.Int8Dtype(): "INTEGER",
    np.int8: "INTEGER",

    "float64": "DOUBLE",
    np.float64: "DOUBLE",
    pd.Float64Dtype(): "DOUBLE",

    "float32": "FLOAT",
    np.float32: "FLOAT",
    pd.Float32Dtype: "FLOAT",

    "bool": "BOOLEAN",
    np.bool_: "BOOLEAN",
    pd.BooleanDtype(): "BOOLEAN",

    "datetime64": "TIMESTAMP",
    np.datetime64: "TIMESTAMP",
    datetime.datetime: "TIMESTAMP",

    pd.StringDtype(): "STRING"
}

PYTHON_TO_DDL = {
    str: "STRING",
    int: "LONG",
    float: "DOUBLE",
    bool: "BOOLEAN"
}

OBJ_TYPE = np.dtype(np.object_)

# Group 1: Field Name
# Group 2: Type
# Group 3: Type if Group 2 was an ARRAY
# Group 4: NOT NULL
SPARK_FIELD_DDL_REGEX = re.compile("^`(.*)?` ([A-Z]+)(<[A-Z]+>)?( NOT NULL)?$")
DECIMAL_REGEX = re.compile(r"DECIMAL\(\d+\,\d+\)")


def ddl_to_pd_schema(ddl: str) -> Tuple[Dict, Dict]:
    """
    Convert Schema DDL string to a Pandas dtypes dictionary.
    The function helps to handle data coming from Spark.

    Args:
        ddl: String with data schema in DDL format.
             Usually produced by Spark.
    Returns:
        A tuple that contains two dictionaries.
        The first one has fields as keys and Pandas dtypes as values.
        The second one has fields as keys and DDL types as values.

    Example Usage:

    >>> ddl_str = "`_time` BIGINT,`some_field` DOUBLE,`another_field` ARRAY<INT>"
    >>> s, d = ddl_to_pd_schema(ddl_str)
    >>> s
    {'_time': Int64Dtype(), 'some_field': 'float64', 'another_field': dtype('O')}
    >>> d
    {'_time': 'BIGINT', 'some_field': 'DOUBLE', 'another_field': 'ARRAY<INT>'}
    """
    # Добавлен Костыль для исправления ошибки с полями типа DECIMAL(3,2) Заменяем DECIMAL на FLOAT
    # В будущем весь этот файл нужно переписывать и делать преобразование типов спарка в pandas с использованием pyspark
    # https://github.com/apache/spark/blob/master/python/pyspark/pandas/typedef/typehints.py
    fields = DECIMAL_REGEX.sub(
        'FLOAT', ddl
    ).split(',')  # Yep, would fail on Structures and Maps, which we do not support anyway

    schema = {}
    ddl_schema = {}

    for field in fields:
        field_name, field_type, array_type, _ = SPARK_FIELD_DDL_REGEX.match(field).groups()
        field_name = field_name.replace('``', '`')  # DDL escaping for backticks
        _field_type = field_type  # copy for later
        if array_type:
            field_type = None  # Pandas will think it is an Object, it does not have an implementation of Array dtype
            _field_type = f"{_field_type}{array_type}"
        schema[field_name] = DDL_TO_PANDAS.get(field_type, OBJ_TYPE)
        ddl_schema[field_name] = _field_type
    return schema, ddl_schema


def read_schema(schema_path: str) -> Tuple[Dict, Dict]:
    """
    Read DDL Schema file and convert it with `ddl_to_pd_schema`

    Args:
        schema_path: Path to the file. Usually filename is _SCHEMA.
    Returns:
        See `ddl_to_pd_schema` function.

    Example Usage:

    >>> from pp_exec_env.schema import read_schema
    >>> import os
    >>> schema, schema_ddl = read_schema(os.path.join(os.curdir, "tests", "resources", "misc", "sample_schema"))
    >>> schema
    {'_time': Int64Dtype(), 'some_field': 'float64', 'another_field': 'int32'}
    >>> schema_ddl
    {'_time': 'BIGINT', 'some_field': 'DOUBLE', 'another_field': 'INT'}
    """
    with open(schema_path) as file:
        ddl = file.read()

    return ddl_to_pd_schema(ddl)


def read_jsonl_with_schema(schema_path: str, data_path: str) -> pd.DataFrame:
    """
    Read jsonlines data and infer data types from schema

    Args:
        schema_path: Path to schema file. Usually filename is _SCHEMA.
        data_path: Path to file with data. Usually filename is data.
    Returns:
        A pd.DataFrame with data from the files.

    Example Usage:

    >>> from pp_exec_env.schema import read_jsonl_with_schema
    >>> import os
    >>> df = read_jsonl_with_schema(os.path.join(os.curdir, "tests", "resources", "data", "simple_jsonl", "_SCHEMA"),
    ...                             os.path.join(os.curdir, "tests", "resources", "data", "simple_jsonl", "data"))
    >>> df.head(1)
                _time
    Index
    0      1644423843
    """
    schema, ddl_schema = read_schema(schema_path)
    df = pd.read_json(data_path, lines=True, orient="records", dtype=schema, keep_default_dates=False)
    df.index.name = "Index"
    df.schema._initial_schema = ddl_schema  # Redefine initial schema to avoid upcasting as much as possible
    return df


def read_parquet_with_schema(schema_path: str, data_path: str) -> pd.DataFrame:
    """
    Read parquet data and infer data types from schema

    Args:
        schema_path: Path to schema file. Usually filename is _SCHEMA.
        data_path: Path to file with data. Usually filename is data.
    Returns:
        A pd.DataFrame with data from the files.

    Example Usage:

    >>> from pp_exec_env.schema import read_jsonl_with_schema
    >>> import os
    >>> df = read_parquet_with_schema(os.path.join(os.curdir, "tests", "resources", "data", "simple_parquet", "_SCHEMA"),
    ...                               os.path.join(os.curdir, "tests", "resources", "data", "simple_parquet", "data"))
    >>> df.head(1)
                _time
    Index
    0      1644425044
    """
    schema, ddl_schema = read_schema(schema_path)
    df = pd.read_parquet(data_path)
    df = df.astype(schema)
    df.index.name = "Index"
    df.schema._initial_schema = ddl_schema  # Redefine initial schema to avoid upcasting as much as possible
    return df


def write_schema(df: pd.DataFrame, schema_path: str):
    """
    Write schema of provided DataFrame to the given path.

    Args:
        df: Target pd.DataFrame.
        schema_path: Path for future schema.

    No example usage due to side effects.
    """
    with open(schema_path, 'w') as file:
        file.write(df.schema.ddl)


def write_jsonl_with_schema(df: pd.DataFrame, schema_path: str, data_path: str):
    """
    Write data and schema to the provided folder in jsonlines format.

    Args:
        df: Target pd.DataFrame.
        schema_path: Path for future schema.
        data_path: Path for future data.

    No example usage due to side effects.
    """
    write_schema(df, schema_path)
    df.to_json(data_path, lines=True, orient="records")


def write_parquet_with_schema(df: pd.DataFrame, schema_path: str, data_path: str):
    """
    Write data and schema to the provided folder in parquet format.

    Args:
        df: Target pd.DataFrame.
        schema_path: Path for future schema.
        data_path: Path for future data.

    No example usage due to side effects.
    """
    write_schema(df, schema_path)
    df.to_parquet(data_path, compression="snappy")


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
