import re
from typing import Dict, Tuple

import pandas as pd
import numpy as np


DDL_TO_PANDAS = {
    "STRING": pd.StringDtype(),
    "FLOAT": "float32",
    "DOUBLE":  "float64",
    "INTEGER": "int32",
    "INT": "int32",
    "LONG": "int64",
    "BIGINT": "int64",  # This is not technically correct, as BIGINT in Scala can go from LONG to BIGDECIMAL when needed
    "NULL": "int64",
}

PANDAS_TO_DDL = {
    "int64": "LONG",
    np.int64: "LONG",
    pd.Int64Dtype(): "LONG",

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

    pd.StringDtype(): "STRING"
}

PYTHON_TO_DDL = {
    str: "STRING",
    int: "LONG",
    float: "DOUBLE",
}

OBJ_TYPE = np.dtype(np.object_)


# Group 1: Field Name
# Group 2: Type
# Group 3: Type if Group 2 was an ARRAY
# Group 4: NOT NULL
SPARK_FIELD_DDL_REGEX = re.compile("^`(.*)?` ([A-Z]+)(<[A-Z]+>)?( NOT NULL)?$")


def read_schema(schema_path: str) -> Tuple[Dict, Dict]:
    with open(schema_path) as file:
        ddl = file.read()
    fields = ddl.split(',')  # Yep, would fail on Structures and Maps, which we do not support anyway

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


def read_jsonl_with_schema(schema_path: str, data_path: str) -> pd.DataFrame:
    schema, ddl_schema = read_schema(schema_path)
    df = pd.read_json(data_path, lines=True, orient='records', dtype=schema)
    df.index.name = "Index"
    df.schema._initial_schema = ddl_schema
    return df


def read_parquet_with_schema(schema_path: str, data_path: str) -> pd.DataFrame:
    schema, ddl_schema = read_schema(schema_path)
    df = pd.read_parquet(data_path)
    df = df.astype(schema)
    df.index.name = "Index"
    df.schema._initial_schema = ddl_schema
    return df
