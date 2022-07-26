import os
import datetime
import unittest
from pathlib import Path

import numpy as np

from pp_exec_env.schema import read_jsonl_with_schema


class TestDatetime(unittest.TestCase):
    def setUp(self):
        tests_path = "" if os.getcwd().endswith("tests") else "tests"
        self.data = Path(os.path.join(os.path.curdir, tests_path, "resources", "data"))
        self.df = read_jsonl_with_schema(self.data / "datetime" / "_SCHEMA", self.data / "datetime" / "test_dates.csv")

    def tearDown(self):
        self.df = None

    def test_initial_ddl(self):
        self.assertEqual(self.df.schema.ddl, "`test_date` TIMESTAMP,`datetime` TIMESTAMP,`records` STRING")

    def test_type_change(self):
        self.df.test_date = self.df.test_date.astype(int)
        self.assertEqual(self.df.schema.ddl, "`test_date` LONG,`datetime` TIMESTAMP,`records` STRING")

    def test_new_column_datetime(self):
        self.df["new"] = [datetime.datetime(2022, 2, 4) for _ in range(len(self.df))]
        expected = "`test_date` TIMESTAMP,`datetime` TIMESTAMP,`records` STRING,`new` TIMESTAMP"
        self.assertEqual(self.df.schema.ddl, expected)

    def test_new_column_np(self):
        self.df["new"] = [np.datetime64("2022-02-01 12:00:00") for _ in range(len(self.df))]
        expected = "`test_date` TIMESTAMP,`datetime` TIMESTAMP,`records` STRING,`new` TIMESTAMP"
        self.assertEqual(self.df.schema.ddl, expected)


class TestBoolean(unittest.TestCase):
    def setUp(self):
        tests_path = "" if os.getcwd().endswith("tests") else "tests"
        self.data = Path(os.path.join(os.path.curdir, tests_path, "resources", "data"))
        self.df = read_jsonl_with_schema(self.data / "boolean" / "_SCHEMA", self.data / "boolean" / "test_boolean.csv")

    def tearDown(self):
        self.df = None

    def test_initial_ddl(self):
        self.assertEqual(self.df.schema.ddl, "`records` BOOLEAN")

    def test_type_change(self):
        self.df.records = self.df.records.astype(np.int32)
        self.assertEqual(self.df.schema.ddl, "`records` INTEGER")

    def test_new_column_bool(self):
        self.df["new"] = [True for _ in range(len(self.df))]
        expected = "`records` BOOLEAN,`new` BOOLEAN"
        self.assertEqual(self.df.schema.ddl, expected)

    def test_new_column_bool_np(self):
        self.df["new"] = [np.bool_(1) for _ in range(len(self.df))]
        expected = "`records` BOOLEAN,`new` BOOLEAN"
        self.assertEqual(self.df.schema.ddl, expected)


if __name__ == '__main__':
    unittest.main()
