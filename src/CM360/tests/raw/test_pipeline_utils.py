# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests of RAW Beam pipeline utilities."""

from datetime import date
from types import GeneratorType
import unittest
from unittest.mock import MagicMock
from unittest.mock import mock_open
from unittest.mock import patch

from apache_beam.io.filesystems import FileSystems

from src.raw.pipelines.helpers.pipeline_utils import _add_service_columns
from src.raw.pipelines.helpers.pipeline_utils import _cast_to_bool
from src.raw.pipelines.helpers.pipeline_utils import _cast_to_date
from src.raw.pipelines.helpers.pipeline_utils import _ColumnMapping
from src.raw.pipelines.helpers.pipeline_utils import _transform_bq_to_python_type
from src.raw.pipelines.helpers.pipeline_utils import change_dict_key_names
from src.raw.pipelines.helpers.pipeline_utils import create_bigquery_schema_from_mapping
from src.raw.pipelines.helpers.pipeline_utils import create_column_mapping
from src.raw.pipelines.helpers.pipeline_utils import get_max_recordstamp
from src.raw.pipelines.helpers.pipeline_utils import LackBigQueryToPythonTypeMappingError
from src.raw.pipelines.helpers.pipeline_utils import yield_dict_rows_from_compressed_csv


class TestPipelineUtils(unittest.TestCase):
    """Test case for pipeline utilities."""

    csv_content = ("one,two,three\n"
                   "1,2,3\n"
                   "11,22,33\n")
    mapping_content = ("SourceField,TargetField,DataType\n"
                       "Activity ID,activity_id,INT64\n"
                       "Event Time,event_time,INT64\n")

    def setUp(self):
        self.column_mapping = _ColumnMapping(original_name="ActivityID",
                                             target_name="activity_id",
                                             bq_type="INT64",
                                             python_type=int)
        self.mapping = {self.column_mapping.original_name: self.column_mapping}

    def test_cast_date_successful(self):
        # given
        expected = date(year=2012, month=1, day=19)

        # when
        result = _cast_to_date("2012-01-19 17:21:00 UTC")

        # then
        self.assertEqual(expected, result)

    def test_cast_bool_successful(self):
        # when
        result_true = _cast_to_bool("TRUE")
        result_false = _cast_to_bool("FALSE")

        # then
        self.assertTrue(result_true)
        self.assertFalse(result_false)

    def test_cast_bool_failed(self):
        # when
        with self.assertRaises(TypeError) as context:
            _cast_to_bool("")

        # then
        self.assertIn("This column cannot be casted to bool.",
                      str(context.exception))

    def test_transform_bq_to_python_type_int64(self):
        # given
        type_, expected = "INT64", int

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_float64(self):
        # given
        type_, expected = "FLOAT64", float

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_string(self):
        # given
        type_, expected = "STRING", str

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_bool(self):
        # given
        type_, expected = "BOOL", _cast_to_bool

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_date(self):
        # given
        type_, expected = "DATE", _cast_to_date

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_timestamp(self):
        # given
        type_, expected = "TIMESTAMP", float

        # when
        result = _transform_bq_to_python_type(bq_type=type_)

        # then
        self.assertEqual(result, expected)

    def test_transform_bq_to_python_type_not_valid_type(self):
        # given
        not_valid_type = "fsfsd"

        # when, then
        with self.assertRaises(LackBigQueryToPythonTypeMappingError):
            _transform_bq_to_python_type(bq_type=not_valid_type)

    def test_cast_according_to_bq_type(self):
        # given
        test_mapping = _ColumnMapping(original_name="ActivityID",
                                      target_name="activity_id",
                                      bq_type="INT64",
                                      python_type=int)

        result = test_mapping.cast_according_to_bq_type("1")
        self.assertEqual(1, result)

        result = test_mapping.cast_according_to_bq_type("")
        self.assertEqual(None, result)

        with self.assertLogs(level="WARNING") as log:
            result = test_mapping.cast_according_to_bq_type("test")

            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertEqual(None, result)

    def test_add_service_columns(self):
        # given
        original_columns = {"column1": 1}
        timestamp = 10000
        updated_timestamp = 1110000
        path = "gs://path"
        expected_columns = {
            **original_columns,
            "recordstamp": timestamp,
            "source_file_name": path,
            "source_file_last_update_timestamp": updated_timestamp,
        }

        # then
        result = _add_service_columns(row=original_columns,
                                      timestamp=timestamp,
                                      object_path=path,
                                      updated_timestamp=updated_timestamp)

        # then
        self.assertDictEqual(result, expected_columns)

    @patch("gzip.open", new_callable=mock_open, read_data=csv_content)
    @patch("src.raw.pipelines.helpers.pipeline_utils._add_service_columns",
           side_effect=lambda x, *_: x)
    @patch.object(FileSystems, "open")
    def test_yield_dict_rows_from_compressed_csv(self, file_system_mock,
                                                 add_service_columns_mock,
                                                 gzip_open_mock):
        # given
        dummy_timestamp = 1
        file = MagicMock()
        expected = [{
            "one": "1",
            "two": "2",
            "three": "3"
        }, {
            "one": "11",
            "two": "22",
            "three": "33"
        }]

        # when
        row_gen = yield_dict_rows_from_compressed_csv(file, dummy_timestamp)
        rows = list(row_gen)

        # then
        self.assertTrue(isinstance(row_gen, GeneratorType))
        file_system_mock.assert_called_once()
        gzip_open_mock.assert_called_once()
        self.assertEqual(add_service_columns_mock.call_count, 2)
        self.assertListEqual(rows, expected)

    def test_change_dict_key_names(self):
        # given
        row = {self.column_mapping.original_name: "123"}
        expected = {self.column_mapping.target_name: 123}

        # when
        result = change_dict_key_names(row, self.mapping)

        # then
        self.assertDictEqual(result, expected)

    def test_create_bigquery_schema_from_mapping(self):
        # given
        expected = {"fields": []}
        expected_column_mode = "NULLABLE"
        expected = {
            "fields": [{
                "name": self.column_mapping.target_name,
                "type": self.column_mapping.bq_type,
                "mode": expected_column_mode
            }]
        }

        # when
        result = create_bigquery_schema_from_mapping(self.mapping)

        # then
        self.assertDictEqual(result, expected)

    @patch("src.raw.pipelines.helpers.pipeline_utils._inject_service_columns",
           side_effect=lambda x: x)
    @patch("builtins.open", new_callable=mock_open, read_data=mapping_content)
    def test_create_column_mapping(self, open_mock,
                                   inject_service_columns_mock):
        # given
        dummy_path = "./somewhere.csv"
        activity_mapping = _ColumnMapping("Activity ID", "activity_id", "INT64",
                                          int)
        event_time_mapping = _ColumnMapping("Event Time", "event_time", "INT64",
                                            int)
        expected = {
            activity_mapping.original_name: activity_mapping,
            event_time_mapping.original_name: event_time_mapping
        }

        # when
        result = create_column_mapping(dummy_path)

        # then
        open_mock.assert_called_once()
        inject_service_columns_mock.assert_called_once()
        self.assertDictEqual(result, expected)

    def test_get_max_recordstamp_existing_table(self):
        # given
        total_rows = 1
        expected_max_recordstamp = 333
        any_three_values = [None] * 3

        client_mock, results, row = MagicMock(), MagicMock(), MagicMock()
        results.total_rows = total_rows
        row.max_recordstamp = expected_max_recordstamp
        results.__next__.return_value = row
        client_mock.query.return_value.result.return_value = results

        # when
        max_recordstamp = get_max_recordstamp(client_mock, *any_three_values)

        # then
        self.assertEqual(max_recordstamp, expected_max_recordstamp)

    def test_get_max_recordstamp_non_existing_table(self):
        # given
        total_rows = 0
        expected_max_recordstamp = None
        any_three_values = [None] * 3

        client_mock, results, row = MagicMock(), MagicMock(), MagicMock()
        results.total_rows = total_rows
        row.max_recordstamp = expected_max_recordstamp
        results.__next__.return_value = row
        client_mock.query.return_value.result.return_value = results

        # when
        max_recordstamp = get_max_recordstamp(client_mock, *any_three_values)

        # then
        self.assertIs(max_recordstamp, expected_max_recordstamp)


if __name__ == "__main__":
    unittest.main()
