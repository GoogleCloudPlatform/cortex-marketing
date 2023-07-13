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
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import GeneratorType
import unittest
from unittest.mock import MagicMock
from unittest.mock import mock_open
from unittest.mock import patch

from src.raw.pipelines.helpers.pipeline_utils import _add_service_columns
from src.raw.pipelines.helpers.pipeline_utils import _cast_to_date
from src.raw.pipelines.helpers.pipeline_utils import _ColumnMapping
from src.raw.pipelines.helpers.pipeline_utils import _transform_bq_to_python_type
from src.raw.pipelines.helpers.pipeline_utils import change_dict_key_names
from src.raw.pipelines.helpers.pipeline_utils import create_bigquery_schema_from_mapping
from src.raw.pipelines.helpers.pipeline_utils import create_column_mapping
from src.raw.pipelines.helpers.pipeline_utils import generate_query
from src.raw.pipelines.helpers.pipeline_utils import get_available_client_ids
from src.raw.pipelines.helpers.pipeline_utils import get_credentials_from_secret_manager
from src.raw.pipelines.helpers.pipeline_utils import get_data_for_customer
from src.raw.pipelines.helpers.pipeline_utils import get_max_recordstamp
from src.raw.pipelines.helpers.pipeline_utils import get_select_columns
from src.raw.pipelines.helpers.pipeline_utils import LackBigQueryToPythonTypeMappingError


class TestPipelineUtils(unittest.TestCase):
    """Tests raw.pipeline.helpers.pipeline_utils functionality."""

    csv_content = ("one,two,three\n"
                   "1,2,3\n"
                   "11,22,33\n")
    mapping_content = (
        "SourceField,ResponseField,TargetField,DataType\n"
        "campaign.resource_name,resource_name,resource_name,STRING\n"
        "campaign.id,id,id,INT64\n")

    def setUp(self):
        self.column_mapping = _ColumnMapping(original_name="id",
                                             target_name="id",
                                             bq_type="INT64",
                                             python_type=int)
        self.mapping = {self.column_mapping.original_name: self.column_mapping}
        self.credentials = {
            "login_customer_id": "some_id",
            "password": "some_pass"
        }
        self.api_version = "v13"
        self.api_name = "some_api"

    def test_cast_date_successful(self):
        # given
        expected = date(year=2012, month=1, day=19)

        # when
        result = _cast_to_date("2012-01-19 17:21:00 UTC")

        # then
        self.assertEqual(expected, result)

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
        type_, expected = "BOOL", bool

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
        expected_columns = {**original_columns, "recordstamp": timestamp}

        # then
        result = _add_service_columns(row=original_columns, timestamp=timestamp)

        # then
        self.assertDictEqual(result, expected_columns)

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

    @patch("builtins.open", new_callable=mock_open, read_data=mapping_content)
    def test_create_column_mapping(self, open_mock):
        # given
        dummy_path = "./somewhere.csv"
        resource_mapping = _ColumnMapping("resource_name", "resource_name",
                                          "STRING", str)
        id_mapping = _ColumnMapping("id", "id", "INT64", int)
        recordtimestamp_mapping = _ColumnMapping("recordstamp", "recordstamp",
                                                 "TIMESTAMP", float)
        expected = {
            resource_mapping.original_name: resource_mapping,
            id_mapping.original_name: id_mapping,
            recordtimestamp_mapping.original_name: recordtimestamp_mapping
        }

        # when
        result = create_column_mapping(dummy_path)

        # then
        open_mock.assert_called_once()
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

    def test_generate_query_no_date(self):
        # given
        column_names = ["first_column", "second_column", "third_column"]
        expected = f"SELECT {', '.join(column_names)} FROM {self.api_name} "
        start_window = None
        end_window = None

        # when
        result = generate_query(column_names, self.api_name, start_window,
                                end_window)

        # then
        self.assertEqual(result, expected)

    def test_generate_query_with_date(self):
        # given
        column_names = ["first_column", "second_column", "third_column"]

        start_window = datetime.now(tz=timezone.utc)
        end_window = datetime.now(tz=timezone.utc) - timedelta(days=10)
        start_day = start_window.strftime("%Y-%m-%d")
        end_day = end_window.strftime("%Y-%m-%d")

        expected = f"""SELECT {', '.join(column_names)} FROM {self.api_name
        } WHERE segments.date BETWEEN '{start_day}' AND '{end_day}'"""

        # when
        result = generate_query(column_names, self.api_name, start_window,
                                end_window)

        # then
        self.assertEqual(result, expected)

    # pylint: disable=C0301
    @patch(
        "src.raw.pipelines.helpers.pipeline_utils.GoogleAdsClient.load_from_dict"
    )
    def test_get_availible_client_ids(self, google_client_mock):
        # given
        is_metric_table = True

        # when
        get_available_client_ids(credentials=self.credentials,
                                 api_version=self.api_version,
                                 is_metric_table=is_metric_table)

        # result
        google_client_mock.assert_called()

    @patch("builtins.open", new_callable=mock_open, read_data=mapping_content)
    def test_get_select_columns(self, open_mock):
        # given
        dummy_path = "./somewhere.csv"

        expected_list = ["campaign.resource_name", "campaign.id"]

        # when
        result_list = get_select_columns(path_to_mapping=dummy_path)

        # result
        open_mock.assert_called_once()
        self.assertEqual(len(result_list), len(expected_list))

    # pylint: disable=C0301
    @patch(
        "src.raw.pipelines.helpers.pipeline_utils.secretmanager.SecretManagerServiceClient"
    )
    @patch("src.raw.pipelines.helpers.pipeline_utils.ast.literal_eval")
    def test_get_credentials_from_secret_manager(self, ast_mock,
                                                 secret_manager_mock):
        # given
        some_secret = "some_secret"
        some_project = "some_project"

        # when
        get_credentials_from_secret_manager(project_id=some_project,
                                            secret_name=some_secret)

        # result
        secret_manager_mock.assert_called_once()
        ast_mock.assert_called_once()

    # pylint: disable=C0301
    @patch(
        "src.raw.pipelines.helpers.pipeline_utils.GoogleAdsClient.load_from_dict"
    )
    def test_get_data_for_customer(self, client_mock):

        # given
        some_customer = "123456789"
        dummy_query = "SELECT somithing FROM some_source"
        timestamp = datetime.now(tz=timezone.utc)
        some_bool = True

        mock_ads_client = client_mock.return_value

        mock_ads_service = mock_ads_client.get_service.return_value
        mock_stream = mock_ads_service.search_stream.return_value

        # when
        data_gen = get_data_for_customer(customer_id=some_customer,
                                         credentials=self.credentials,
                                         api_version=self.api_version,
                                         api_name=self.api_name,
                                         query=dummy_query,
                                         timestamp=timestamp,
                                         is_metric_table=some_bool)
        data_list = list(data_gen)

        # result
        self.assertTrue(isinstance(data_gen, GeneratorType))
        client_mock.assert_called_with(self.credentials,
                                       version=self.api_version)
        mock_ads_service.search_stream.assert_called_once()
        mock_stream.__iter__.assert_called_once()
        self.assertListEqual(data_list, [])


if __name__ == "__main__":
    unittest.main()
