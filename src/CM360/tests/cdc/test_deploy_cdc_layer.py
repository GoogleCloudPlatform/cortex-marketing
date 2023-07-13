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
"Unit tests for CDC layer deployment."

from pathlib import Path
import unittest
from unittest.mock import MagicMock
from unittest.mock import mock_open
from unittest.mock import patch

from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

from src.cdc.deploy_cdc_layer import _create_output_dir_structure
from src.cdc.deploy_cdc_layer import _create_sql_output_dir_structure
from src.cdc.deploy_cdc_layer import _get_sql_template
from src.cdc.deploy_cdc_layer import _parse_args
from src.cdc.deploy_cdc_layer import _populate_test_data
from src.cdc.deploy_cdc_layer import create_bq_schema
from src.py_libs.utils import TableNotFoundError


class TestDeployCDCLayer(unittest.TestCase):
    """Tests cdc.deploy_cdc_layer functionality."""

    def setUp(self) -> None:
        self.full_table_name = "project_id.dataset.table"

    @patch("pathlib.Path.exists", return_value=True)
    @patch("pathlib.Path.mkdir")
    @patch("shutil.rmtree")
    def test_create_output_dir_structure_cleans_up_if_exists_before_creation(
            self, rmtree_mock, mkdir_mock, exists_mock):
        # when
        _create_output_dir_structure()

        # then
        exists_mock.assert_called()
        rmtree_mock.assert_called()
        mkdir_mock.asset_called()

    @patch("pathlib.Path.exists", return_value=False)
    @patch("pathlib.Path.mkdir")
    @patch("shutil.rmtree")
    def test_create_output_dir_structure_created(self, rmtree_mock, mkdir_mock,
                                                 exists_mock):
        # when
        _create_output_dir_structure()

        # then
        exists_mock.assert_called()
        rmtree_mock.assert_not_called()
        mkdir_mock.assert_called()

    @patch("pathlib.Path.exists", return_value=True)
    @patch("pathlib.Path.mkdir")
    @patch("shutil.rmtree")
    def test_sql_output_dir_structure_cleans_up_if_exists_before_creation(
            self, rmtree_mock, mkdir_mock, exists_mock):
        # when
        _create_sql_output_dir_structure()

        # then
        exists_mock.assert_called()
        rmtree_mock.assert_called()
        mkdir_mock.asset_called()

    @patch("pathlib.Path.exists", return_value=False)
    @patch("pathlib.Path.mkdir")
    @patch("shutil.rmtree")
    def test_sql_output_dir_structure_created(self, rmtree_mock, mkdir_mock,
                                              exists_mock):
        """Directory was not present so it is generated."""
        # when
        _create_sql_output_dir_structure()

        # then
        exists_mock.assert_called()
        rmtree_mock.assert_not_called()
        mkdir_mock.asset_called()

    @patch("src.cdc.deploy_cdc_layer.Client", autospec=True)
    @patch("builtins.open")
    def test_populate_test_data_successful(self, open_mock, client_mock):
        # given
        client_mock.get_table.return_value = None
        client_mock.query.return_value = MagicMock(return_value=[{
            "counter": 0
        }])
        test_sql_script_path = Path("test.sql")

        # when
        _populate_test_data(client=client_mock,
                            path_to_script=test_sql_script_path,
                            full_table_name=self.full_table_name)

        # then
        open_mock.assert_called_once()
        client_mock.query.assert_called_once()

    @patch("src.cdc.deploy_cdc_layer.Client", autospec=True)
    def test_populate_test_data_table_does_not_exist(self, client_mock):
        # given
        client_mock.get_table.side_effect = NotFound("Table not found!")
        sql_script_path = Path("test.sql")

        # when, then
        with self.assertRaises(TableNotFoundError,
                               msg=(f"Test table {self.full_table_name}"
                                    "is not found!")):
            _populate_test_data(client=client_mock,
                                path_to_script=sql_script_path,
                                full_table_name=self.full_table_name)
        client_mock.query.assert_not_called()

    def test_parse_args_flag_provided(self):
        # given
        input_ = ["--debug"]

        # when
        args = _parse_args(input_)

        # then
        self.assertTrue(args.debug)

    def test_parse_args_flag_not_provided(self):
        # given
        input_ = []

        # when
        args = _parse_args(input_)

        # then
        self.assertFalse(args.debug)

    csv_content = ("SourceField,TargetField,DataType\n"
                   "Activity ID,activity_id,INT64\n"
                   "Event Type,event_type,STRING\n")

    @patch("builtins.open", new_callable=mock_open, read_data=csv_content)
    def test_create_bq_schema(self, mocked_open):
        """Correct schema is generated with additional system fields."""
        # given
        mapping_filepath = "any"
        expected_schema = [
            SchemaField(name="activity_id", field_type="INT64"),
            SchemaField(name="event_type", field_type="STRING"),
            SchemaField(name="recordstamp", field_type="TIMESTAMP"),
            SchemaField(name="source_file_name", field_type="STRING"),
            SchemaField(name="source_file_last_update_timestamp",
                        field_type="TIMESTAMP")
        ]

        # when
        generated_schema = create_bq_schema(mapping_filepath=mapping_filepath)

        # then
        mocked_open.assert_called_with(mapping_filepath,
                                       encoding="utf-8",
                                       newline="")
        self.assertListEqual(generated_schema, expected_schema)

    def test_get_sql_template_match(self):
        # given
        table_name = "match_table_activity"
        expected_sql_template = ("cm360_raw_to_cdc_template_match_table_files"
                                 ".sql")

        # when
        sql_table_template = _get_sql_template(table_name)

        # then
        self.assertEqual(sql_table_template, expected_sql_template)

    def test_get_sql_template_rest(self):
        # given
        table_name = "report_table"
        expected_sql_template = "cm360_raw_to_cdc_template_report_files.sql"

        # when
        sql_table_template = _get_sql_template(table_name)

        # then
        self.assertEqual(sql_table_template, expected_sql_template)


if __name__ == "__main__":
    unittest.main()
