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
from src.cdc.deploy_cdc_layer import _parse_args
from src.cdc.deploy_cdc_layer import _populate_test_data
from src.cdc.deploy_cdc_layer import convert_to_bq_schema
from src.cdc.deploy_cdc_layer import generate_schema
from src.cdc.deploy_cdc_layer import TableNotFoundError


class TestDeployCDCLayer(unittest.TestCase):
    """Tests cdc.deploy_cdc_layer functionality."""

    project = "project_id"
    dataset = "dataset"
    table = "table"

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
                            project=self.project,
                            dataset=self.dataset,
                            table_name=self.table)

        # then
        open_mock.assert_called_once()
        client_mock.query.assert_called_once()

    @patch("src.cdc.deploy_cdc_layer.Client", autospec=True)
    def test_populate_test_data_table_does_not_exist(self, client_mock):
        # given
        client_mock.get_table.side_effect = NotFound("Table not found!")
        sql_script_path = Path("test.sql")

        # when, then
        with self.assertRaises(
                TableNotFoundError,
                msg=(f"Test table {self.project}.{self.dataset}.{self.table}"
                     " is not found!")):
            _populate_test_data(client=client_mock,
                                path_to_script=sql_script_path,
                                project=self.project,
                                dataset=self.dataset,
                                table_name=self.table)
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

    # pylint: disable=C0301
    csv_content = (
        "SourceField,ResponseField,TargetField,DataType\n"
        "campaign.resource_name,resource_name,resource_name,STRING\n"
        "campaign.id,id,id,INT64\n"
        "campaign.hotel_setting.hotel_center_id,hotel_setting,hotel_setting.hotel_center_id,INT64\n"
    )

    @patch("builtins.open", new_callable=mock_open, read_data=csv_content)
    def test_create_bq_schema_for_table(self, mocked_open):

        # given
        schema_file = "any"
        expected_schema = {
            "resource_name": "STRING",
            "id": "INT64",
            "hotel_setting": {
                "hotel_center_id": "INT64"
            }
        }
        # when
        generated_schema = generate_schema(schema_file=schema_file,
                                           bq_type="table")

        # then
        mocked_open.assert_called_with(schema_file,
                                       mode="r",
                                       encoding="utf-8",
                                       newline="")
        self.assertDictEqual(generated_schema, expected_schema)

    def test_convert_to_bq_schema(self):

        # given
        schema = {
            "resource_name": "STRING",
            "id": "INT64",
            "hotel_setting": {
                "hotel_center_id": "INT64"
            }
        }
        expected_schema = [
            SchemaField(name="resource_name", field_type="STRING"),
            SchemaField(name="id", field_type="INT64"),
            SchemaField(name="hotel_setting",
                        field_type="RECORD",
                        fields=[(SchemaField(name="hotel_center_id",
                                             field_type="INT64"))])
        ]

        # when
        generated_schema = convert_to_bq_schema(schema=schema)

        # then
        self.assertListEqual(generated_schema, expected_schema)


if __name__ == "__main__":
    unittest.main()
