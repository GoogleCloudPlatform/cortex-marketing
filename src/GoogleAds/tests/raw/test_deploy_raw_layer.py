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
"Unit tests for RAW layer deployment."

import sys
import unittest
from unittest.mock import mock_open
from unittest.mock import patch

from google.cloud.bigquery import SchemaField

from src.raw.deploy_raw_layer import _create_output_dir_structure
from src.raw.deploy_raw_layer import _parse_args
from src.raw.deploy_raw_layer import create_bq_schema
from src.raw.deploy_raw_layer import create_table
from src.raw.deploy_raw_layer import create_view


class TestDeployRAWLayer(unittest.TestCase):
    """Tests raw.deploy_raw_layer functionality."""

    project = "project_id"
    dataset = "dataset"
    table = "table"

    @patch("pathlib.Path.mkdir")
    def test_create_output_dir_structure_created(self, mkdir_mock):
        # given
        expected_number_of_calls = 4

        # when
        _create_output_dir_structure()

        # then
        self.assertEqual(mkdir_mock.call_count, expected_number_of_calls)

    def test_parse_args_flag_provided(self):
        # given
        pipeline_temp_location = "some_bucket"
        pipeline_staging_location = "other_bucket"
        sys.argv = [
            "test.py", "--pipeline-temp-bucket", pipeline_temp_location,
            "--pipeline-staging-bucket", pipeline_staging_location, "--debug"
        ]
        # when
        exp_pipeline_temp_location, exp_pipeline_staging_location,\
              exp_debug = _parse_args()

        # then
        self.assertTrue(exp_debug)
        self.assertEqual(pipeline_temp_location, exp_pipeline_temp_location)
        self.assertEqual(pipeline_staging_location,
                         exp_pipeline_staging_location)

    # pylint: disable=C0301
    # Test with duplicated field.
    csv_content = (
        "SourceField,ResponseField,TargetField,DataType\n"
        "campaign.resource_name,resource_name,resource_name,STRING\n"
        "campaign.id,id,id,INT64\n"
        "campaign.id,id,id,INT64\n"
        "campaign.hotel_setting.hotel_center_id,hotel_setting,hotel_setting.hotel_center_id,INT64\n"
    )

    @patch("builtins.open", new_callable=mock_open, read_data=csv_content)
    def test_create_bq_schema(self, mocked_open):
        """Correct schema is generated with additional system fields."""
        # given
        mapping_file = "any"
        expected_schema = [
            SchemaField(name="resource_name", field_type="STRING"),
            SchemaField(name="id", field_type="INT64"),
            SchemaField(name="hotel_setting", field_type="STRING"),
            SchemaField(name="recordstamp", field_type="TIMESTAMP")
        ]

        # when
        generated_schema = create_bq_schema(mapping_file=mapping_file)

        # then
        mocked_open.assert_called_with(mapping_file,
                                       encoding="utf-8",
                                       newline="")
        self.assertListEqual(generated_schema, expected_schema)

    @patch("src.py_libs.table_creation_utils.Client")
    def test_create_table_calls_execute_sql_file(self, client_mock):
        """Table was created."""
        # given
        schema = [SchemaField(name="resource_name", field_type="STRING")]

        # when
        create_table(client=client_mock,
                     schema=schema,
                     project=self.project,
                     dataset=self.dataset,
                     table_name=self.table,
                     partition_details=None,
                     cluster_details=None)

        # then
        client_mock.create_table.assert_called()

    @patch("src.raw.view_creation_utils.Client")
    @patch("src.raw.view_creation_utils.table_exists")
    @patch("src.raw.view_creation_utils.generate_schema")
    @patch("src.raw.view_creation_utils.generate_sql")
    @patch("src.raw.view_creation_utils.render_template_file")
    @patch("src.raw.view_creation_utils.execute_sql_file")
    def test_create_view(
        self,
        execute_sql_mock,
        template_mock,
        gen_sql_mock,
        gen_schema_mock,
        table_mock,
        client_mock,
    ):
        # given
        table_mapping_path = "any"
        key_filds = "field1,field2"

        # when
        create_view(client_mock, table_mapping_path, self.table, self.project,
                    self.dataset, key_filds)

        # then
        table_mock.assert_called_once()
        gen_schema_mock.assert_called_once()
        gen_sql_mock.assert_called_once()
        template_mock.assert_called_once()
        execute_sql_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
