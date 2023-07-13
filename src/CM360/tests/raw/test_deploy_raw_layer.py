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

import unittest
from unittest.mock import mock_open
from unittest.mock import patch

from google.cloud.bigquery import SchemaField

from src.raw.deploy_raw_layer import _create_output_dir_structure
from src.raw.deploy_raw_layer import _parse_args
from src.raw.deploy_raw_layer import create_bq_schema


class TestDeployRAWLayer(unittest.TestCase):
    """Tests raw.deploy_raw_layer functionality."""

    @patch("pathlib.Path.mkdir")
    def test_create_output_dir_structure_created(self, mkdir_mock):
        # given
        expected_number_of_calls = 3

        # when
        _create_output_dir_structure()

        # then
        self.assertEqual(mkdir_mock.call_count, expected_number_of_calls)

    def test_parse_args_flag_provided(self):
        # given
        input_ = [
            "--debug", "--pipeline-temp-bucket", "wdd",
            "--pipeline-staging-bucket", "sdfsd"
        ]
        # when
        args = _parse_args(input_)

        # then
        self.assertTrue(args.debug)

    def test_parse_args_flag_not_provided(self):
        # given
        input_ = [
            "--pipeline-temp-bucket", "wdd", "--pipeline-staging-bucket",
            "sdfsd"
        ]

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


if __name__ == "__main__":
    unittest.main()
