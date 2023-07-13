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
"""This DAG starts a Beam pipeline for processing Ads CDC layer ingestion."""

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

_CDC_SQL_PATH = "${cdc_sql_path}"
_DAG_SCHEDULE = "${load_frequency}"
_PROJECT_ID = "${project_id}"
_DATASET_ID = "${cdc_dataset}"
_TABLE_NAME = "${table_name}"

_START_DATETIME = datetime.fromisoformat("${start_datetime}")
_IDENTIFIER = f"googleads_{_PROJECT_ID}_{_DATASET_ID}_raw_to_cdc_{_TABLE_NAME}"

with DAG(dag_id=_IDENTIFIER,
         description=f"Extract {_TABLE_NAME} from raw to cdc",
         schedule=_DAG_SCHEDULE,
         start_date=_START_DATETIME,
         dagrun_timeout=timedelta(minutes=60),
         tags=["GoogleAds", "cdc"],
         catchup=False,
         max_active_runs=1) as dag:

    # Import here to avoid slow DAG imports in Airflow.
    if AIRFLOW_VERSION.startswith("1."):
        from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
    else:
        from airflow.operators.empty import EmptyOperator

    start_task = EmptyOperator(task_id="start",
                               depends_on_past=True,
                               wait_for_downstream=True)

    if AIRFLOW_VERSION.startswith("1."):
        copy_raw_to_cdc = BigQueryOperator(task_id=_IDENTIFIER,
                                           sql=_CDC_SQL_PATH,
                                           bigquery_conn_id="googleads_cdc_bq",
                                           use_legacy_sql=False,
                                           retries=0)
    else:
        copy_raw_to_cdc = BigQueryOperator(task_id=_IDENTIFIER,
                                           sql=_CDC_SQL_PATH,
                                           gcp_conn_id="googleads_cdc_bq",
                                           use_legacy_sql=False,
                                           retries=0)

    stop_task = EmptyOperator(task_id="stop")

start_task >> copy_raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
