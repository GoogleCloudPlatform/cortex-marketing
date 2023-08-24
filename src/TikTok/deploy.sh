#!/bin/bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run by Marketing cloudbuild pipeline to produce artifacts for TikTok submodule.

set -e

export PYTHONPATH=$PYTHONPATH:src/TikTok/:src/

_DEPLOY_CDC=$(jq -r .marketing.TikTok.deployCDC ${_CONFIG_FILE})
_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

if [ ${_DEPLOY_CDC} = "true" ]
then
    echo "Generating TikTok Raw layer files."
    python src/TikTok/src/raw/deploy_raw_layer.py \
        --pipeline-temp-bucket gs://${_TGT_BUCKET}/_pipeline_temp/ \
        --pipeline-staging-bucket gs://${_TGT_BUCKET}/_pipeline_staging/
    echo "✅ Generated TikTok Raw layer files."

    echo "Generating TikTok CDC layer files."
    python src/TikTok/src/cdc/deploy_cdc_layer.py
    echo "✅ Generated TikTok CDC layer files."

    # Copy generated files to Target GCS bucket
    if [ ! -z "$(shopt -s nullglob dotglob; echo src/TikTok/_generated_dags/*)" ]
    then
        echo "Copying TikTok artifacts to gs://${_TGT_BUCKET}/dags/tiktok."
        gsutil -m cp -r src/TikTok/_generated_dags/* gs://${_TGT_BUCKET}/dags/tiktok/
        echo "✅ TikTok artifacts have been copied."
    else
        echo "🔪🔪🔪No files found under _generated_dag folder or the folder does not exist. Skipping copy.🔪🔪🔪"
    fi
else
    echo "== Skipping RAW and CDC layers for TikTok =="
fi

# Deploy reporting layer
src/common/materializer/deploy.sh \
    --gcs_logs_bucket ${_GCS_LOGS_BUCKET} \
    --gcs_tgt_bucket ${_TGT_BUCKET} \
    --module_name TikTok \
    --config_file ${_CONFIG_FILE} \
    --target_type "Reporting" \
    --materializer_settings_file src/TikTok/config/reporting_settings.yaml
