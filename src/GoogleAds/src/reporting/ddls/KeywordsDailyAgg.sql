#-- Copyright 2023 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

/* Keyword metrics with daily granularity. */

SELECT
  Keywords.segments.date AS report_date,
  Criterion.criterion_id,
  Criterion.keyword.text AS keyword,
  Criterion.keyword.match_type,
  Criterion.status AS keyword_status,
  Customers.customer_id,
  Customers.descriptive_name AS customer_name,
  Customers.status AS customer_status,
  Customers.currency_code,
  Customers.time_zone AS customer_time_zone,
  Campaigns.campaign_id,
  Campaigns.name AS campaign_name,
  Campaigns.start_date AS campaign_start_date,
  Campaigns.end_date AS campaign_end_date,
  Adgroups.adgroup_id,
  Adgroups.name AS ad_group,
  Criterion.quality_info.quality_score,
  Keywords.metrics.clicks,
  Keywords.metrics.cost_micros / 1000000 AS cost
FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Keywords` AS Keywords
INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroupCriterion`
  AS Criterion
  ON Criterion.criterion_id = Keywords.ad_group_criterion.criterion_id
    AND Criterion.adgroup_id = Keywords.ad_group.id
    AND Criterion.type_ = 'KEYWORD'
INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Campaigns` AS Campaigns
  ON Campaigns.campaign_id = Keywords.campaign.id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroups` AS Adgroups
  ON Adgroups.adgroup_id = Keywords.ad_group.id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Customers` AS Customers
  ON Customers.customer_id = Keywords.customer.id
