---description:SmokeTest: Data is populated to the AdPerformance table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
)
> 0;

---description:DataTypesCheck1: Schema should match expected schema for AdPerformance table
---INFORMATION_SCHEMA data EXCEPT expected schema
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdPerformance'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('AdPerformance', 'advertiser_id', 'STRING'),
      STRUCT('AdPerformance', 'campaign_id', 'STRING'),
      STRUCT('AdPerformance', 'adgroup_id', 'STRING'),
      STRUCT('AdPerformance', 'ad_id', 'STRING'),
      STRUCT('AdPerformance', 'country_code', 'STRING'),
      STRUCT('AdPerformance', 'stat_time_day', 'DATE'),
      STRUCT('AdPerformance', 'campaign_name', 'STRING'),
      STRUCT('AdPerformance', 'adgroup_name', 'STRING'),
      STRUCT('AdPerformance', 'advertiser_name', 'STRING'),
      STRUCT('AdPerformance', 'ad_name', 'STRING'),
      STRUCT('AdPerformance', 'aeo_type', 'STRING'),
      STRUCT('AdPerformance', 'ad_text', 'STRING'),
      STRUCT('AdPerformance', 'image_mode', 'STRING'),
      STRUCT('AdPerformance', 'gross_impressions', 'INT64'),
      STRUCT('AdPerformance', 'cost_per_1000_reached', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_conversion', 'INT64'),
      STRUCT('AdPerformance', 'real_time_cost_per_conversion', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_conversion_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'result', 'INT64'),
      STRUCT('AdPerformance', 'cost_per_result', 'FLOAT64'),
      STRUCT('AdPerformance', 'result_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_result', 'INT64'),
      STRUCT('AdPerformance', 'real_time_cost_per_result', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_result_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'is_smart_creative', 'BOOL'),
      STRUCT('AdPerformance', 'video_watched_2s', 'INT64'),
      STRUCT('AdPerformance', 'video_watched_6s', 'INT64'),
      STRUCT('AdPerformance', 'average_video_play', 'FLOAT64'),
      STRUCT('AdPerformance', 'average_video_play_per_user', 'FLOAT64'),
      STRUCT('AdPerformance', 'video_views_p25', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p50', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p75', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p100', 'INT64'),
      STRUCT('AdPerformance', 'engaged_view', 'INT64'),
      STRUCT('AdPerformance', 'profile_visits', 'INT64'),
      STRUCT('AdPerformance', 'profile_visits_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'spend', 'FLOAT64'),
      STRUCT('AdPerformance', 'clicks_on_music_disc', 'INT64'),
      STRUCT('AdPerformance', 'impressions', 'INT64'),
      STRUCT('AdPerformance', 'clicks', 'INT64'),
      STRUCT('AdPerformance', 'ctr', 'FLOAT64'),
      STRUCT('AdPerformance', 'cpc', 'FLOAT64'),
      STRUCT('AdPerformance', 'cpm', 'FLOAT64'),
      STRUCT('AdPerformance', 'reach', 'INT64'),
      STRUCT('AdPerformance', 'frequency', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion', 'INT64'),
      STRUCT('AdPerformance', 'video_play_actions', 'INT64'),
      STRUCT('AdPerformance', 'likes', 'INT64'),
      STRUCT('AdPerformance', 'comments', 'INT64'),
      STRUCT('AdPerformance', 'shares', 'INT64'),
      STRUCT('AdPerformance', 'follows', 'INT64'),
      STRUCT('AdPerformance', 'currency', 'STRING'),
      STRUCT('AdPerformance', 'budget', 'FLOAT64'),
      STRUCT('AdPerformance', 'campaign_budget', 'FLOAT64'),
      STRUCT('AdPerformance', 'placement_type', 'STRING'),
      STRUCT('AdPerformance', 'smart_target', 'STRING'),
      STRUCT('AdPerformance', 'cost_per_conversion', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion_rate_v2', 'FLOAT64'),
      STRUCT('AdPerformance', 'timezone', 'STRING'),
      STRUCT('AdPerformance', 'display_timezone', 'STRING')])
  )
)
= 0;

---description:DataTypesCheck2: Schema should match expected schema for AdPerformance table
---Expected schema EXCEPT INFORMATION_SCHEMA data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('AdPerformance', 'advertiser_id', 'STRING'),
      STRUCT('AdPerformance', 'campaign_id', 'STRING'),
      STRUCT('AdPerformance', 'adgroup_id', 'STRING'),
      STRUCT('AdPerformance', 'ad_id', 'STRING'),
      STRUCT('AdPerformance', 'country_code', 'STRING'),
      STRUCT('AdPerformance', 'stat_time_day', 'DATE'),
      STRUCT('AdPerformance', 'campaign_name', 'STRING'),
      STRUCT('AdPerformance', 'adgroup_name', 'STRING'),
      STRUCT('AdPerformance', 'advertiser_name', 'STRING'),
      STRUCT('AdPerformance', 'ad_name', 'STRING'),
      STRUCT('AdPerformance', 'aeo_type', 'STRING'),
      STRUCT('AdPerformance', 'ad_text', 'STRING'),
      STRUCT('AdPerformance', 'image_mode', 'STRING'),
      STRUCT('AdPerformance', 'gross_impressions', 'INT64'),
      STRUCT('AdPerformance', 'cost_per_1000_reached', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_conversion', 'INT64'),
      STRUCT('AdPerformance', 'real_time_cost_per_conversion', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_conversion_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'result', 'INT64'),
      STRUCT('AdPerformance', 'cost_per_result', 'FLOAT64'),
      STRUCT('AdPerformance', 'result_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_result', 'INT64'),
      STRUCT('AdPerformance', 'real_time_cost_per_result', 'FLOAT64'),
      STRUCT('AdPerformance', 'real_time_result_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'is_smart_creative', 'BOOL'),
      STRUCT('AdPerformance', 'video_watched_2s', 'INT64'),
      STRUCT('AdPerformance', 'video_watched_6s', 'INT64'),
      STRUCT('AdPerformance', 'average_video_play', 'FLOAT64'),
      STRUCT('AdPerformance', 'average_video_play_per_user', 'FLOAT64'),
      STRUCT('AdPerformance', 'video_views_p25', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p50', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p75', 'INT64'),
      STRUCT('AdPerformance', 'video_views_p100', 'INT64'),
      STRUCT('AdPerformance', 'engaged_view', 'INT64'),
      STRUCT('AdPerformance', 'profile_visits', 'INT64'),
      STRUCT('AdPerformance', 'profile_visits_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'spend', 'FLOAT64'),
      STRUCT('AdPerformance', 'clicks_on_music_disc', 'INT64'),
      STRUCT('AdPerformance', 'impressions', 'INT64'),
      STRUCT('AdPerformance', 'clicks', 'INT64'),
      STRUCT('AdPerformance', 'ctr', 'FLOAT64'),
      STRUCT('AdPerformance', 'cpc', 'FLOAT64'),
      STRUCT('AdPerformance', 'cpm', 'FLOAT64'),
      STRUCT('AdPerformance', 'reach', 'INT64'),
      STRUCT('AdPerformance', 'frequency', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion', 'INT64'),
      STRUCT('AdPerformance', 'video_play_actions', 'INT64'),
      STRUCT('AdPerformance', 'likes', 'INT64'),
      STRUCT('AdPerformance', 'comments', 'INT64'),
      STRUCT('AdPerformance', 'shares', 'INT64'),
      STRUCT('AdPerformance', 'follows', 'INT64'),
      STRUCT('AdPerformance', 'currency', 'STRING'),
      STRUCT('AdPerformance', 'budget', 'FLOAT64'),
      STRUCT('AdPerformance', 'campaign_budget', 'FLOAT64'),
      STRUCT('AdPerformance', 'placement_type', 'STRING'),
      STRUCT('AdPerformance', 'smart_target', 'STRING'),
      STRUCT('AdPerformance', 'cost_per_conversion', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion_rate', 'FLOAT64'),
      STRUCT('AdPerformance', 'conversion_rate_v2', 'FLOAT64'),
      STRUCT('AdPerformance', 'timezone', 'STRING'),
      STRUCT('AdPerformance', 'display_timezone', 'STRING')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdPerformance'
  )
)
= 0;

---description:TestPKPosition1: PKs in AdPerformance should be in correct order
---INFORMATION_SCHEMA data EXCEPT expected order
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      column_name, ordinal_position
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdPerformance'
      AND column_name
      IN ('advertiser_id', 'campaign_id', 'adgroup_id', 'ad_id', 'country_code', 'stat_time_day')
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('stat_time_day', 1),
      STRUCT('advertiser_id', 2),
      STRUCT('campaign_id', 3),
      STRUCT('adgroup_id', 4),
      STRUCT('ad_id', 5),
      STRUCT('country_code', 6)])
  )
)
= 0;

---description:TestPKPosition2: PKs in AdPerformance should be in correct order
---Expected order EXCEPT INFORMATION_SCHEMA data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('stat_time_day', 1),
      STRUCT('advertiser_id', 2),
      STRUCT('campaign_id', 3),
      STRUCT('adgroup_id', 4),
      STRUCT('ad_id', 5),
      STRUCT('country_code', 6)])
    EXCEPT DISTINCT
    SELECT
      column_name, ordinal_position
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdPerformance'
      AND column_name
      IN ('advertiser_id', 'campaign_id', 'adgroup_id', 'ad_id', 'country_code', 'stat_time_day')
  )
)
= 0;

---description:PKCheckUnique: PK values should be unique in AdPerformance table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      advertiser_id, campaign_id, adgroup_id, ad_id, country_code, stat_time_day, COUNT(*)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
    GROUP BY advertiser_id, campaign_id, adgroup_id, ad_id, country_code, stat_time_day
    HAVING COUNT(*) > 1
    )
)
= 0;

---description:PKCheckNotNULL: PK values should be not NULL in AdPerformance table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
  WHERE 1 != 1
    OR advertiser_id IS NULL
    OR CAST(advertiser_id AS STRING) = ''
    OR campaign_id IS NULL
    OR CAST(campaign_id AS STRING) = ''
    OR adgroup_id IS NULL
    OR CAST(adgroup_id AS STRING) = ''
    OR ad_id IS NULL
    OR CAST(ad_id AS STRING) = ''
    OR country_code IS NULL
    OR CAST(country_code AS STRING) = ''
    OR stat_time_day IS NULL
    OR CAST(stat_time_day AS STRING) = ''

)
= 0;

---description:TestReportingTransformation1:
---AdPerformance should be transformed between CDC and Reporting correctly
---CDC layer data EXCEPT Reporting layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      stat_time_day,
      advertiser_id,
      campaign_id,
      adgroup_id,
      ad_id,
      country_code, -- noqa: L034
      * EXCEPT(
        stat_time_day,
        advertiser_id,
        campaign_id,
        adgroup_id,
        ad_id,
        country_code,
        recordstamp)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_cdc }}.auction_ad_performance`
    EXCEPT DISTINCT
    SELECT *
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
  )
)
= 0;

---description:TestReportingTransformation2:
---AdPerformance should be transformed between CDC and Reporting correctly
---Reporting layer data EXCEPT CDC layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT *
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
    EXCEPT DISTINCT
    SELECT
      stat_time_day,
      advertiser_id,
      campaign_id,
      adgroup_id,
      ad_id,
      country_code, -- noqa: L034
      * EXCEPT(
        stat_time_day,
        advertiser_id,
        campaign_id,
        adgroup_id,
        ad_id,
        country_code,
        recordstamp)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_cdc }}.auction_ad_performance`
  )
)
= 0;
