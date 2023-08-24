---description:SmokeTest: Data is populated to the AdGroupPerformance table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
)
> 0;

---description:DataTypesCheck1: Schema should match expected schema for AdGroupPerformance table
---INFORMATION_SCHEMA data EXCEPT expected schema
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformance'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('AdGroupPerformance', 'advertiser_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'campaign_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'adgroup_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'country_code', 'STRING'),
      STRUCT('AdGroupPerformance', 'stat_time_day', 'DATE'),
      STRUCT('AdGroupPerformance', 'campaign_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'adgroup_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'advertiser_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'reach', 'INT64'),
      STRUCT('AdGroupPerformance', 'frequency', 'FLOAT64'),
      STRUCT('AdGroupPerformance', 'timezone', 'STRING'),
      STRUCT('AdGroupPerformance', 'display_timezone', 'STRING')])
  )
)
= 0;

---description:DataTypesCheck2: Schema should match expected schema for AdGroupPerformance table
---Expected schema EXCEPT INFORMATION_SCHEMA data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('AdGroupPerformance', 'advertiser_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'campaign_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'adgroup_id', 'STRING'),
      STRUCT('AdGroupPerformance', 'country_code', 'STRING'),
      STRUCT('AdGroupPerformance', 'stat_time_day', 'DATE'),
      STRUCT('AdGroupPerformance', 'campaign_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'adgroup_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'advertiser_name', 'STRING'),
      STRUCT('AdGroupPerformance', 'reach', 'INT64'),
      STRUCT('AdGroupPerformance', 'frequency', 'FLOAT64'),
      STRUCT('AdGroupPerformance', 'timezone', 'STRING'),
      STRUCT('AdGroupPerformance', 'display_timezone', 'STRING')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformance'
  )
)
= 0;

---description:TestPKPosition1: PKs in AdGroupPerformance should be in correct order
---INFORMATION_SCHEMA data EXCEPT expected order
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      column_name, ordinal_position
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformance'
      AND column_name
      IN ('advertiser_id', 'campaign_id', 'adgroup_id', 'country_code', 'stat_time_day')
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('stat_time_day', 1),
      STRUCT('advertiser_id', 2),
      STRUCT('campaign_id', 3),
      STRUCT('adgroup_id', 4),
      STRUCT('country_code', 5)])
  )
)
= 0;

---description:TestPKPosition2: PKs in AdGroupPerformance should be in correct order
---Expected order EXCEPT INFORMATION_SCHEMA data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('stat_time_day', 1),
      STRUCT('advertiser_id', 2),
      STRUCT('campaign_id', 3),
      STRUCT('adgroup_id', 4),
      STRUCT('country_code', 5)])
    EXCEPT DISTINCT
    SELECT
      column_name, ordinal_position
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformance'
      AND column_name
      IN ('advertiser_id', 'campaign_id', 'adgroup_id', 'country_code', 'stat_time_day')
  )
)
= 0;

---description:PKCheckUnique: PK values should be unique in AdGroupPerformance table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      advertiser_id, campaign_id, adgroup_id, country_code, stat_time_day, COUNT(*)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
    GROUP BY advertiser_id, campaign_id, adgroup_id, country_code, stat_time_day
    HAVING COUNT(*) > 1
    )
)
= 0;

---description:PKCheckNotNULL: PK values should be not NULL in AdGroupPerformance table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
  WHERE 1 != 1
    OR advertiser_id IS NULL
    OR CAST(advertiser_id AS STRING) = ''
    OR campaign_id IS NULL
    OR CAST(campaign_id AS STRING) = ''
    OR adgroup_id IS NULL
    OR CAST(adgroup_id AS STRING) = ''
    OR country_code IS NULL
    OR CAST(country_code AS STRING) = ''
    OR stat_time_day IS NULL
    OR CAST(stat_time_day AS STRING) = ''

)
= 0;

---description:TestReportingTransformation1:
---AdGroupPerformance should be transformed between CDC and Reporting correctly
---CDC layer data EXCEPT Reporting layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      stat_time_day,
      advertiser_id,
      campaign_id,
      adgroup_id,
      country_code, -- noqa: L034
      * EXCEPT(stat_time_day, advertiser_id, campaign_id, adgroup_id, country_code, recordstamp)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_cdc }}.auction_adgroup_performance`
    EXCEPT DISTINCT
    SELECT *
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
  )
)
= 0;

---description:TestReportingTransformation2:
---AdGroupPerformance should be transformed between CDC and Reporting correctly
---Reporting layer data EXCEPT CDC layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT *
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
    EXCEPT DISTINCT
    SELECT
      stat_time_day,
      advertiser_id,
      campaign_id,
      adgroup_id,
      country_code, -- noqa: L034
      * EXCEPT(stat_time_day, advertiser_id, campaign_id, adgroup_id, country_code, recordstamp)
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_cdc }}.auction_adgroup_performance`
  )
)
= 0;
