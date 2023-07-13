---description:SmokeTest: Data is populated to the ImpressionsDailyAgg table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
> 0;

---description:DataTypesCheck: Schema should match expected schema for ImpressionsDailyAgg table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ImpressionsDailyAgg'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('ImpressionsDailyAgg', 'date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'account_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'state_region', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'device', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'site_name', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'url', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'country_code', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'browser_timezone_offset_minutes', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_impressions', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_billable_impressions', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ImpressionsDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64')])
  )
)
= 0;

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('ImpressionsDailyAgg', 'date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'account_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ImpressionsDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'state_region', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'device', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'site_name', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'url', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'country_code', 'STRING'),
      STRUCT('ImpressionsDailyAgg', 'browser_timezone_offset_minutes', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_impressions', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_billable_impressions', 'INT64'),
      STRUCT('ImpressionsDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ImpressionsDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ImpressionsDailyAgg'
  )
)
= 0;

---description:RowCountCheck: Row count in the ImpressionsDailyAgg should be as expected
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 49358.0;

---description:MinValueProfileCheck: date column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(date) IS NULL THEN '999999999999'
      ELSE CAST(MIN(date) AS STRING)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE date IS NOT NULL
)
>= '2023-05-04';

---description:ValueFormatCheck: Column account_id has expected pattern
ASSERT (
  SELECT
    COUNT(account_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(account_id AS STRING),
    r'7480|9386')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in account_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE account_id IS NULL
)
= 0;

---description:NullCountCheck: NULL count should be as expected in advertiser_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_id IS NULL
)
= 3545;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_id column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 6;

---description:MinValueProfileCheck: advertiser_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(advertiser_id) IS NULL THEN 999999999999
      ELSE MIN(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
>= 3927831;

---description:MaxValueProfileCheck: advertiser_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(advertiser_id) IS NULL THEN -999999999999
      ELSE MAX(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
<= 1279097001;

---description:AvgValueProfileCheck: advertiser_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(advertiser_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
= 755153102.690786;

---description:MinValueProfileCheck: advertiser_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(advertiser_id) IS NULL THEN 999999999999
      ELSE MIN(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in advertiser_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE advertiser_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_name column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 36;

---description:NullCountCheck: NULL count should be as expected in campaign_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_id IS NULL
)
= 4291;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_id column
ASSERT (
  SELECT COUNT(DISTINCT campaign_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 5;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 52433136;

---description:MaxValueProfileCheck: campaign_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(campaign_id) IS NULL THEN -999999999999
      ELSE MAX(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
<= 53419006;

---description:AvgValueProfileCheck: campaign_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(campaign_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
= 53391432.586837;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in campaign_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_name column
ASSERT (
  SELECT COUNT(DISTINCT campaign_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 35;

---description:NullCountCheck: NULL count should be as expected in campaign_start_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_start_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_start_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_start_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 37;

---description:NullCountCheck: NULL count should be as expected in campaign_end_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE campaign_end_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_end_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_end_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 24;

---description:NullCountCheck: NULL count should be as expected in ad_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_id column
ASSERT (
  SELECT COUNT(DISTINCT ad_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 63;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 284281829;

---description:MaxValueProfileCheck: ad_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(ad_id) IS NULL THEN -999999999999
      ELSE MAX(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_id IS NOT NULL
)
<= 558418510;

---description:AvgValueProfileCheck: ad_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(ad_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_id IS NOT NULL
)
= 547069970.577495;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in ad_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_name column
ASSERT (
  SELECT COUNT(DISTINCT ad_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 60;

---description:NullCountCheck: NULL count should be as expected in ad_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE ad_type IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_type column
ASSERT (
  SELECT COUNT(DISTINCT ad_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 6;

---description:NullCountCheck: NULL count should be as expected in state_region column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE state_region IS NULL
)
= 46056;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region column
ASSERT (
  SELECT COUNT(DISTINCT state_region)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 63;

---description:NullCountCheck: NULL count should be as expected in state_region_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE state_region_name IS NULL
)
= 49031;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region_name column
ASSERT (
  SELECT COUNT(DISTINCT state_region_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 50;

---description:NullCountCheck: NULL count should be as expected in insertion_order column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE insertion_order IS NULL
)
= 3545;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in insertion_order column
ASSERT (
  SELECT COUNT(DISTINCT insertion_order)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 6;

---description:MinValueProfileCheck: insertion_order column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(insertion_order) IS NULL THEN 999999999999
      ELSE MIN(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE insertion_order IS NOT NULL
)
>= 1011083458;

---description:MaxValueProfileCheck: insertion_order column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(insertion_order) IS NULL THEN -999999999999
      ELSE MAX(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE insertion_order IS NOT NULL
)
<= 1012852681;

---description:AvgValueProfileCheck: insertion_order column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(insertion_order AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE insertion_order IS NOT NULL
)
= 1012304910.930129;

---description:NullCountCheck: NULL count should be as expected in line_item_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE line_item_id IS NULL
)
= 3545;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in line_item_id column
ASSERT (
  SELECT COUNT(DISTINCT line_item_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 9;

---description:MinValueProfileCheck: line_item_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(line_item_id) IS NULL THEN 999999999999
      ELSE MIN(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE line_item_id IS NOT NULL
)
>= 19810579330;

---description:MaxValueProfileCheck: line_item_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(line_item_id) IS NULL THEN -999999999999
      ELSE MAX(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE line_item_id IS NOT NULL
)
<= 20185372519;

---description:AvgValueProfileCheck: line_item_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(line_item_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE line_item_id IS NOT NULL
)
= 20146233325.877982;

---description:ENUMCheck:
---Values in device column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE CAST(device AS STRING) NOT IN ('0', '1', '2', '3', '4', '5', '29', '78', '27', '30', '28',
                                       '31', '76', '77', '26', '25')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in device column
ASSERT (
  SELECT COUNT(DISTINCT device)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 14;

---description:MinValueProfileCheck: device column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(device) IS NULL THEN 999999999999
      ELSE MIN(device)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE device IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: device column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(device) IS NULL THEN -999999999999
      ELSE MAX(device)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE device IS NOT NULL
)
<= 78;

---description:AvgValueProfileCheck: device column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(device AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE device IS NOT NULL
)
= 10.670955;

---description:NullCountCheck: NULL count should be as expected in site_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE site_name IS NULL
)
= 3545;

---description:DistinctCountProfileCheck: Distinct count should be as expected in site_name column
ASSERT (
  SELECT COUNT(DISTINCT site_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 7946;

---description:ValueFormatCheck: Column url has expected pattern
ASSERT (
  SELECT
    COUNT(url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(url AS STRING),
    r'http[s]?:\/\/[\S]+.[\S]+|source_url_hidden')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in url column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE url IS NULL
)
= 3545;

---description:DistinctCountProfileCheck: Distinct count should be as expected in url column
ASSERT (
  SELECT COUNT(DISTINCT url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 37275;

---description:NullCountCheck: NULL count should be as expected in audience_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE audience_type IS NULL
)
= 49358;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in audience_type column
ASSERT (
  SELECT COUNT(DISTINCT audience_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 0;

---description:NullCountCheck: NULL count should be as expected in browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_platform_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 12;

---description:MinValueProfileCheck: browser_platform_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(browser_platform_id) IS NULL THEN 999999999999
      ELSE MIN(browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: browser_platform_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(browser_platform_id) IS NULL THEN -999999999999
      ELSE MAX(browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
<= 78;

---description:AvgValueProfileCheck: browser_platform_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(browser_platform_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
= 29.919446;

---description:NullCountCheck: NULL count should be as expected in browser_platform column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_platform IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 12;

---description:NullCountCheck: NULL count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE dv360_browser_platform_id IS NULL
)
= 3545;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT dv360_browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 16;

---description:MinValueProfileCheck: dv360_browser_platform_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(dv360_browser_platform_id) IS NULL THEN 999999999999
      ELSE MIN(dv360_browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: dv360_browser_platform_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(dv360_browser_platform_id) IS NULL THEN -999999999999
      ELSE MAX(dv360_browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
<= 1038;

---description:AvgValueProfileCheck: dv360_browser_platform_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(dv360_browser_platform_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
= 19.377775;

---description:ENUMCheck:
---Values in country_code column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE CAST(country_code AS STRING) NOT IN ('UK', 'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO',
                                             'AQ', 'AR', 'AS', 'AT', 'AU', 'AW', 'AX', 'AZ', 'BA',
                                             'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BL',
                                             'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW',
                                             'BY', 'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI',
                                             'CK', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CW',
                                             'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ',
                                             'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ',
                                             'FK', 'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF',
                                             'GG', 'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ', 'GR',
                                             'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM', 'HN', 'HR',
                                             'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ',
                                             'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG',
                                             'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ',
                                             'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU',
                                             'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH',
                                             'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS',
                                             'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NC',
                                             'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU',
                                             'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL',
                                             'PM', 'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE',
                                             'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE',
                                             'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO',
                                             'SR', 'SS', 'ST', 'SV', 'SX', 'SY', 'SZ', 'TC', 'TD',
                                             'TF', 'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO',
                                             'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US',
                                             'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU',
                                             'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 'ZW')
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in country_code column
ASSERT (
  SELECT COUNT(DISTINCT country_code)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 16;

---description:NullCountCheck:
---NULL count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_timezone_offset_minutes IS NULL
)
= 3545;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT COUNT(DISTINCT browser_timezone_offset_minutes)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 13;

---description:MinValueProfileCheck: browser_timezone_offset_minutes column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(browser_timezone_offset_minutes) IS NULL THEN 999999999999
      ELSE MIN(browser_timezone_offset_minutes)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
>= 120;

---description:MaxValueProfileCheck: browser_timezone_offset_minutes column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(browser_timezone_offset_minutes) IS NULL THEN -999999999999
      ELSE MAX(browser_timezone_offset_minutes)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
<= 1050;

---description:AvgValueProfileCheck: browser_timezone_offset_minutes column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(browser_timezone_offset_minutes AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
= 918.662825;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_impressions column
ASSERT (
  SELECT COUNT(DISTINCT total_impressions)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 146;

---description:MinValueProfileCheck: total_impressions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_impressions) IS NULL THEN 999999999999
      ELSE MIN(total_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_impressions IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_impressions column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_impressions) IS NULL THEN -999999999999
      ELSE MAX(total_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_impressions IS NOT NULL
)
<= 4339;

---description:AvgValueProfileCheck: total_impressions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_impressions AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_impressions IS NOT NULL
)
= 1.865655;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_billable_impressions column
ASSERT (
  SELECT COUNT(DISTINCT total_billable_impressions)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 202;

---description:MinValueProfileCheck: total_billable_impressions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_billable_impressions) IS NULL THEN 999999999999
      ELSE MIN(total_billable_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_billable_impressions IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_billable_impressions column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_billable_impressions) IS NULL THEN -999999999999
      ELSE MAX(total_billable_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_billable_impressions IS NOT NULL
)
<= 9399;

---description:AvgValueProfileCheck: total_billable_impressions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_billable_impressions AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_billable_impressions IS NOT NULL
)
= 4.349487;

---description:NullCountCheck: NULL count should be as expected in total_billable_impressions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_billable_impressions IS NULL
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_revenue_advertiser_currency IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_revenue_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 3224;

---description:MinValueProfileCheck: total_revenue_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_revenue_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_revenue_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_revenue_advertiser_currency column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_revenue_advertiser_currency) IS NULL THEN -999999999999
      ELSE MAX(total_revenue_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
<= 0.740503034;

---description:AvgValueProfileCheck:
---total_revenue_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_revenue_advertiser_currency AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
= 0.000534;

---description:NullCountCheck:
---NULL count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_media_cost_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
)
= 2221;

---description:MinValueProfileCheck: total_media_cost_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_media_cost_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_media_cost_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_media_cost_advertiser_currency column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_media_cost_advertiser_currency) IS NULL THEN -999999999999
      ELSE MAX(total_media_cost_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
<= 0.740503034;

---description:AvgValueProfileCheck:
---total_media_cost_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_media_cost_advertiser_currency AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
= 0.000533;

---description:CDCCheck1:
---Data in the ImpressionsDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedImpressions AS (
      SELECT
        Impressions.account_id,
        Impressions.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Impressions.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Impressions.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Impressions.event_time)) AS date,
        Impressions.dv360_insertion_order_id AS insertion_order,
        Impressions.dv360_line_item_id AS line_item_id,
        COALESCE(Impressions.dv360_device_type, Impressions.browser_platform_id) AS device,
        Impressions.dv360_browser_platform_id,
        Impressions.browser_platform_id,
        Browsers.browser_platform,
        Impressions.dv360_site_id AS site_name,
        Impressions.dv360_country_code AS country_code,
        Impressions.state_region,
        States.state_region_full_name AS state_region_name,
        Impressions.dv360_url AS url,
        Impressions.dv360_matching_targeted_segments AS audience_type,
        Impressions.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        Impressions.active_view_viewable_impressions,
        Impressions.active_view_eligible_impressions,
        Impressions.dv360_revenue_advertiser_currency,
        Impressions.dv360_total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Impressions` AS Impressions
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Impressions.advertiser_id
          AND Advertisers.account_id = Impressions.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Impressions.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Impressions.ad_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Impressions.browser_platform_id
          AND Browsers.account_id = Impressions.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Impressions.dv360_country_code, '-', Impressions.state_region)
          AND States.account_id = Impressions.account_id
    )
    SELECT
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      SUM(active_view_viewable_impressions) AS total_impressions,
      SUM(active_view_eligible_impressions) AS total_billable_impressions,
      SUM(dv360_revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
      SUM(dv360_total_media_cost_advertiser_currency) / 1000000000
        AS total_media_cost_advertiser_currency
    FROM AggregatedImpressions
    GROUP BY
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes
    EXCEPT DISTINCT
    SELECT
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      total_impressions,
      total_billable_impressions,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
  )
)
= 0;

---description:CDCCheck2:
---Data in the ImpressionsDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedImpressions AS (
      SELECT
        Impressions.account_id,
        Impressions.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Impressions.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Impressions.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Impressions.event_time)) AS date,
        Impressions.dv360_insertion_order_id AS insertion_order,
        Impressions.dv360_line_item_id AS line_item_id,
        COALESCE(Impressions.dv360_device_type, Impressions.browser_platform_id) AS device,
        Impressions.dv360_browser_platform_id,
        Impressions.browser_platform_id,
        Browsers.browser_platform,
        Impressions.dv360_site_id AS site_name,
        Impressions.dv360_country_code AS country_code,
        Impressions.state_region,
        States.state_region_full_name AS state_region_name,
        Impressions.dv360_url AS url,
        Impressions.dv360_matching_targeted_segments AS audience_type,
        Impressions.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        Impressions.active_view_viewable_impressions,
        Impressions.active_view_eligible_impressions,
        Impressions.dv360_revenue_advertiser_currency,
        Impressions.dv360_total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Impressions` AS Impressions
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Impressions.advertiser_id
          AND Advertisers.account_id = Impressions.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Impressions.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Impressions.ad_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Impressions.browser_platform_id
          AND Browsers.account_id = Impressions.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Impressions.dv360_country_code, '-', Impressions.state_region)
          AND States.account_id = Impressions.account_id
    )
    SELECT
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      total_impressions,
      total_billable_impressions,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ImpressionsDailyAgg`
    EXCEPT DISTINCT
    SELECT
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      SUM(active_view_viewable_impressions) AS total_impressions,
      SUM(active_view_eligible_impressions) AS total_billable_impressions,
      SUM(dv360_revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
      SUM(dv360_total_media_cost_advertiser_currency) / 1000000000
        AS total_media_cost_advertiser_currency
    FROM AggregatedImpressions
    GROUP BY
      account_id,
      advertiser_id,
      advertiser_name,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      ad_id,
      ad_name,
      ad_type,
      date,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      browser_platform_id,
      browser_platform,
      site_name,
      country_code,
      state_region,
      state_region_name,
      url,
      audience_type,
      browser_timezone_offset_minutes
  )
)
= 0;
