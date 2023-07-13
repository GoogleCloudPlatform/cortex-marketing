---description:SmokeTest: Data is populated to the ClicksDailyAgg table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
> 0;

---description:DataTypesCheck: Schema should match expected schema for ClicksDailyAgg table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ClicksDailyAgg'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('ClicksDailyAgg', 'account_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ClicksDailyAgg', 'date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ClicksDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'device', 'INT64'),
      STRUCT('ClicksDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ClicksDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'site_name', 'INT64'),
      STRUCT('ClicksDailyAgg', 'country_code', 'STRING'),
      STRUCT('ClicksDailyAgg', 'state_region', 'STRING'),
      STRUCT('ClicksDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'url', 'STRING'),
      STRUCT('ClicksDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ClicksDailyAgg', 'total_clicks', 'INT64'),
      STRUCT('ClicksDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ClicksDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64'),
      STRUCT('ClicksDailyAgg', 'browser_timezone_offset_minutes', 'INT64')])
  )
)
= 0;

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('ClicksDailyAgg', 'account_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ClicksDailyAgg', 'date', 'DATE'),
      STRUCT('ClicksDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ClicksDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'device', 'INT64'),
      STRUCT('ClicksDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ClicksDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ClicksDailyAgg', 'site_name', 'INT64'),
      STRUCT('ClicksDailyAgg', 'country_code', 'STRING'),
      STRUCT('ClicksDailyAgg', 'state_region', 'STRING'),
      STRUCT('ClicksDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ClicksDailyAgg', 'url', 'STRING'),
      STRUCT('ClicksDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ClicksDailyAgg', 'total_clicks', 'INT64'),
      STRUCT('ClicksDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ClicksDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64'),
      STRUCT('ClicksDailyAgg', 'browser_timezone_offset_minutes', 'INT64')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ClicksDailyAgg'
  )
)
= 0;

---description:RowCountCheck: Row count in the ClicksDailyAgg should be as expected
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 79.0;

---description:ValueFormatCheck: Column account_id has expected pattern
ASSERT (
  SELECT
    COUNT(account_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(account_id AS STRING),
    r'7480|9386')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in account_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE account_id IS NULL
)
= 0;

---description:NullCountCheck: NULL count should be as expected in advertiser_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE advertiser_id IS NULL
)
= 76;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_id column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 2;

---description:MinValueProfileCheck: advertiser_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(advertiser_id) IS NULL THEN 999999999999
      ELSE MIN(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
<= 1045195587;

---description:AvgValueProfileCheck: advertiser_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(advertiser_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
= 351017083;

---description:MinValueProfileCheck: advertiser_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(advertiser_id) IS NULL THEN 999999999999
      ELSE MIN(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in advertiser_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE advertiser_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_name column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 58;

---description:NullCountCheck: NULL count should be as expected in campaign_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_id IS NULL
)
= 76;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_id column
ASSERT (
  SELECT COUNT(DISTINCT campaign_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 2;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 53381346;

---description:MaxValueProfileCheck: campaign_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(campaign_id) IS NULL THEN -999999999999
      ELSE MAX(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_id IS NOT NULL
)
<= 53419006;

---description:AvgValueProfileCheck: campaign_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(campaign_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_id IS NOT NULL
)
= 53406452.666667;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in campaign_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_name column
ASSERT (
  SELECT COUNT(DISTINCT campaign_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 32;

---description:NullCountCheck: NULL count should be as expected in campaign_start_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_start_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_start_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_start_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 58;

---description:NullCountCheck: NULL count should be as expected in campaign_end_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE campaign_end_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_end_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_end_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 50;

---description:NullCountCheck: NULL count should be as expected in ad_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_id column
ASSERT (
  SELECT COUNT(DISTINCT ad_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 79;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 282169739;

---description:MaxValueProfileCheck: ad_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(ad_id) IS NULL THEN -999999999999
      ELSE MAX(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_id IS NOT NULL
)
<= 558581982;

---description:AvgValueProfileCheck: ad_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(ad_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_id IS NOT NULL
)
= 468774071.367089;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in ad_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_name column
ASSERT (
  SELECT COUNT(DISTINCT ad_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 77;

---description:NullCountCheck: NULL count should be as expected in ad_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE ad_type IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_type column
ASSERT (
  SELECT COUNT(DISTINCT ad_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 8;

---description:MinValueProfileCheck: date column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(date) IS NULL THEN '999999999999'
      ELSE CAST(MIN(date) AS STRING)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE date IS NOT NULL
)
>= '2023-04-26';

---description:NullCountCheck: NULL count should be as expected in insertion_order column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE insertion_order IS NULL
)
= 76;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in insertion_order column
ASSERT (
  SELECT COUNT(DISTINCT insertion_order)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 2;

---description:MinValueProfileCheck: insertion_order column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(insertion_order) IS NULL THEN 999999999999
      ELSE MIN(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE insertion_order IS NOT NULL
)
>= 1012225374;

---description:MaxValueProfileCheck: insertion_order column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(insertion_order) IS NULL THEN -999999999999
      ELSE MAX(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE insertion_order IS NOT NULL
)
<= 1012494195;

---description:AvgValueProfileCheck: insertion_order column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(insertion_order AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE insertion_order IS NOT NULL
)
= 1012404588;

---description:MinValueProfileCheck: insertion_order column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(insertion_order) IS NULL THEN 999999999999
      ELSE MIN(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE insertion_order IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in line_item_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE line_item_id IS NULL
)
= 76;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in line_item_id column
ASSERT (
  SELECT COUNT(DISTINCT line_item_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 3;

---description:MinValueProfileCheck: line_item_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(line_item_id) IS NULL THEN 999999999999
      ELSE MIN(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE line_item_id IS NOT NULL
)
>= 20108429655;

---description:MaxValueProfileCheck: line_item_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(line_item_id) IS NULL THEN -999999999999
      ELSE MAX(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE line_item_id IS NOT NULL
)
<= 20170098397;

---description:AvgValueProfileCheck: line_item_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(line_item_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE line_item_id IS NOT NULL
)
= 20148433614;

---description:MinValueProfileCheck: line_item_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(line_item_id) IS NULL THEN 999999999999
      ELSE MIN(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE line_item_id IS NOT NULL
)
>= 0.0;

---description:ENUMCheck:
---Values in device column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE CAST(device AS STRING) NOT IN ('0', '1', '2', '3', '4', '5', '25', '27', '29', '26', '28',
                                       '77', '30')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in device column
ASSERT (
  SELECT COUNT(DISTINCT device)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 9;

---description:MinValueProfileCheck: device column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(device) IS NULL THEN 999999999999
      ELSE MIN(device)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE device IS NOT NULL
)
<= 77;

---description:AvgValueProfileCheck: device column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(device AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE device IS NOT NULL
)
= 27;

---description:NullCountCheck: NULL count should be as expected in browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_platform_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 8;

---description:MinValueProfileCheck: browser_platform_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(browser_platform_id) IS NULL THEN 999999999999
      ELSE MIN(browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
<= 77;

---description:AvgValueProfileCheck: browser_platform_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(browser_platform_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
= 27.329114;

---description:NullCountCheck: NULL count should be as expected in browser_platform column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_platform IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 8;

---description:NullCountCheck: NULL count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE dv360_browser_platform_id IS NULL
)
= 76;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT dv360_browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 1;

---description:MinValueProfileCheck: dv360_browser_platform_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(dv360_browser_platform_id) IS NULL THEN 999999999999
      ELSE MIN(dv360_browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
>= 3;

---description:MaxValueProfileCheck: dv360_browser_platform_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(dv360_browser_platform_id) IS NULL THEN -999999999999
      ELSE MAX(dv360_browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
<= 3;

---description:AvgValueProfileCheck: dv360_browser_platform_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(dv360_browser_platform_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE dv360_browser_platform_id IS NOT NULL
)
= 3;

---description:NullCountCheck: NULL count should be as expected in site_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE site_name IS NULL
)
= 76;

---description:DistinctCountProfileCheck: Distinct count should be as expected in site_name column
ASSERT (
  SELECT COUNT(DISTINCT site_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 3;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in country_code column
ASSERT (
  SELECT COUNT(DISTINCT country_code)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 2;

---description:ENUMCheck:
---Values in country_code column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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

---description:NullCountCheck: NULL count should be as expected in state_region column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE state_region IS NULL
)
= 65;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region column
ASSERT (
  SELECT COUNT(DISTINCT state_region)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 7;

---description:NullCountCheck: NULL count should be as expected in state_region_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE state_region_name IS NULL
)
= 79;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region_name column
ASSERT (
  SELECT COUNT(DISTINCT state_region_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 0;

---description:NullCountCheck: NULL count should be as expected in url column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE url IS NULL
)
= 76;

---description:DistinctCountProfileCheck: Distinct count should be as expected in url column
ASSERT (
  SELECT COUNT(DISTINCT url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 3;

---description:ValueFormatCheck: Column url has expected pattern
ASSERT (
  SELECT
    COUNT(url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(url AS STRING),
    r'http[s]?:\/\/[\S]+.[\S]+|source_url_hidden')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in audience_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE audience_type IS NULL
)
= 79;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in audience_type column
ASSERT (
  SELECT COUNT(DISTINCT audience_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 0;

---description:NullCountCheck: NULL count should be as expected in total_clicks column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_clicks IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_clicks column
ASSERT (
  SELECT COUNT(DISTINCT total_clicks)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_clicks column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_clicks) IS NULL THEN 999999999999
      ELSE MIN(total_clicks)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_clicks IS NOT NULL
)
>= 1;

---description:MaxValueProfileCheck: total_clicks column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_clicks) IS NULL THEN -999999999999
      ELSE MAX(total_clicks)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_clicks IS NOT NULL
)
<= 1;

---description:AvgValueProfileCheck: total_clicks column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_clicks AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_clicks IS NOT NULL
)
= 1;

---description:NullCountCheck:
---NULL count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_revenue_advertiser_currency IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_revenue_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_revenue_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_revenue_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_revenue_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck:
---total_revenue_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_revenue_advertiser_currency AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_media_cost_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_media_cost_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_media_cost_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_media_cost_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck:
---total_media_cost_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_media_cost_advertiser_currency AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_timezone_offset_minutes IS NULL
)
= 76;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT COUNT(DISTINCT browser_timezone_offset_minutes)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
)
= 2;

---description:MinValueProfileCheck: browser_timezone_offset_minutes column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(browser_timezone_offset_minutes) IS NULL THEN 999999999999
      ELSE MIN(browser_timezone_offset_minutes)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
>= 840;

---description:MaxValueProfileCheck: browser_timezone_offset_minutes column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(browser_timezone_offset_minutes) IS NULL THEN -999999999999
      ELSE MAX(browser_timezone_offset_minutes)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
<= 1050;

---description:AvgValueProfileCheck: browser_timezone_offset_minutes column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(browser_timezone_offset_minutes AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  WHERE browser_timezone_offset_minutes IS NOT NULL
)
= 910;

---description:CDCCheck1:
---Data in the ClicksDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedClicks AS (
      SELECT
        Clicks.account_id,
        Clicks.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Clicks.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Clicks.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Clicks.event_time)) AS date,
        Clicks.dv360_insertion_order_id AS insertion_order,
        Clicks.dv360_line_item_id AS line_item_id,
        COALESCE(Clicks.dv360_device_type, Clicks.browser_platform_id) AS device,
        Clicks.dv360_browser_platform_id,
        Clicks.browser_platform_id,
        Browsers.browser_platform,
        Clicks.dv360_site_id AS site_name,
        Clicks.dv360_country_code AS country_code,
        Clicks.state_region,
        States.state_region_full_name AS state_region_name,
        Clicks.dv360_url AS url,
        Clicks.dv360_matching_targeted_segments AS audience_type,
        Clicks.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        IF(Clicks.event_type = 'CLICK', 1, 0) AS total_clicks,
        Clicks.dv360_revenue_advertiser_currency / 1000000000 AS total_revenue_advertiser_currency,
        Clicks.dv360_total_media_cost_advertiser_currency / 1000000000
          AS total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Clicks` AS Clicks
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Clicks.advertiser_id
          AND Advertisers.account_id = Clicks.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Clicks.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Clicks.ad_id AND Ads.advertiser_id = Clicks.advertiser_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Clicks.browser_platform_id
          AND Browsers.account_id = Clicks.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Clicks.dv360_country_code, '-', Clicks.state_region)
          AND States.account_id = Clicks.account_id
    )
    SELECT
      date,
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
      state_region,
      state_region_name,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      site_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      browser_platform_id,
      browser_platform,
      country_code,
      SUM(total_clicks) AS total_clicks,
      SUM(total_revenue_advertiser_currency) AS total_revenue_advertiser_currency,
      SUM(total_media_cost_advertiser_currency) AS total_media_cost_advertiser_currency
    FROM
      AggregatedClicks
    GROUP BY
      date,
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
      state_region,
      state_region_name,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      site_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      browser_platform_id,
      browser_platform,
      country_code
    EXCEPT DISTINCT
    SELECT
      date,
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
      state_region,
      state_region_name,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      site_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      browser_platform_id,
      browser_platform,
      country_code,
      total_clicks,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
  )
)
= 0;

---description:CDCCheck2:
---Data in the ClicksDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedClicks AS (
      SELECT
        Clicks.account_id,
        Clicks.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Clicks.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Clicks.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Clicks.event_time)) AS date,
        Clicks.dv360_insertion_order_id AS insertion_order,
        Clicks.dv360_line_item_id AS line_item_id,
        COALESCE(Clicks.dv360_device_type, Clicks.browser_platform_id) AS device,
        Clicks.dv360_browser_platform_id,
        Clicks.browser_platform_id,
        Browsers.browser_platform,
        Clicks.dv360_site_id AS site_name,
        Clicks.dv360_country_code AS country_code,
        Clicks.state_region,
        States.state_region_full_name AS state_region_name,
        Clicks.dv360_url AS url,
        Clicks.dv360_matching_targeted_segments AS audience_type,
        Clicks.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        IF(Clicks.event_type = 'CLICK', 1, 0) AS total_clicks,
        Clicks.dv360_revenue_advertiser_currency / 1000000000 AS total_revenue_advertiser_currency,
        Clicks.dv360_total_media_cost_advertiser_currency / 1000000000
          AS total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Clicks` AS Clicks
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Clicks.advertiser_id
          AND Advertisers.account_id = Clicks.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Clicks.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Clicks.ad_id AND Ads.advertiser_id = Clicks.advertiser_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Clicks.browser_platform_id
          AND Browsers.account_id = Clicks.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Clicks.dv360_country_code, '-', Clicks.state_region)
          AND States.account_id = Clicks.account_id
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
      total_clicks,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ClicksDailyAgg`
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
      SUM(total_clicks) AS total_clicks,
      SUM(total_revenue_advertiser_currency) AS total_revenue_advertiser_currency,
      SUM(total_media_cost_advertiser_currency) AS total_media_cost_advertiser_currency
    FROM
      AggregatedClicks
    GROUP BY
      date,
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
      state_region,
      state_region_name,
      insertion_order,
      line_item_id,
      device,
      dv360_browser_platform_id,
      site_name,
      url,
      audience_type,
      browser_timezone_offset_minutes,
      browser_platform_id,
      browser_platform,
      country_code
  )
)
= 0;
