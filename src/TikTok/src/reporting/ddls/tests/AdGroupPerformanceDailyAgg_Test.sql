---description:SmokeTest: Data is populated to the AdGroupPerformanceDailyAgg table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
> 0;

---description:DataTypesCheck1:
---Schema should match expected schema for AdGroupPerformanceDailyAgg table
---INFORMATION_SCHEMA data EXCEPT expected schema
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformanceDailyAgg'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('AdGroupPerformanceDailyAgg', 'date', 'DATE'),
      STRUCT('AdGroupPerformanceDailyAgg', 'advertiser_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'campaign_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'adgroup_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'adgroup_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'country_code', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'currency', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'timezone', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'display_timezone', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_cost', 'FLOAT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_impressions', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_clicks', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'reach_daily', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_conversions', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_video_views', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_likes', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_comments', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_shares', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_follows', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'conversion_eligible_cost', 'FLOAT64')])
  )
)
= 0;

---description:DataTypesCheck2:
---Schema should match expected schema for AdGroupPerformanceDailyAgg table
---Expected schema EXCEPT INFORMATION_SCHEMA data
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('AdGroupPerformanceDailyAgg', 'date', 'DATE'),
      STRUCT('AdGroupPerformanceDailyAgg', 'advertiser_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'campaign_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'adgroup_id', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'adgroup_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'country_code', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'currency', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'timezone', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'display_timezone', 'STRING'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_cost', 'FLOAT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_impressions', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_clicks', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'reach_daily', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_conversions', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_video_views', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_likes', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_comments', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_shares', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'total_follows', 'INT64'),
      STRUCT('AdGroupPerformanceDailyAgg', 'conversion_eligible_cost', 'FLOAT64')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'AdGroupPerformanceDailyAgg'
  )
)
= 0;

---description:MinValueProfileCheck: date column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(date) IS NULL THEN '999999999999'
      ELSE CAST(MIN(date) AS STRING)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE date IS NOT NULL
)
>= '2023-05-17';

---description:NullCountCheck: NULL count should be as expected in advertiser_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE advertiser_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_id column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_id)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in campaign_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE campaign_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_id column
ASSERT (
  SELECT COUNT(DISTINCT campaign_id)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in adgroup_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE adgroup_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in adgroup_id column
ASSERT (
  SELECT COUNT(DISTINCT adgroup_id)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 2;

---description:NullCountCheck: NULL count should be as expected in campaign_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE campaign_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_name column
ASSERT (
  SELECT COUNT(DISTINCT campaign_name)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:ENUMCheck:
---Values in adgroup_name column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE CAST(adgroup_name AS STRING)
    NOT IN ('Known audience', 'Cat lovers', 'Female >18', 'My data')
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in adgroup_name column
ASSERT (
  SELECT COUNT(DISTINCT adgroup_name)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 2;

---description:NullCountCheck: NULL count should be as expected in advertiser_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE advertiser_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_name column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_name)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in country_code column
ASSERT (
  SELECT COUNT(DISTINCT country_code)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 2;

---description:ENUMCheck:
---Values in country_code column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE CAST(country_code AS STRING)
    NOT IN ('AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU',
            'AT', 'AZ', 'BS', 'BH', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ',
            'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'CV', 'KH', 'CM', 'CA', 'KY',
            'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CG', 'CD', 'CK', 'CR', 'CI', 'HR',
            'CU', 'CW', 'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE',
            'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE',
            'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM',
            'VA', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM',
            'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS',
            'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ',
            'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA',
            'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MK', 'MP', 'NO', 'OM',
            'PK', 'PW', 'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE',
            'RO', 'RU', 'RW', 'BL', 'SH', 'KN', 'LC', 'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA',
            'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES',
            'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TK',
            'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'UK', 'US', 'UM', 'UY',
            'UZ', 'VU', 'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in currency column
ASSERT (
  SELECT COUNT(DISTINCT currency)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:ENUMCheck:
---Values in currency column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE CAST(currency AS STRING) NOT IN ('USD')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in timezone column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE timezone IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in timezone column
ASSERT (
  SELECT COUNT(DISTINCT timezone)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:ValueFormatCheck: Column timezone has expected pattern
ASSERT (
  SELECT
    COUNT(timezone)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(timezone AS STRING),
    r'\S*/\S*')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in display_timezone column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE display_timezone IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in display_timezone column
ASSERT (
  SELECT COUNT(DISTINCT display_timezone)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in total_cost column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_cost IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in total_cost column
ASSERT (
  SELECT COUNT(DISTINCT total_cost)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 15;

---description:MinValueProfileCheck: total_cost column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_cost) IS NULL THEN 999999999999
      ELSE MIN(total_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_cost IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_cost column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_cost) IS NULL THEN -999999999999
      ELSE MAX(total_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_cost IS NOT NULL
)
<= 15;

---description:AvgValueProfileCheck: total_cost column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_cost AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_cost IS NOT NULL
)
= 9.083333;

---description:FractionCheck: total_cost column has valid average value
ASSERT (
  SELECT
    CASE
      WHEN STRPOS(CAST(total_cost AS STRING), '.') = 0 THEN 0
      ELSE LENGTH(CAST(total_cost AS STRING)) - STRPOS(CAST(total_cost AS STRING), '.')
      END AS frac_len  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  ORDER BY frac_len DESC
  LIMIT 1
)
= 2;

---description:MinValueProfileCheck: total_cost column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_cost) IS NULL THEN 999999999999
      ELSE MIN(total_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_cost IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_impressions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_impressions IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_impressions column
ASSERT (
  SELECT COUNT(DISTINCT total_impressions)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 31;

---description:MinValueProfileCheck: total_impressions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_impressions) IS NULL THEN 999999999999
      ELSE MIN(total_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
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
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_impressions IS NOT NULL
)
<= 4453;

---description:AvgValueProfileCheck: total_impressions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_impressions AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_impressions IS NOT NULL
)
= 2086.757576;

---description:MinValueProfileCheck: total_impressions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_impressions) IS NULL THEN 999999999999
      ELSE MIN(total_impressions)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_impressions IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_clicks column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_clicks IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_clicks column
ASSERT (
  SELECT COUNT(DISTINCT total_clicks)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 15;

---description:MinValueProfileCheck: total_clicks column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_clicks) IS NULL THEN 999999999999
      ELSE MIN(total_clicks)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_clicks IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_clicks column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_clicks) IS NULL THEN -999999999999
      ELSE MAX(total_clicks)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_clicks IS NOT NULL
)
<= 38;

---description:AvgValueProfileCheck: total_clicks column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_clicks AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_clicks IS NOT NULL
)
= 15.363636;

---description:MinValueProfileCheck: total_clicks column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_clicks) IS NULL THEN 999999999999
      ELSE MIN(total_clicks)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_clicks IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in reach_daily column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE reach_daily IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in reach_daily column
ASSERT (
  SELECT COUNT(DISTINCT reach_daily)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 31;

---description:MinValueProfileCheck: reach_daily column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(reach_daily) IS NULL THEN 999999999999
      ELSE MIN(reach_daily)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE reach_daily IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: reach_daily column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(reach_daily) IS NULL THEN -999999999999
      ELSE MAX(reach_daily)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE reach_daily IS NOT NULL
)
<= 3600;

---description:AvgValueProfileCheck: reach_daily column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(reach_daily AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE reach_daily IS NOT NULL
)
= 1696.606061;

---description:MinValueProfileCheck: reach_daily column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(reach_daily) IS NULL THEN 999999999999
      ELSE MIN(reach_daily)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE reach_daily IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_conversions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_conversions IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_conversions column
ASSERT (
  SELECT COUNT(DISTINCT total_conversions)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_conversions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_conversions) IS NULL THEN 999999999999
      ELSE MIN(total_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_conversions IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_conversions column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_conversions) IS NULL THEN -999999999999
      ELSE MAX(total_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_conversions IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck: total_conversions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_conversions AS BIGNUMERIC)), 11)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_conversions IS NOT NULL
)
= 0;

---description:MinValueProfileCheck: total_conversions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_conversions) IS NULL THEN 999999999999
      ELSE MIN(total_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_conversions IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_video_views column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_video_views IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_video_views column
ASSERT (
  SELECT COUNT(DISTINCT total_video_views)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 30;

---description:MinValueProfileCheck: total_video_views column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_video_views) IS NULL THEN 999999999999
      ELSE MIN(total_video_views)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_video_views IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_video_views column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_video_views) IS NULL THEN -999999999999
      ELSE MAX(total_video_views)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_video_views IS NOT NULL
)
<= 3925;

---description:AvgValueProfileCheck: total_video_views column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_video_views AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_video_views IS NOT NULL
)
= 1862.515152;

---description:MinValueProfileCheck: total_video_views column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_video_views) IS NULL THEN 999999999999
      ELSE MIN(total_video_views)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_video_views IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_likes column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_likes IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in total_likes column
ASSERT (
  SELECT COUNT(DISTINCT total_likes)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 4;

---description:MinValueProfileCheck: total_likes column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_likes) IS NULL THEN 999999999999
      ELSE MIN(total_likes)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_likes IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_likes column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_likes) IS NULL THEN -999999999999
      ELSE MAX(total_likes)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_likes IS NOT NULL
)
<= 3;

---description:AvgValueProfileCheck: total_likes column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_likes AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_likes IS NOT NULL
)
= 0.69697;

---description:MinValueProfileCheck: total_likes column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_likes) IS NULL THEN 999999999999
      ELSE MIN(total_likes)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_likes IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_comments column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_comments IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_comments column
ASSERT (
  SELECT COUNT(DISTINCT total_comments)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 2;

---description:MinValueProfileCheck: total_comments column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_comments) IS NULL THEN 999999999999
      ELSE MIN(total_comments)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_comments IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_comments column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_comments) IS NULL THEN -999999999999
      ELSE MAX(total_comments)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_comments IS NOT NULL
)
<= 1;

---description:AvgValueProfileCheck: total_comments column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_comments AS BIGNUMERIC)), 6)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_comments IS NOT NULL
)
= 0.030303;

---description:MinValueProfileCheck: total_comments column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_comments) IS NULL THEN 999999999999
      ELSE MIN(total_comments)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_comments IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_shares column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_shares IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_shares column
ASSERT (
  SELECT COUNT(DISTINCT total_shares)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_shares column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_shares) IS NULL THEN 999999999999
      ELSE MIN(total_shares)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_shares IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_shares column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_shares) IS NULL THEN -999999999999
      ELSE MAX(total_shares)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_shares IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck: total_shares column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_shares AS BIGNUMERIC)), 11)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_shares IS NOT NULL
)
= 0;

---description:MinValueProfileCheck: total_shares column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_shares) IS NULL THEN 999999999999
      ELSE MIN(total_shares)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_shares IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in total_follows column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_follows IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_follows column
ASSERT (
  SELECT COUNT(DISTINCT total_follows)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_follows column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_follows) IS NULL THEN 999999999999
      ELSE MIN(total_follows)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_follows IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_follows column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_follows) IS NULL THEN -999999999999
      ELSE MAX(total_follows)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_follows IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck: total_follows column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_follows AS BIGNUMERIC)), 11)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_follows IS NOT NULL
)
= 0;

---description:MinValueProfileCheck: total_follows column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_follows) IS NULL THEN 999999999999
      ELSE MIN(total_follows)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE total_follows IS NOT NULL
)
>= 0;

---description:NullCountCheck: NULL count should be as expected in conversion_eligible_cost column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE conversion_eligible_cost IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in conversion_eligible_cost column
ASSERT (
  SELECT COUNT(DISTINCT conversion_eligible_cost)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
)
= 1;

---description:MinValueProfileCheck: conversion_eligible_cost column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(conversion_eligible_cost) IS NULL THEN 999999999999
      ELSE MIN(conversion_eligible_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE conversion_eligible_cost IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: conversion_eligible_cost column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(conversion_eligible_cost) IS NULL THEN -999999999999
      ELSE MAX(conversion_eligible_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE conversion_eligible_cost IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck: conversion_eligible_cost column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(conversion_eligible_cost AS BIGNUMERIC)), 11)
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE conversion_eligible_cost IS NOT NULL
)
= 0;

---description:FractionCheck: conversion_eligible_cost column has valid average value
ASSERT (
  SELECT
    CASE
      WHEN STRPOS(CAST(conversion_eligible_cost AS STRING), '.') = 0 THEN 0
      ELSE LENGTH(CAST(conversion_eligible_cost AS STRING))
        -
        STRPOS(CAST(conversion_eligible_cost AS STRING), '.')
      END AS frac_len  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  ORDER BY frac_len DESC
  LIMIT 1
)
= 0;

---description:MinValueProfileCheck: conversion_eligible_cost column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(conversion_eligible_cost) IS NULL THEN 999999999999
      ELSE MIN(conversion_eligible_cost)
      END  -- noqa: L003
  FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  WHERE conversion_eligible_cost IS NOT NULL
)
>= 0;

---description:CDCCheck1:
---Data in the AdGroupPerformanceDailyAgg table should be transformed following business rules
---Reporting 1 layer data EXCEPT Reporting 2 layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AdGroupPerfAgg AS (
      SELECT
        stat_time_day,
        advertiser_id,
        campaign_id,
        adgroup_id,
        campaign_name,
        adgroup_name,
        advertiser_name,
        country_code,
        currency,
        timezone,
        display_timezone,
        SUM(spend) AS total_cost,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversion) AS total_conversions,
        SUM(video_play_actions) AS total_video_views,
        SUM(likes) AS total_likes,
        SUM(comments) AS total_comments,
        SUM(shares) AS total_shares,
        SUM(follows) AS total_follows,
        SUM(conversion * cost_per_conversion) AS conversion_eligible_cost
      FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
      GROUP BY
        stat_time_day,
        advertiser_id,
        advertiser_name,
        adgroup_id,
        adgroup_name,
        campaign_id,
        campaign_name,
        country_code,
        currency,
        timezone,
        display_timezone
    )
    SELECT
      AdGroupPerfAgg.stat_time_day AS date,
      AdGroupPerfAgg.advertiser_id,
      AdGroupPerfAgg.campaign_id,
      AdGroupPerfAgg.adgroup_id,
      AdGroupPerfAgg.campaign_name,
      AdGroupPerfAgg.adgroup_name,
      AdGroupPerfAgg.advertiser_name,
      AdGroupPerfAgg.country_code,
      AdGroupPerfAgg.currency,
      AdGroupPerfAgg.timezone,
      AdGroupPerfAgg.display_timezone,
      AdGroupPerformance.reach AS reach_daily,
      AdGroupPerfAgg.total_cost,
      AdGroupPerfAgg.total_impressions,
      AdGroupPerfAgg.total_clicks,
      AdGroupPerfAgg.total_conversions,
      AdGroupPerfAgg.total_video_views,
      AdGroupPerfAgg.total_likes,
      AdGroupPerfAgg.total_comments,
      AdGroupPerfAgg.total_shares,
      AdGroupPerfAgg.total_follows,
      AdGroupPerfAgg.conversion_eligible_cost
    FROM AdGroupPerfAgg
    INNER JOIN `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
      AS AdGroupPerformance
      ON
        AdGroupPerfAgg.advertiser_id = AdGroupPerformance.advertiser_id
        AND AdGroupPerfAgg.campaign_id = AdGroupPerformance.campaign_id
        AND AdGroupPerfAgg.adgroup_id = AdGroupPerformance.adgroup_id
        AND AdGroupPerfAgg.country_code = AdGroupPerformance.country_code
        AND AdGroupPerfAgg.stat_time_day = AdGroupPerformance.stat_time_day
    EXCEPT DISTINCT
    SELECT
      date,
      advertiser_id,
      campaign_id,
      adgroup_id,
      campaign_name,
      adgroup_name,
      advertiser_name,
      country_code,
      currency,
      timezone,
      display_timezone,
      reach_daily,
      total_cost,
      total_impressions,
      total_clicks,
      total_conversions,
      total_video_views,
      total_likes,
      total_comments,
      total_shares,
      total_follows,
      conversion_eligible_cost
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
  )
)
= 0;

---description:CDCCheck2:
---Data in the AdGroupPerformanceDailyAgg table should be transformed following business rules
---Reporting 2 layer data EXCEPT Reporting 1 layer data
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AdGroupPerfAgg AS (
      SELECT
        stat_time_day,
        advertiser_id,
        campaign_id,
        adgroup_id,
        campaign_name,
        adgroup_name,
        advertiser_name,
        country_code,
        currency,
        timezone,
        display_timezone,
        SUM(spend) AS total_cost,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversion) AS total_conversions,
        SUM(video_play_actions) AS total_video_views,
        SUM(likes) AS total_likes,
        SUM(comments) AS total_comments,
        SUM(shares) AS total_shares,
        SUM(follows) AS total_follows,
        SUM(conversion * cost_per_conversion) AS conversion_eligible_cost
      FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
      GROUP BY
        stat_time_day,
        advertiser_id,
        advertiser_name,
        adgroup_id,
        adgroup_name,
        campaign_id,
        campaign_name,
        country_code,
        currency,
        timezone,
        display_timezone
    )
    SELECT
      date,
      advertiser_id,
      campaign_id,
      adgroup_id,
      campaign_name,
      adgroup_name,
      advertiser_name,
      country_code,
      currency,
      timezone,
      display_timezone,
      reach_daily,
      total_cost,
      total_impressions,
      total_clicks,
      total_conversions,
      total_video_views,
      total_likes,
      total_comments,
      total_shares,
      total_follows,
      conversion_eligible_cost
    FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
    EXCEPT DISTINCT
    SELECT
      AdGroupPerfAgg.stat_time_day AS date,
      AdGroupPerfAgg.advertiser_id,
      AdGroupPerfAgg.campaign_id,
      AdGroupPerfAgg.adgroup_id,
      AdGroupPerfAgg.campaign_name,
      AdGroupPerfAgg.adgroup_name,
      AdGroupPerfAgg.advertiser_name,
      AdGroupPerfAgg.country_code,
      AdGroupPerfAgg.currency,
      AdGroupPerfAgg.timezone,
      AdGroupPerfAgg.display_timezone,
      AdGroupPerformance.reach AS reach_daily,
      AdGroupPerfAgg.total_cost,
      AdGroupPerfAgg.total_impressions,
      AdGroupPerfAgg.total_clicks,
      AdGroupPerfAgg.total_conversions,
      AdGroupPerfAgg.total_video_views,
      AdGroupPerfAgg.total_likes,
      AdGroupPerfAgg.total_comments,
      AdGroupPerfAgg.total_shares,
      AdGroupPerfAgg.total_follows,
      AdGroupPerfAgg.conversion_eligible_cost
    FROM AdGroupPerfAgg
    INNER JOIN `{{ project_id_src }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
      AS AdGroupPerformance
      ON
        AdGroupPerfAgg.advertiser_id = AdGroupPerformance.advertiser_id
        AND AdGroupPerfAgg.campaign_id = AdGroupPerformance.campaign_id
        AND AdGroupPerfAgg.adgroup_id = AdGroupPerformance.adgroup_id
        AND AdGroupPerfAgg.country_code = AdGroupPerformance.country_code
        AND AdGroupPerfAgg.stat_time_day = AdGroupPerformance.stat_time_day
  )
)
= 0;
