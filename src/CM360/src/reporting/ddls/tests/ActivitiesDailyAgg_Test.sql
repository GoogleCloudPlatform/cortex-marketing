---description:SmokeTest: Data is populated to the ActivitiesDailyAgg table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
> 0;

---description:DataTypesCheck: Schema should match expected schema for ActivitiesDailyAgg table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ActivitiesDailyAgg'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('ActivitiesDailyAgg', 'account_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'device', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'site_name', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'country_code', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'state_region', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'url', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'browser_timezone_offset_minutes', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_post_click_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_post_view_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ActivitiesDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64')])
  )
)
= 0;

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('ActivitiesDailyAgg', 'account_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'advertiser_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'advertiser_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'ad_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'ad_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'ad_type', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'date', 'DATE'),
      STRUCT('ActivitiesDailyAgg', 'insertion_order', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'line_item_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'device', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'browser_platform_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'browser_platform', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'dv360_browser_platform_id', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'site_name', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'country_code', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'state_region', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'state_region_name', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'url', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'audience_type', 'STRING'),
      STRUCT('ActivitiesDailyAgg', 'browser_timezone_offset_minutes', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_post_click_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_post_view_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_conversions', 'INT64'),
      STRUCT('ActivitiesDailyAgg', 'total_revenue_advertiser_currency', 'FLOAT64'),
      STRUCT('ActivitiesDailyAgg', 'total_media_cost_advertiser_currency', 'FLOAT64')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'ActivitiesDailyAgg'
  )
)
= 0;

---description:RowCountCheck: Row count in the ActivitiesDailyAgg should be as expected
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 12.0;

---description:ValueFormatCheck: Column account_id has expected pattern
ASSERT (
  SELECT
    COUNT(account_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(account_id AS STRING),
    r'7480|9386')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in account_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE account_id IS NULL
)
= 0;

---description:NullCountCheck: NULL count should be as expected in advertiser_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE advertiser_id IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_id column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:MinValueProfileCheck: advertiser_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(advertiser_id) IS NULL THEN 999999999999
      ELSE MIN(advertiser_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE advertiser_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in advertiser_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE advertiser_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in advertiser_name column
ASSERT (
  SELECT COUNT(DISTINCT advertiser_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 9;

---description:NullCountCheck: NULL count should be as expected in campaign_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE campaign_id IS NULL
)
= 12;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_id column
ASSERT (
  SELECT COUNT(DISTINCT campaign_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in campaign_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE campaign_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_name column
ASSERT (
  SELECT COUNT(DISTINCT campaign_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 9;

---description:NullCountCheck: NULL count should be as expected in campaign_start_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE campaign_start_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_start_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_start_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 9;

---description:NullCountCheck: NULL count should be as expected in campaign_end_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE campaign_end_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in campaign_end_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_end_date)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 8;

---description:NullCountCheck: NULL count should be as expected in ad_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_id column
ASSERT (
  SELECT COUNT(DISTINCT ad_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 10;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 401794089;

---description:MaxValueProfileCheck: ad_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(ad_id) IS NULL THEN -999999999999
      ELSE MAX(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_id IS NOT NULL
)
<= 558793868;

---description:AvgValueProfileCheck: ad_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(ad_id AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_id IS NOT NULL
)
= 493890297.25;

---description:MinValueProfileCheck: ad_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(ad_id) IS NULL THEN 999999999999
      ELSE MIN(ad_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_id IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in ad_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_name column
ASSERT (
  SELECT COUNT(DISTINCT ad_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 10;

---description:NullCountCheck: NULL count should be as expected in ad_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE ad_type IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_type column
ASSERT (
  SELECT COUNT(DISTINCT ad_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 4;

---description:MinValueProfileCheck: date column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(date) IS NULL THEN '999999999999'
      ELSE CAST(MIN(date) AS STRING)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE date IS NOT NULL
)
>= '2023-02-22';

---description:NullCountCheck: NULL count should be as expected in insertion_order column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE insertion_order IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in insertion_order column
ASSERT (
  SELECT COUNT(DISTINCT insertion_order)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:MinValueProfileCheck: insertion_order column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(insertion_order) IS NULL THEN 999999999999
      ELSE MIN(insertion_order)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE insertion_order IS NOT NULL
)
>= 0.0;

---description:NullCountCheck: NULL count should be as expected in line_item_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE line_item_id IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in line_item_id column
ASSERT (
  SELECT COUNT(DISTINCT line_item_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:MinValueProfileCheck: line_item_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(line_item_id) IS NULL THEN 999999999999
      ELSE MIN(line_item_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE line_item_id IS NOT NULL
)
>= 0.0;

---description:ENUMCheck:
---Values in device column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE CAST(device AS STRING) NOT IN ('0', '1', '2', '3', '4', '5', '28')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in device column
ASSERT (
  SELECT COUNT(DISTINCT device)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 1;

---description:MinValueProfileCheck: device column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(device) IS NULL THEN 999999999999
      ELSE MIN(device)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE device IS NOT NULL
)
>= 28;

---description:MaxValueProfileCheck: device column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(device) IS NULL THEN -999999999999
      ELSE MAX(device)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE device IS NOT NULL
)
<= 28;

---description:AvgValueProfileCheck: device column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(device AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE device IS NOT NULL
)
= 28;

---description:NullCountCheck: NULL count should be as expected in browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_platform_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 1;

---description:MinValueProfileCheck: browser_platform_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(browser_platform_id) IS NULL THEN 999999999999
      ELSE MIN(browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
>= 28;

---description:MaxValueProfileCheck: browser_platform_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(browser_platform_id) IS NULL THEN -999999999999
      ELSE MAX(browser_platform_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
<= 28;

---description:AvgValueProfileCheck: browser_platform_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(browser_platform_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_platform_id IS NOT NULL
)
= 28;

---description:NullCountCheck: NULL count should be as expected in browser_platform column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_platform IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_platform column
ASSERT (
  SELECT COUNT(DISTINCT browser_platform)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE dv360_browser_platform_id IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in dv360_browser_platform_id column
ASSERT (
  SELECT COUNT(DISTINCT dv360_browser_platform_id)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:NullCountCheck: NULL count should be as expected in site_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE site_name IS NULL
)
= 12;

---description:DistinctCountProfileCheck: Distinct count should be as expected in site_name column
ASSERT (
  SELECT COUNT(DISTINCT site_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in country_code column
ASSERT (
  SELECT COUNT(DISTINCT country_code)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:ENUMCheck:
---Values in country_code column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE state_region IS NULL
)
= 7;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region column
ASSERT (
  SELECT COUNT(DISTINCT state_region)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 4;

---description:NullCountCheck: NULL count should be as expected in state_region_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE state_region_name IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in state_region_name column
ASSERT (
  SELECT COUNT(DISTINCT state_region_name)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:NullCountCheck: NULL count should be as expected in url column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE url IS NULL
)
= 12;

---description:DistinctCountProfileCheck: Distinct count should be as expected in url column
ASSERT (
  SELECT COUNT(DISTINCT url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:ValueFormatCheck: Column url has expected pattern
ASSERT (
  SELECT
    COUNT(url)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(url AS STRING),
    r'http[s]?:\/\/[\S]+.[\S]+|source_url_hidden')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in audience_type column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE audience_type IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in audience_type column
ASSERT (
  SELECT COUNT(DISTINCT audience_type)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE browser_timezone_offset_minutes IS NULL
)
= 12;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in browser_timezone_offset_minutes column
ASSERT (
  SELECT COUNT(DISTINCT browser_timezone_offset_minutes)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in total_post_click_conversions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_click_conversions IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_post_click_conversions column
ASSERT (
  SELECT COUNT(DISTINCT total_post_click_conversions)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 5;

---description:MinValueProfileCheck: total_post_click_conversions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_post_click_conversions) IS NULL THEN 999999999999
      ELSE MIN(total_post_click_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_click_conversions IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_post_click_conversions column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_post_click_conversions) IS NULL THEN -999999999999
      ELSE MAX(total_post_click_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_click_conversions IS NOT NULL
)
<= 7;

---description:AvgValueProfileCheck: total_post_click_conversions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_post_click_conversions AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_click_conversions IS NOT NULL
)
= 1.5;

---description:NullCountCheck:
---NULL count should be as expected in total_post_view_conversions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_view_conversions IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_post_view_conversions column
ASSERT (
  SELECT COUNT(DISTINCT total_post_view_conversions)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 2;

---description:MinValueProfileCheck: total_post_view_conversions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_post_view_conversions) IS NULL THEN 999999999999
      ELSE MIN(total_post_view_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_view_conversions IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: total_post_view_conversions column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(total_post_view_conversions) IS NULL THEN -999999999999
      ELSE MAX(total_post_view_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_view_conversions IS NOT NULL
)
<= 1;

---description:AvgValueProfileCheck: total_post_view_conversions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_post_view_conversions AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_post_view_conversions IS NOT NULL
)
= 0.083333;

---description:NullCountCheck: NULL count should be as expected in total_conversions column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_conversions IS NULL
)
= 0;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_conversions column
ASSERT (
  SELECT COUNT(DISTINCT total_conversions)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 5;

---description:MinValueProfileCheck: total_conversions column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_conversions) IS NULL THEN 999999999999
      ELSE MIN(total_conversions)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_conversions IS NOT NULL
)
<= 7;

---description:AvgValueProfileCheck: total_conversions column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_conversions AS BIGNUMERIC)), 6)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_conversions IS NOT NULL
)
= 1.583333;

---description:NullCountCheck:
---NULL count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_revenue_advertiser_currency IS NULL
)
= 4;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_revenue_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_revenue_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_revenue_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_revenue_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_revenue_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck:
---total_revenue_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_revenue_advertiser_currency AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_revenue_advertiser_currency IS NOT NULL
)
= 0;

---description:NullCountCheck:
---NULL count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NULL
)
= 4;

---description:DistinctCountProfileCheck:
---Distinct count should be as expected in total_media_cost_advertiser_currency column
ASSERT (
  SELECT COUNT(DISTINCT total_media_cost_advertiser_currency)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
)
= 1;

---description:MinValueProfileCheck: total_media_cost_advertiser_currency column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(total_media_cost_advertiser_currency) IS NULL THEN 999999999999
      ELSE MIN(total_media_cost_advertiser_currency)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
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
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
<= 0;

---description:AvgValueProfileCheck:
---total_media_cost_advertiser_currency column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(total_media_cost_advertiser_currency AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  WHERE total_media_cost_advertiser_currency IS NOT NULL
)
= 0;

---description:CDCCheck1:
---Data in the ActivitiesDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedActivities AS (
      SELECT
        Activities.account_id,
        Activities.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Activities.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Activities.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Activities.event_time)) AS date,
        Activities.dv360_insertion_order_id AS insertion_order,
        Activities.dv360_line_item_id AS line_item_id,
        COALESCE(Activities.dv360_device_type, Activities.browser_platform_id) AS device,
        Activities.dv360_browser_platform_id,
        Activities.browser_platform_id,
        Browsers.browser_platform,
        Activities.dv360_site_id AS site_name,
        Activities.dv360_country_code AS country_code,
        Activities.state_region,
        States.state_region_full_name AS state_region_name,
        Activities.dv360_url AS url,
        Activities.dv360_matching_targeted_segments AS audience_type,
        Activities.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTCLICK')
          AS is_post_click_conversion,
        (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTVIEW')
          AS is_post_view_conversion,
        Activities.dv360_revenue_advertiser_currency AS total_revenue_advertiser_currency,
        Activities.dv360_total_media_cost_advertiser_currency
          AS total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Activities` AS Activities
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Activities.advertiser_id
          AND Advertisers.account_id = Activities.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Activities.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Activities.ad_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Activities.browser_platform_id
          AND Browsers.account_id = Activities.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Activities.dv360_country_code, '-', Activities.state_region)
          AND States.account_id = Activities.account_id
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
      COUNTIF(is_post_click_conversion) AS total_post_click_conversions,
      COUNTIF(is_post_view_conversion) AS total_post_view_conversions,
      COUNTIF(is_post_click_conversion) + COUNTIF(is_post_view_conversion)
        AS total_conversions,
      SUM(total_revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
      SUM(total_media_cost_advertiser_currency) / 1000000000 AS total_media_cost_advertiser_currency
    FROM
      AggregatedActivities
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
      total_post_click_conversions,
      total_post_view_conversions,
      total_conversions,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
  )
)
= 0;

---description:CDCCheck2:
---Data in the ActivitiesDailyAgg table should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    WITH AggregatedActivities AS (
      SELECT
        Activities.account_id,
        Activities.dv360_advertiser_id AS advertiser_id,
        Advertisers.advertiser AS advertiser_name,
        Activities.dv360_campaign_id AS campaign_id,
        Campaigns.campaign AS campaign_name,
        Campaigns.campaign_start_date,
        Campaigns.campaign_end_date,
        Activities.ad_id,
        Ads.ad AS ad_name,
        Ads.ad_type,
        DATE(TIMESTAMP_MICROS(Activities.event_time)) AS date,
        Activities.dv360_insertion_order_id AS insertion_order,
        Activities.dv360_line_item_id AS line_item_id,
        COALESCE(Activities.dv360_device_type, Activities.browser_platform_id) AS device,
        Activities.dv360_browser_platform_id,
        Activities.browser_platform_id,
        Browsers.browser_platform,
        Activities.dv360_site_id AS site_name,
        Activities.dv360_country_code AS country_code,
        Activities.state_region,
        States.state_region_full_name AS state_region_name,
        Activities.dv360_url AS url,
        Activities.dv360_matching_targeted_segments AS audience_type,
        Activities.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
        (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTCLICK')
          AS is_post_click_conversion,
        (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTVIEW')
          AS is_post_view_conversion,
        Activities.dv360_revenue_advertiser_currency AS total_revenue_advertiser_currency,
        Activities.dv360_total_media_cost_advertiser_currency
          AS total_media_cost_advertiser_currency
      FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Activities` AS Activities
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
        ON
          Advertisers.advertiser_id = Activities.advertiser_id
          AND Advertisers.account_id = Activities.account_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
        ON Campaigns.campaign_id = Activities.campaign_id
      INNER JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
        ON Ads.ad_id = Activities.ad_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
        ON
          Browsers.browser_platform_id = Activities.browser_platform_id
          AND Browsers.account_id = Activities.account_id
      LEFT JOIN `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
        ON
          States.state_region = CONCAT(Activities.dv360_country_code, '-', Activities.state_region)
          AND States.account_id = Activities.account_id
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
      total_post_click_conversions,
      total_post_view_conversions,
      total_conversions,
      total_revenue_advertiser_currency,
      total_media_cost_advertiser_currency
    FROM
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.ActivitiesDailyAgg`
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
      COUNTIF(is_post_click_conversion) AS total_post_click_conversions,
      COUNTIF(is_post_view_conversion) AS total_post_view_conversions,
      COUNTIF(is_post_click_conversion) + COUNTIF(is_post_view_conversion)
        AS total_conversions,
      SUM(total_revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
      SUM(total_media_cost_advertiser_currency) / 1000000000 AS total_media_cost_advertiser_currency
    FROM
      AggregatedActivities
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
