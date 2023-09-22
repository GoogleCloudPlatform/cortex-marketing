---description:SmokeTest: Data is populated to the KeywordsDailyAgg table
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
> 0;

---description:DataTypesCheck1: Schema should match expected schema for KeywordsDailyAgg table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'KeywordsDailyAgg'
    EXCEPT DISTINCT
    SELECT * FROM UNNEST([
      STRUCT('KeywordsDailyAgg', 'report_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'criterion_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'keyword', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'match_type', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'keyword_status', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'customer_name', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_status', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'currency_code', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_time_zone', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'adgroup_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'ad_group', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'quality_score', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'clicks', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'cost', 'FLOAT64')])
  )
)
= 0;

---description:DataTypesCheck2: Schema should match expected schema for KeywordsDailyAgg table
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT * FROM UNNEST([
      STRUCT('KeywordsDailyAgg', 'report_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'criterion_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'keyword', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'match_type', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'keyword_status', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'customer_name', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_status', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'currency_code', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'customer_time_zone', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'campaign_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'campaign_name', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'campaign_start_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'campaign_end_date', 'DATE'),
      STRUCT('KeywordsDailyAgg', 'adgroup_id', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'ad_group', 'STRING'),
      STRUCT('KeywordsDailyAgg', 'quality_score', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'clicks', 'INT64'),
      STRUCT('KeywordsDailyAgg', 'cost', 'FLOAT64')])
    EXCEPT DISTINCT
    SELECT
      table_name,
      column_name,
      data_type
    FROM {{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'KeywordsDailyAgg'
  )
)
= 0;

---description:RowCountCheck: Row count in the KeywordsDailyAgg should be as expected
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1164.0;

---description:NullCountCheck: NULL count should be as expected in report_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE report_date IS NULL
)
= 0;

---description:NullCountCheck: NULL count should be as expected in criterion_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE criterion_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in criterion_id column
ASSERT (
  SELECT COUNT(DISTINCT criterion_id)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 8;

---description:MinValueProfileCheck: criterion_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(criterion_id) IS NULL THEN 999999999999
      ELSE MIN(criterion_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE criterion_id IS NOT NULL
)
>= 10053926;

---description:MaxValueProfileCheck: criterion_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(criterion_id) IS NULL THEN -999999999999
      ELSE MAX(criterion_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE criterion_id IS NOT NULL
)
<= 493051433472;

---description:AvgValueProfileCheck: criterion_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(criterion_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE criterion_id IS NOT NULL
)
= 174361622899.1795532646;

---description:NullCountCheck: NULL count should be as expected in keyword column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE keyword IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in keyword column
ASSERT (
  SELECT COUNT(DISTINCT keyword)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 8;

---description:ENUMCheck: Values in match_type column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE CAST(match_type AS STRING) NOT IN ('UNSPECIFIED', 'UNKNOWN', 'EXACT', 'PHRASE', 'BROAD')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in match_type column
ASSERT (
  SELECT COUNT(DISTINCT match_type)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:ENUMCheck: Values in keyword_status column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE CAST(keyword_status AS STRING) NOT IN ('UNSPECIFIED', 'UNKNOWN', 'ENABLED', 'PAUSED',
                                               'REMOVED')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in keyword_status column
ASSERT (
  SELECT COUNT(DISTINCT keyword_status)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in customer_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in customer_id column
ASSERT (
  SELECT COUNT(DISTINCT customer_id)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:MinValueProfileCheck: customer_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(customer_id) IS NULL THEN 999999999999
      ELSE MIN(customer_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_id IS NOT NULL
)
>= 2437583133;

---description:MaxValueProfileCheck: customer_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(customer_id) IS NULL THEN -999999999999
      ELSE MAX(customer_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_id IS NOT NULL
)
<= 2437583133;

---description:AvgValueProfileCheck: customer_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(customer_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_id IS NOT NULL
)
= 2437583133;

---description:NullCountCheck: NULL count should be as expected in customer_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_name IS NULL
)
= 1164;

---description:DistinctCountProfileCheck: Distinct count should be as expected in customer_name column
ASSERT (
  SELECT COUNT(DISTINCT customer_name)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 0;

---description:ENUMCheck: Values in customer_status column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE CAST(customer_status AS STRING) NOT IN ('CANCELED', 'CLOSED', 'ENABLED', 'SUSPENDED',
                                                'UNKNOWN', 'UNSPECIFIED')
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in customer_status column
ASSERT (
  SELECT COUNT(DISTINCT customer_status)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:LengthCheck: Length of values in currency_code column should be as expected
ASSERT (
  SELECT
    COUNT(currency_code)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE LENGTH(CAST(currency_code AS STRING)) > 3
)
= 0;

---description:TestUPPERCASE: All values should be in UPPERCASE only in currency_code column
ASSERT (
  SELECT COUNT(currency_code)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE UPPER(currency_code) != currency_code
)
= 0;

---description:ENUMCheck: Values in currency_code column should be as expected based on API specification or NULL
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE CAST(currency_code AS STRING) NOT IN ('AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS',
                                              'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN',
                                              'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL',
                                              'BSD', 'BTN', 'BWP', 'BYN', 'BZD', 'CAD', 'CDF',
                                              'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'COP', 'COU',
                                              'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK',
                                              'DOP', 'DZD', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD',
                                              'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF',
                                              'GTQ', 'GYD', 'HKD', 'HNL', 'HTG', 'HUF', 'IDR',
                                              'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD',
                                              'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW',
                                              'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD',
                                              'LSL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK',
                                              'MNT', 'MOP', 'MRU', 'MUR', 'MVR', 'MWK', 'MXN',
                                              'MXV', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK',
                                              'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP',
                                              'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'CNY',
                                              'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK',
                                              'SGD', 'SHP', 'SLE', 'SLL', 'SOS', 'SRD', 'SSP',
                                              'STN', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT',
                                              'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH',
                                              'UGX', 'USD', 'USN', 'UYI', 'UYU', 'UYW', 'UZS',
                                              'VED', 'VES', 'VND', 'VUV', 'WST', 'XAF', 'XAG',
                                              'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR',
                                              'XOF', 'XPD', 'XPF', 'XPT', 'XSU', 'XTS', 'XUA',
                                              'XXX', 'YER', 'ZAR', 'ZMW', 'ZWL')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in currency_code column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE currency_code IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in currency_code column
ASSERT (
  SELECT COUNT(DISTINCT currency_code)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:ValueFormatCheck: Column customer_time_zone has expected pattern
ASSERT (
  SELECT
    COUNT(customer_time_zone)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE NOT REGEXP_CONTAINS(CAST(customer_time_zone AS STRING),
    r'\S*/\S*')
)
= 0;

---description:NullCountCheck: NULL count should be as expected in customer_time_zone column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE customer_time_zone IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in customer_time_zone column
ASSERT (
  SELECT COUNT(DISTINCT customer_time_zone)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in campaign_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_id column
ASSERT (
  SELECT COUNT(DISTINCT campaign_id)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 2;

---description:MinValueProfileCheck: campaign_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(campaign_id) IS NULL THEN 999999999999
      ELSE MIN(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
>= 18373697715;

---description:MaxValueProfileCheck: campaign_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(campaign_id) IS NULL THEN -999999999999
      ELSE MAX(campaign_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
<= 19675732311;

---description:AvgValueProfileCheck: campaign_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(campaign_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_id IS NOT NULL
)
= 18616430968.72164948454;

---description:NullCountCheck: NULL count should be as expected in campaign_name column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_name IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_name column
ASSERT (
  SELECT COUNT(DISTINCT campaign_name)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 2;

---description:NullCountCheck: NULL count should be as expected in campaign_start_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_start_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_start_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_start_date)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 2;

---description:NullCountCheck: NULL count should be as expected in campaign_end_date column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE campaign_end_date IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in campaign_end_date column
ASSERT (
  SELECT COUNT(DISTINCT campaign_end_date)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 1;

---description:NullCountCheck: NULL count should be as expected in adgroup_id column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE adgroup_id IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in adgroup_id column
ASSERT (
  SELECT COUNT(DISTINCT adgroup_id)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 6;

---description:MinValueProfileCheck: adgroup_id column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(adgroup_id) IS NULL THEN 999999999999
      ELSE MIN(adgroup_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE adgroup_id IS NOT NULL
)
>= 141607684545;

---description:MaxValueProfileCheck: adgroup_id column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(adgroup_id) IS NULL THEN -999999999999
      ELSE MAX(adgroup_id)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE adgroup_id IS NOT NULL
)
<= 146809382635;

---description:AvgValueProfileCheck: adgroup_id column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(adgroup_id AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE adgroup_id IS NOT NULL
)
= 143190688385.45532646048;

---description:NullCountCheck: NULL count should be as expected in ad_group column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE ad_group IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in ad_group column
ASSERT (
  SELECT COUNT(DISTINCT ad_group)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 6;

---description:NullCountCheck: NULL count should be as expected in quality_score column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE quality_score IS NULL
)
= 369;

---description:DistinctCountProfileCheck: Distinct count should be as expected in quality_score column
ASSERT (
  SELECT COUNT(DISTINCT quality_score)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 5;

---description:MinValueProfileCheck: quality_score column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(quality_score) IS NULL THEN 999999999999
      ELSE MIN(quality_score)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE quality_score IS NOT NULL
)
>= 1;

---description:MaxValueProfileCheck: quality_score column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(quality_score) IS NULL THEN -999999999999
      ELSE MAX(quality_score)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE quality_score IS NOT NULL
)
<= 8;

---description:AvgValueProfileCheck: quality_score column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(quality_score AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE quality_score IS NOT NULL
)
= 4.25408805031;

---description:NullCountCheck: NULL count should be as expected in clicks column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE clicks IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in clicks column
ASSERT (
  SELECT COUNT(DISTINCT clicks)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 55;

---description:MinValueProfileCheck: clicks column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(clicks) IS NULL THEN 999999999999
      ELSE MIN(clicks)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE clicks IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: clicks column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(clicks) IS NULL THEN -999999999999
      ELSE MAX(clicks)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE clicks IS NOT NULL
)
<= 166;

---description:AvgValueProfileCheck: clicks column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(clicks AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE clicks IS NOT NULL
)
= 6.27233676976;

---description:NullCountCheck: NULL count should be as expected in cost column
ASSERT (
  SELECT
    COUNT(*)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE cost IS NULL
)
= 0;

---description:DistinctCountProfileCheck: Distinct count should be as expected in cost column
ASSERT (
  SELECT COUNT(DISTINCT cost)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
)
= 350;

---description:MinValueProfileCheck: cost column has valid MIN value
ASSERT (
  SELECT
    CASE
      WHEN MIN(cost) IS NULL THEN 999999999999
      ELSE MIN(cost)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE cost IS NOT NULL
)
>= 0;

---description:MaxValueProfileCheck: cost column has valid MAX value
ASSERT (
  SELECT
    CASE
      WHEN MAX(cost) IS NULL THEN -999999999999
      ELSE MAX(cost)
      END  -- noqa: L003
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE cost IS NOT NULL
)
<= 10.03;

---description:AvgValueProfileCheck: cost column has valid average value
ASSERT (
  SELECT ROUND(AVG(CAST(cost AS BIGNUMERIC)), 11)
  FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  WHERE cost IS NOT NULL
)
= 1.21164089347;

---description:CDCCheck1: Data in the KeywordsDailyAgg view should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      Keywords.segments.date AS report_date,
      AdGroupCriterion.criterion_id,
      AdGroupCriterion.keyword.text AS keyword,
      AdGroupCriterion.keyword.match_type,
      AdGroupCriterion.status AS keyword_status,
      Customers.customer_id,
      Customers.descriptive_name AS customer_name,
      Customers.status AS customer_status,
      Customers.currency_code,
      Customers.time_zone AS customer_time_zone,
      Campaigns.campaign_id,
      Campaigns.name AS campaign_name,
      Campaigns.start_date AS campaign_start_date,
      Campaigns.end_date AS campaign_end_date,
      AdGroups.adgroup_id,
      AdGroups.name AS ad_group,
      AdGroupCriterion.quality_info.quality_score,
      Keywords.metrics.clicks,
      Keywords.metrics.cost_micros / 1000000 AS cost
    FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Keywords` AS Keywords
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroupCriterion`
        AS AdGroupCriterion
      ON
        AdGroupCriterion.criterion_id = Keywords.ad_group_criterion.criterion_id
        AND AdGroupCriterion.adgroup_id = Keywords.ad_group.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Campaigns`
      AS Campaigns
      ON
        Campaigns.campaign_id = Keywords.campaign.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroups`
      AS AdGroups
      ON
        AdGroups.adgroup_id = Keywords.ad_group.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Customers`
      AS Customers
      ON
        Customers.customer_id = Keywords.customer.id
    WHERE AdGroupCriterion.type_ = 'KEYWORD'
    EXCEPT DISTINCT
    SELECT
      report_date,
      criterion_id,
      keyword,
      match_type,
      keyword_status,
      customer_id,
      customer_name,
      customer_status,
      currency_code,
      customer_time_zone,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      adgroup_id,
      ad_group,
      quality_score,
      clicks,
      cost
    FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
  )
)
= 0;

---description:CDCCheck2: Data in the KeywordsDailyAgg view should be transformed following business rules
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      report_date,
      criterion_id,
      keyword,
      match_type,
      keyword_status,
      customer_id,
      customer_name,
      customer_status,
      currency_code,
      customer_time_zone,
      campaign_id,
      campaign_name,
      campaign_start_date,
      campaign_end_date,
      adgroup_id,
      ad_group,
      quality_score,
      clicks,
      cost
    FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.KeywordsDailyAgg`
    EXCEPT DISTINCT
    SELECT
      Keywords.segments.date AS report_date,
      AdGroupCriterion.criterion_id,
      AdGroupCriterion.keyword.text AS keyword,
      AdGroupCriterion.keyword.match_type,
      AdGroupCriterion.status AS keyword_status,
      Customers.customer_id,
      Customers.descriptive_name AS customer_name,
      Customers.status AS customer_status,
      Customers.currency_code,
      Customers.time_zone AS customer_time_zone,
      Campaigns.campaign_id,
      Campaigns.name AS campaign_name,
      Campaigns.start_date AS campaign_start_date,
      Campaigns.end_date AS campaign_end_date,
      AdGroups.adgroup_id,
      AdGroups.name AS ad_group,
      AdGroupCriterion.quality_info.quality_score,
      Keywords.metrics.clicks,
      Keywords.metrics.cost_micros / 1000000 AS cost
    FROM `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Keywords` AS Keywords
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroupCriterion`
        AS AdGroupCriterion
      ON
        AdGroupCriterion.criterion_id = Keywords.ad_group_criterion.criterion_id
        AND AdGroupCriterion.adgroup_id = Keywords.ad_group.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Campaigns`
      AS Campaigns
      ON
        Campaigns.campaign_id = Keywords.campaign.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.AdGroups`
      AS AdGroups
      ON
        AdGroups.adgroup_id = Keywords.ad_group.id
    INNER JOIN `{{ project_id_tgt }}.{{ marketing_googleads_datasets_reporting }}.Customers`
      AS Customers
      ON
        Customers.customer_id = Keywords.customer.id
    WHERE AdGroupCriterion.type_ = 'KEYWORD'
  )
)
= 0;
