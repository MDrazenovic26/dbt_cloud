{{
    config(
        materialized= 'incremental',
        unique_key = 'HASHID',
        incremental_strategy = 'merge'
    )
}}

with TCURC_NULLS as (
    select
        CASE WHEN INDEX IS NULL THEN -99 ELSE INDEX END AS INDEX,
        CASE WHEN STOREID IS NULL THEN 'NA' ELSE STOREID END AS STOREID,
        CASE WHEN STORENAME IS NULL THEN 'NA' ELSE STORENAME END AS STORENAME,
        CASE WHEN CITY IS NULL THEN 'NA' ELSE CITY END AS CITY,
        CASE WHEN ADDRESS IS NULL THEN 'NA' ELSE ADDRESS END AS ADDRESS,
        CASE WHEN ZIP IS NULL THEN 'NA' ELSE ZIP END AS ZIP,
        CASE WHEN COUNTRYCODE IS NULL THEN 'NA' ELSE COUNTRYCODE END AS COUNTRYCODE,
        CASE WHEN COUNTRY IS NULL THEN 'NA' ELSE COUNTRY END AS COUNTRY,
        CASE WHEN LINK IS NULL THEN 'NA' ELSE LINK END AS LINK,
        CASE WHEN CHANNEL IS NULL THEN 'NA' ELSE CHANNEL END AS CHANNEL,
        TO_VARCHAR(DATE_TRUNC('day', _airbyte_extracted_at), 'yyyy-MM-dd') as DATE_EXTRACTED,
        CURRENT_DATE() AS LAST_MODIFIED
    from {{ source('snf_staging','TCURC') }}
),
TCURC_FINAL AS (
    SELECT
        SHA2 (INDEX || STOREID, 256) as HASHID,
        INDEX AS INDEX,
        STOREID AS STORE_ID,
        STORENAME AS STORE_NAME,
        CITY AS CITY,
        ADDRESS AS ADDRESS,
        ZIP AS ZIP,
        COUNTRYCODE AS COUNTRY_CODE,
        COUNTRY AS COUNTRY,
        LINK AS LINK,
        CHANNEL AS CHANNEL,
        DATE_EXTRACTED as INSERT_DATE,
        LAST_MODIFIED
    FROM
        TCURC_NULLS

    {%if is_incremental()%}

    where INSERT_DATE >  (select max(INSERT_DATE) from {{this}})

    {%endif%}
)

{{ dbt_utils.deduplicate(
    relation='TCURC_FINAL',
    partition_by='INDEX,STORE_ID',
    order_by='LAST_MODIFIED desc',
   )
}}