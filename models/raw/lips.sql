{{
    config(
        materialized= 'incremental',
        unique_key = 'HASHID',
        incremental_strategy = 'merge'
    )
}}

with lips_nulls AS (
    select
        COALESCE(INDEX,-99) AS INDEX,
        COALESCE(POSNR,'NA') AS POSNR,
        COALESCE(VBELN,'NA') AS VBELN,
        VBDAT,
        COALESCE(FBL1N,'1900-01-01 00:00:00') AS FBL1N,
        COALESCE(SENDD,'1900-01-01 00:00:00') AS SENDD,
        COALESCE(OVXD,'1900-01-01') AS OVXD,
        COALESCE(ARRID,'1900-01-01 00:00:00') AS ARRID,
        TO_VARCHAR(DATE_TRUNC('day', _airbyte_extracted_at), 'yyyy-MM-dd') as DATE_EXTRACTED
    from
        {{source ('snf_staging','LIPS')}}
),
lips_dates as (
    select
        SHA2 (INDEX || POSNR, 256) as HASHID,
        INDEX,
        POSNR,
        VBELN,
        TO_VARCHAR(TO_DATE(VBDAT, 'MM-DD-YYYY'), 'yyyy-MM-dd') AS VBDAT,
        FBL1N,
        SENDD,
        to_date(to_varchar(OVXD::DATE, 'yyyy-mm-dd')) AS OVXD,
        ARRID,
        DATE_EXTRACTED
    from
        lips_nulls
),
lips_inc as (
    select
        HASHID,
        INDEX,
        POSNR,
        VBELN,
        COALESCE(VBDAT,'1900-01-01') AS VBDAT,
        FBL1N,
        SENDD,
        to_date(to_varchar(OVXD::DATE, 'yyyy-mm-dd')) AS OVXD,
        ARRID,
        DATE_EXTRACTED as INSERT_DATE,
        CURRENT_DATE() as LAST_MODIFIED
    from
        lips_dates
),
lips_final as (

    SELECT 
        * 
    FROM 
        lips_inc

{%if is_incremental()%}

    where INSERT_DATE >  (select max(INSERT_DATE) from {{this}})

{%endif%}
)


{{ dbt_utils.deduplicate(
    relation='lips_final',
    partition_by='INDEX,POSNR',
    order_by='ARRID desc,SENDD desc',
   )
}}

