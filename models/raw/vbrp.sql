{{
    config(
        materialized= 'incremental',
        unique_key = 'HASHID',
        incremental_strategy = 'merge'
    )
}}

with vbrp_initial as (
    select    
        coalesce(INDEX, -99) as INDEX,
        COALESCE(POSNR,'NA') as POSNR,
        COALESCE(VBELN,'NA') as VBELN,
        COALESCE(ORDDT,'1900-01-01') as ORDDT,
        COALESCE(MANDT,'NA') as MANDT,
        COALESCE(MATNR,'NA') as MATNR,
        COALESCE(STKPR,0) as STKPR,
        COALESCE(WAERS,'NA') as WAERS,
        COALESCE(FKIMG,0) as FKIMG,
        COALESCE(SKTOF,'NA') as SKTOF,
        COALESCE(KPEIN,0) as KPEIN,
        COALESCE(TIMESTAMP,'00:00:00') as TIMESTAMP,
        COALESCE(PERNR,'NA') as PERNR,
        COALESCE(BEZNR,'NA') as BEZNR,
        COALESCE(RSNR,'NA') as RSNR,
        TO_VARCHAR(DATE_TRUNC('day', _airbyte_extracted_at), 'yyyy-MM-dd') as DATE_EXTRACTED
    from {{source ('snf_staging','VBRP')}}
),
vbrp_final as (
    select
        SHA2 (INDEX || POSNR, 256) as HASHID,
        INDEX,
        POSNR,
        VBELN,
        to_date(to_varchar(ORDDT::date, 'yyyy-mm-dd')) as ORDDT,
        MANDT,
        MATNR,
        STKPR,
        WAERS,
        FKIMG,
        SKTOF,
        KPEIN,
        TIMESTAMP,
        PERNR,
        BEZNR,
        RSNR,
        DATE_EXTRACTED as INSERT_DATE,
        CURRENT_DATE() as LAST_MODIFIED
    from vbrp_initial
    {%if is_incremental()%}

    where INSERT_DATE >  (select max(INSERT_DATE) from {{this}})

    {%endif%}
)

{{ dbt_utils.deduplicate(
    relation='vbrp_final',
    partition_by='INDEX,POSNR',
    order_by='ORDDT desc,TIMESTAMP desc',
   )
}}