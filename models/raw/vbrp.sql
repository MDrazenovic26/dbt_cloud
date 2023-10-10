{{
    config(
        materialized= 'table'
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
        COALESCE(RSNR,'NA') as RSNR
    from {{source ('snf_staging','VBRP')}}
),
vbrp_final as (
    select
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
        RSNR
    from vbrp_initial
)
SELECT * FROM vbrp_final