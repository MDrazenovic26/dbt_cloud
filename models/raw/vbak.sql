{{
    config(
        materialized= 'table'
    )
}}

with vbak_1 as (
    select  
        CASE WHEN INDEX IS NULL THEN -99 ELSE INDEX END INDEX,
        CASE WHEN REID IS NULL THEN 'NA' ELSE REID END REID,
        CASE WHEN VBELN IS NULL THEN 'NA' ELSE VBELN END VBELN,
        CASE WHEN MANDT IS NULL THEN 'NA' ELSE MANDT END MANDT,
        CASE WHEN MATNR IS NULL THEN 'NA' ELSE MATNR END MATNR,
        CASE WHEN STKPR IS NULL THEN 'NA' ELSE STKPR END STKPR,
        CASE WHEN WAERS IS NULL THEN 'NA' ELSE WAERS END WAERS,
        CASE WHEN FKIMG IS NULL THEN 0 ELSE FKIMG END FKIMG,
        CASE WHEN SKTOF IS NULL THEN 'NA' ELSE SKTOF END SKTOF,
        CASE WHEN KPEIN IS NULL THEN 0 ELSE KPEIN END KPEIN,
        CASE WHEN TIMESTAMP IS NULL THEN '00:00:00' ELSE TIMESTAMP END TIMESTAMP,
        CASE WHEN PERNR IS NULL THEN 'NA' ELSE PERNR END PERNR,
        CASE WHEN BEZNR IS NULL THEN 'NA' ELSE BEZNR END BEZNR,
        CASE WHEN RSNR IS NULL THEN 'NA' ELSE RSNR END RSNR,
        CASE WHEN RETDA IS NULL THEN '1900-01-01' ELSE RETDA END RETDA,
        CASE WHEN RECDA IS NULL THEN '1900-01-01' ELSE RECDA END RECDA
    from {{source ('snf_staging','VBAK')}}
),
vbak_2 as(
    select
        INDEX,
        REID,
        VBELN,
        MANDT,
        MATNR,
        STKPR,
        WAERS,
        FKIMG,
        SKTOF,
        KPEIN,
        substring(TIMESTAMP,0,len(TIMESTAMP)-4) as TIMESTAMP,
        PERNR,
        BEZNR,
        RSNR,
        RETDA,
        to_date(to_varchar(RECDA::date,'yyyy-mm-dd')) as RECDA
    from vbak_1
),
vbak_final as (
    select
        INDEX,
        REID,
        VBELN,
        MANDT,
        MATNR,
        STKPR,
        WAERS,
        FKIMG,
        SKTOF,
        KPEIN,
        to_time(TIMESTAMP) as TIMESTAMP,
        PERNR,
        BEZNR,
        RSNR,
        to_date(RETDA) as RETDA,
        RECDA
    from vbak_2
)
select * from vbak_final