{{
    config(
        materialized= 'table'
    )
}}

with vbak_1 as (
    select
        coalesce (index, -99) as index,
        coalesce (reid, 'NA') as reid,
        coalesce (vbeln, 'NA') as vbeln,
        coalesce (mandt, 'NA') as mandt,
        coalesce (matnr, 'NA') as matnr,
        coalesce (stkpr, 'NA') as stkpr,
        coalesce (waers, 'NA') as waers,
        coalesce (fkimg, 0) as fkimg,
        coalesce (sktof, 'NA') as sktof,
        coalesce (kpein, 0) as kpein,
        coalesce (timestamp, '00:00:00')
            as timestamp,
        coalesce (pernr, 'NA') as pernr,
        coalesce (beznr, 'NA') as beznr,
        coalesce (rsnr, 'NA') as rsnr,
        coalesce (retda, '1900-01-01') as retda,
        coalesce (recda, '1900-01-01') as recda
    from {{ source ('snf_staging','VBAK') }}
),

vbak_2 as (
    select
        index,
        reid,
        vbeln,
        mandt,
        matnr,
        stkpr,
        waers,
        fkimg,
        sktof,
        kpein,
        pernr,
        beznr,
        rsnr,
        retda,
        substring(timestamp, 0, len(timestamp) - 4) as timestamp,
        to_date(to_varchar(recda::date, 'yyyy-mm-dd')) as recda
    from vbak_1
),

vbak_final as (
    select
        index,
        reid,
        vbeln,
        mandt,
        matnr,
        stkpr,
        waers,
        fkimg,
        sktof,
        kpein,
        pernr,
        beznr,
        rsnr,
        recda,
        to_time(timestamp) as timestamp,
        to_date(retda) as retda
    from vbak_2
)

select * from vbak_final
