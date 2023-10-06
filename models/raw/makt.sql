{{
    config(
        materialized= 'table'
    )
}}

with makt as (
    select  
        CASE WHEN INDEX IS NULL THEN -99 ELSE INDEX END  as INDEX, 
        CASE WHEN MATNR IS NULL THEN 'NA' ELSE MATNR END as MATNR,
        case when MAKTX is null then 'NA' ELSE MAKTX END as MAKTX, 
        CASE WHEN BEZE1 IS NULL THEN 'NA' ELSE BEZE1 END as BEZE1, 
        CASE WHEN BEZE2 IS NULL THEN 'NA' ELSE BEZE2 END as BEZE2, 
        CASE WHEN PRDFB IS NULL THEN 'NA' ELSE PRDFB END as PRDFB, 
        CASE WHEN STKPR IS NULL THEN 0 ELSE STKPR END AS STKPR, 
        CASE WHEN DATEF IS NULL THEN '1900-01-01' ELSE TO_DATE(DATEF) END AS DATEF, 
        CASE WHEN PRDGR IS NULL THEN -99 ELSE PRDGR END AS PRDGR, 
        CURRENT_DATE() AS LAST_MODIFIED
    from {{ source('snf_staging','MAKT')}}
)
    select * from makt