{{
    config(
        materialized= 'table'
    )
}}

with tvkot as (
    select  
        CASE WHEN PAYMENTID IS NULL THEN 'NA' ELSE PAYMENTID END AS PAYMENT_ID,
        CASE WHEN PAYMENTMETHOD IS NULL THEN 'NA' ELSE PAYMENTMETHOD END AS PAYMENT_METHOD,
        CASE WHEN PAYMENTTERMS IS NULL THEN 'NA' ELSE PAYMENTTERMS END AS PAYMENT_TERMS,
        CURRENT_DATE() AS LAST_MODIFIED
    from {{source("snf_staging",'TVKOT')}}
)
SELECT * FROM tvkot