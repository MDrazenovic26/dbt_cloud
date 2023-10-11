{{
    config(
        materialized= 'table'
    )
}}

WITH PAYMENT_FINAL AS (
    select 
        ID,
        PAYMENT_ID,
        PAYMENT_METHOD,
        PAYMENT_TERMS,
        CURRENT_DATE() AS LAST_MODIFIED
    from {{source('snf_integration','PAYMENT')}}
)
SELECT * FROM PAYMENT_FINAL