{{
    config(
        materialized= 'table'
    )
}}

with payment_rn as (
    SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        PAYMENT_ID
                    ORDER BY
                        LAST_MODIFIED DESC
                ) as rn
    FROM
        {{source ('snf_raw','TVKOT')}}

),
payment_final as (
    select 
        SHA2 (PAYMENT_ID, 256) as ID,
        PAYMENT_ID,
        PAYMENT_METHOD,
        PAYMENT_TERMS,
        LAST_MODIFIED
    from
        payment_rn
)
select * from payment_final