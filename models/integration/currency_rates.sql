{{
    config(
        materialized= 'table'
    )
}}

with currency_rates_rn as (
SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        INDEX,
                        CRID
                    ORDER BY
                        LAST_MODIFIED DESC
                ) as rn
            FROM
                {{source ('snf_raw','VCUR')}}
),
currency_rates_final as (
    select
        SHA2 (INDEX || CRID, 256) as ID,
        CRID as CURRENCYRATE_ID,
        T247 as YEAR_MONTH,
        WAERS as CURRENCY_CODE,
        EXRT as CURRENCY_RATE,
        CURRENT_DATE() as LAST_MODIFIED
    from currency_rates_rn
    where rn=1
)
select * from currency_rates_final            