{{
    config(
        materialized= 'table'
    )
}}

with product_rn as (
    SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        INDEX,
                        MATNR
                    ORDER BY
                        LAST_MODIFIED DESC
                ) as rn
    from {{source ('snf_raw','MAKT')}}
),
product_final as(
    select
        SHA2 (INDEX || MATNR, 256) as ID,
        MAKTX as PRODUCT_NAME,
        BEZE1 AS PRODUCT_CATEGORY,
        BEZE2 AS PRODUCT_SUBCATEGORY,
        PRDFB AS PRODUCT_COLOR,
        STKPR AS UNIT_PRICE,
        DATEF AS DATE_INTORDUCED,
        PRDGR AS PRODUCT_SIZE
    from product_rn
    where rn=1
)
select * from product_final
