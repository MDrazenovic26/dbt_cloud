{{
    config(
        materialized= 'table'
    )
}}

with discount_rn as (
    SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        INDEX,
                        SKTOF
                    ORDER BY
                        LAST_MODIFIED DESC
                ) as rn
            FROM
                {{ source ('snf_raw','KONP')}}
),
discount_final as (
    select
        SHA2 (INDEX || SKTOF, 256) as ID,
        SKTOF as DISCOUNT_ID , 
        KNUMA as PROMOTION,
        KONWA as DISCOUNT_TYPE,
        KSCHL as DISCOUNT_ON,
        KSLVL as DISCOUNT_LEVEL,
        KSTBW as DISCOUNT,
        BEGDA as START_DATE,
        ENDDA as END_DATE,
        VAFR as VALID_FROM,
        VATO as VALID_TO,
        CURRENT_DATE() as LAST_MODIFIED
    from 
        discount_rn
    where rn=1
)
select * from discount_final