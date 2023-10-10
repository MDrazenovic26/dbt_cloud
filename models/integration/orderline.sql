{{
    config(
        materialized= 'table'
    )
}}

with vbrp as (
    select *
    from {{ source('snf_raw', 'VBRP') }}
)

{{ dbt_utils.deduplicate(
    relation='vbrp',
    partition_by='INDEX, POSNR',
    order_by='ORDDT desc,timestamp desc',
   )
}}