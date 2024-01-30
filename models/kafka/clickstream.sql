{{
    config(
        materialized= 'table'
    )
}}

with clickstream as (
    select 
        record_content:agent
    from {{ source('snf_kafka','CLICKSTREAM') }}
)
SELECT * FROM clickstream