{{
    config(
        materialized= 'table'
    )
}}

with vcur as(
    select 
    case when index is null then '-99' else index end as index
    ,case when CRID is null then 'NN' else CRID END AS CRID
    ,case when T247 is null then 'NN' else T247 END AS T247
    ,case when WAERS is null then 'NN' else WAERS END AS WAERS
    ,EXRT
    ,CURRENT_DATE() AS LAST_MODIFIED
from {{ source('snf_staging','VCUR') }}
)
select * from vcur