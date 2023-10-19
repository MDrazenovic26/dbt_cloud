{{
    config(
        materialized= 'table',
        unique_key='ID',
        incremental_strategy='merge'
    )
}}

WITH STORE_ORIGINAL AS (
    SELECT
        HASHID,
        STORE_ID,
        STORE_NAME,
        CITY,
        ADDRESS,
        ZIP,
        COUNTRY_CODE,
        COUNTRY,
        LINK,
        CHANNEL,
        INSERT_DATE,
        LAST_MODIFIED
    FROM
        {{source('snf_raw','TCURC')}}
    
    {% if is_incremental() %}
        where INSERT_DATE > (select max(INSERT_DATE) from {{this}}) 
    {%endif%}
),
STORE_UPDATES AS(
    SELECT
        HASHID,
        STORE_ID,
        STORE_NAME,
        CITY,
        ADDRESS,
        ZIP,
        COUNTRY_CODE,
        COUNTRY,
        LINK,
        CHANNEL,
        INSERT_DATE,
        LAST_MODIFIED
    FROM
        STORE_ORIGINAL
    {% if is_incremental() %}
        where HASHID IN (SELECT ID FROM {{this}})
    {%endif%}
),
STORE_INSERTS AS(
    SELECT
        HASHID,
        STORE_ID,
        STORE_NAME,
        CITY,
        ADDRESS,
        ZIP,
        COUNTRY_CODE,
        COUNTRY,
        LINK,
        CHANNEL,
        INSERT_DATE,
        LAST_MODIFIED
    FROM
        STORE_ORIGINAL
    {% if is_incremental() %}
        where HASHID NOT IN (SELECT HASHID FROM STORE_UPDATES)
    {%endif%}
),
STORE_UNION AS (
    SELECT * FROM STORE_UPDATES
        UNION
    SELECT * FROM STORE_INSERTS
),
STORE_FINAL AS (
    SELECT
        HASHID AS ID,
        STORE_ID,
        STORE_NAME,
        CITY,
        ADDRESS,
        ZIP,
        COUNTRY_CODE,
        COUNTRY,
        LINK,
        CHANNEL,
        CURRENT_DATE() AS INSERT_DATE,
        CURRENT_DATE() AS LAST_MODIFIED
    FROM
        STORE_UNION
)
SELECT * FROM STORE_FINAL
