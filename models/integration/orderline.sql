{{
    config(
        materialized= 'table',
        unique_key='ID',
        incremental_strategy='merge'
    )
}}

with orderline_original as (
    select 
        HASHID,
        INDEX, 
	    POSNR,
	    VBELN,
	    ORDDT, 
	    MANDT,
	    MATNR,
	    STKPR, 
	    WAERS,
	    FKIMG, 
	    SKTOF,
	    KPEIN, 
	    TIMESTAMP,
	    PERNR,
	    BEZNR,
	    RSNR,
        INSERT_DATE,
        LAST_MODIFIED
    FROM
        {{ source('snf_raw', 'VBRP') }}

    {% if is_incremental() %}
        where INSERT_DATE > (select max(INSERT_DATE) from {{this}}) 
    {%endif%}
),
ORDERLINE_UPDATES AS (
    SELECT
        HASHID,
        INDEX,  
	    POSNR,
	    VBELN,
	    ORDDT, 
	    MANDT,
	    MATNR,
	    STKPR, 
	    WAERS,
	    FKIMG, 
	    SKTOF,
	    KPEIN, 
	    TIMESTAMP,
	    PERNR,
	    BEZNR,
	    RSNR,
        INSERT_DATE,
        LAST_MODIFIED
    FROM 
        orderline_original
    
    {% if is_incremental() %}
        where HASHID IN (SELECT ID FROM {{this}})
    {% endif %}
),
ORDERLINE_INSERTS AS (
    SELECT
        HASHID,
        INDEX,  
	    POSNR,
	    VBELN,
	    ORDDT, 
	    MANDT,
	    MATNR,
	    STKPR, 
	    WAERS,
	    FKIMG, 
	    SKTOF,
	    KPEIN, 
	    TIMESTAMP,
	    PERNR,
	    BEZNR,
	    RSNR,
        INSERT_DATE,
        LAST_MODIFIED
    FROM 
        orderline_original
    
    {% if is_incremental() %}
        where HASHID NOT IN (SELECT HASHID FROM ORDERLINE_UPDATES)
    {% endif %}
),
ORDERLINE_UNION AS (
    SELECT * FROM ORDERLINE_UPDATES
    UNION
    SELECT * FROM ORDERLINE_INSERTS
),
ORDERLINE_FINAL AS (
    SELECT
        HASHID AS ID,
	    POSNR AS ORDER_LINE_ID,
	    VBELN AS ORDER_ID,
	    ORDDT AS ORDER_DATE, 
	    MANDT AS CUSTOMER_ID,
	    MATNR AS PRODUCT_ID,
	    STKPR AS UNIT_PRICE, 
	    WAERS AS CURRENCY_CODE,
	    FKIMG AS QUANTITY, 
	    SKTOF AS DISCOUNT_ID,
	    KPEIN AS NET_PRICE, 
	    TIMESTAMP AS TIMESTAMP,
	    PERNR AS EMPLOYEE_ID,
	    BEZNR AS PAYMENT_ID,
	    RSNR AS STORE_ID,
        CURRENT_DATE() AS INSERT_DATE,
        CURRENT_DATE() AS LAST_MODIFIED
    FROM 
        ORDERLINE_UNION
)
SELECT * FROM ORDERLINE_FINAL

