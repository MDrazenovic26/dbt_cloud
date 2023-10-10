{{
    config(
        materialized= 'table'
    )
}}

WITH PRODUCT_FINAL AS (
    select 
        ID, 
        PRODUCT_ID, 
        PRODUCT_NAME, 
        PRODUCT_CATEGORY, 
        PRODUCT_SUBCATEGORY, 
        PRODUCT_COLOR, 
        UNIT_PRICE, 
        DATE_INTORDUCED, 
        PRODUCT_SIZE, 
        CURRENT_DATE() AS LAST_MODIFIED
    from {{source('snf_integration','PRODUCT')}}
)
SELECT * FROM PRODUCT_FINAL