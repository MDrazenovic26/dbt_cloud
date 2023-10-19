{{
    config(
        materialized= 'view'
    )
}}

{% set currency_codes = ["GBP", "EUR", "SEK","PLN","NA"] %}

    SELECT 
    TOP 10000
    FI.INVOICE_DATE,
    C.FIRST_NAME,
    C.LAST_NAME,
    S.STORE_NAME,
    P.PAYMENT_METHOD,
    FI.CURRENCY_CODE,
    SUM( 
        case 
    {%for currency_code in currency_codes%}
            when FI.CURRENCY_CODE='{{currency_code}}' then FI.INVOICE_AMOUNT*UNIFORM(1,9,RANDOM()) 
    {%- if not loop.last %}{% endif -%}
    
    {% endfor %}
        else FI.INVOICE_AMOUNT
        end
    ) as INVOICE_AMOUNT
FROM 
    INTEGRATION.INVOICE FI
INNER JOIN
    INTEGRATION.CUSTOMER C
ON
    FI.CUSTOMER_ID=C.CUSTOMER_ID
INNER JOIN
    {{source('snf_integration','STORE')}} S
ON
    FI.STORE_ID=S.STORE_ID
INNER JOIN
    {{ref('dim_payment')}} P
ON
    FI.PAYMENT_ID=P.PAYMENT_ID
WHERE
    FI.INVOICE_DATE >= '2015-01-01'
GROUP BY 
    FI.INVOICE_DATE,
    C.FIRST_NAME,
    C.LAST_NAME,
    S.STORE_NAME,
    P.PAYMENT_METHOD,
    FI.CURRENCY_CODE


