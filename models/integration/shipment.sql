{{
    config(
        materialized='incremental',
        unique_key='ID',
        incremental_strategy='merge'
    )
}}

with shipment_original as (
    select
        HASHID,
	    POSNR,
	    VBELN,
	    VBDAT,
	    FBL1N,
	    SENDD,
	    OVXD,
	    ARRID,
        INSERT_DATE,
        LAST_MODIFIED
    from
        {{source('snf_raw','LIPS')}}

    {% if is_incremental() %}
        where INSERT_DATE > (select max(INSERT_DATE) from {{this}})
    {% endif %}        
),
shipment_updates as (
    select
        HASHID,
	    POSNR,
	    VBELN,
	    VBDAT,
	    FBL1N,
	    SENDD,
	    OVXD,
	    ARRID,
        INSERT_DATE,
        LAST_MODIFIED
    from
        shipment_original

    {% if is_incremental() %}
        where HASHID IN (SELECT HASHIDID FROM {{this}})
    {% endif %}
),
shipment_inserts as (
    select
        HASHID,
	    POSNR,
	    VBELN,
	    VBDAT,
	    FBL1N,
	    SENDD,
	    OVXD,
	    ARRID,
        INSERT_DATE,
        LAST_MODIFIED
    from
        shipment_original
    where HASHID NOT IN (select HASHID from shipment_updates)
),
shipment_union as(
select * from shipment_updates 
UNION 
select * from shipment_inserts
),
shipment_final as(
select 
    HASHID as ID,
	POSNR AS SHIPMENT_ID,
	VBELN AS ORDER_ID,
	VBDAT AS ORDER_DATE ,
	FBL1N AS TRANSACTION_DATE ,
	SENDD AS SHIP_DATE ,
	OVXD AS SHIPPING_POINT_DATE ,
	ARRID AS ARRIVAL_DATE,
    INSERT_DATE,
    LAST_MODIFIED
from shipment_union
)
select * from shipment_final