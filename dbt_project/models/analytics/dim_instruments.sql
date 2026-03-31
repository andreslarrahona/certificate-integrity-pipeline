{{ config(
    materialized='view',
    schema='analytics',
    tags=['core', 'master_data', 'lean']
) }}

with customer_instruments as (
    select * from {{ ref('stg_customer_instruments')}}
    where customer_id <> '124'
),

models as (
    select * from {{ ref('stg_models')}}
),

brands as (
    select * from {{ ref('stg_brands')}}
)

select
    ic.instrument_id,
    ic.serial_number,
    m.instrument_brand,
    mo.instrument_model

from customer_instruments as ic
left join models as mo 
    on mo.model_id = ic.model_id
left join brands as m 
    on mo.brand_id = m.brand_id