{{ config(
    materialized='view',
    schema='staging',
    tags=['core', 'master_data']
) }}

with source as (
    select * from {{ source('centec_raw', 'marcas') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by id order by _ingested_at desc) as rn
    from source
),

renamed_and_casted as (
    select
        -- Keys
        id::string as brand_id,
        
        -- Attributes
        nombre::string as instrument_brand
    from deduplicated 
    where rn = 1
)

select * from renamed_and_casted