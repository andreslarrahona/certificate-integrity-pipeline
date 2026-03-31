{{ config(
    materialized='view',
    schema='staging',
    tags=['core', 'master_data']
) }}

with source as (
    select * from {{ source('centec_raw', 'modelos') }}
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
        id::string as model_id,
        id_marca::string as brand_id,
        id_tipo_instrumento::string as instrument_type_id,
        
        -- Attributes
        nombre::string as instrument_model
    from deduplicated 
    where rn = 1
)

select * from renamed_and_casted