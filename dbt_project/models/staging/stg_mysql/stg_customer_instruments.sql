{{ config(
    materialized='view',
    schema='staging',
    tags=['core', 'master_data']
) }}

with source as (
    select * from {{ source('centec_raw', 'instrumentos_clientes') }}
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
        id::string as instrument_id,
        id_cliente::string as customer_id,
        id_modelo::string as model_id,
        
        -- Attributes
        nro_serie::string as serial_number,
        id_interno_empresa::string as internal_id
    from deduplicated 
    where rn = 1
)

select * from renamed_and_casted