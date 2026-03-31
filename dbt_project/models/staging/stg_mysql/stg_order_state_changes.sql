{{ config(
    materialized='view',
    schema='staging',
    tags=['core', 'oltp_sync', 'audit_trail']
) }}

with source as (
    select * from {{ source('centec_raw', 'cambios_estados_ordenes') }}
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
        id::string as state_change_id,
        id_orden::string as order_id,
        id_estado::string as state_id,
        id_usuario::string as user_id,

        -- Timestamps
        created_at::timestamp as changed_at,

        -- Dimensions & Flags
        motivo as change_reason,
        
        -- Explicit boolean evaluation for Snowflake
        (notificado = 1) as is_pickup_notified,
        (notificado_cc = 1) as is_certificate_notified
        
    from deduplicated 
    where rn = 1
)

select * from renamed_and_casted