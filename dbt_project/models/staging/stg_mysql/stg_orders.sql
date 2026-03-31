{{ config(
    materialized='view',
    schema='staging',
    tags=['core', 'oltp_sync']
) }}

with source as (
    -- Referencing the source defined in sources.yml
    select * from {{ source('centec_raw', 'ordenes') }}
),

deduplicated as (
    select 
        *,
        -- Keeping the latest record per ID based on ingestion timestamp
        row_number() over (
            partition by id 
            order by _ingested_at desc
        ) as rn
    from source
),

renamed_and_casted as (
    select
        -- PKs and FKs (Casted to string for consistent joining)
        id::string as order_id,
        id_instrumento_cliente::string as instrument_id,
        remito_in::string as entry_note_id,
        remito_out::string as delivery_note_id,
        id_ultimo_estado::string as last_state_id,

        -- Dimensions / Business Logic
        case prioridad::int
            when 1 then 'High'
            when 2 then 'Urgent'
            else 'Normal'
        end as priority_level,
        
        -- Boolean Standardization
        case 
            when lower(trim(calibracion_in_situ)) = 'si' then true
            when lower(trim(calibracion_in_situ)) = 'no' then false
            else false
        end as is_onsite_calibration,

        -- Explicit Timestamp/Date Casting
        created_at::timestamp as created_at,
        fecha_ingreso::timestamp as entered_at,
        fecha_calibracion::timestamp as calibrated_at,
        fecha_aprobado::timestamp as approved_at,
        fecha_certificado::timestamp as certified_at,
        fecha_entrega::timestamp as delivered_at,
        fechapactada::date as promised_date
        
    from deduplicated 
    where rn = 1
)

select * from renamed_and_casted