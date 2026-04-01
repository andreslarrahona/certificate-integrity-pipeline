{{ config(
    materialized='view',
    schema='staging',
    tags=['parsing', 'quality_triage']
) }}

with extracted_data as (
    select * from {{ ref('stg_pdf__extracted') }}
),

raw_parsing as (
    select 
        pdf_filename,
        file_timestamp,
        is_truncated,
        
        COALESCE(
            clean_json:certificate_id::string, 
            clean_json:certificate_nro::string,
            clean_json:id::string
        ) as raw_certificate_id,
        
        clean_json:serial_number::string as serial_number,
        try_to_date(clean_json:calibration_date::string, 'DD/MM/YYYY') as calibration_date,
        try_to_double(replace(clean_json:raw_temperature::string, ',', '.')) as nominal_temperature_c,
        try_to_double(replace(clean_json:raw_temp_u::string, ',', '.')) as temperature_uncertainty_c,
        try_to_double(replace(clean_json:raw_humidity::string, ',', '.')) as nominal_humidity_pct,
        try_to_double(replace(clean_json:raw_hum_u::string, ',', '.')) as humidity_uncertainty_pct,
        
        clean_json 
    from extracted_data
),

validation as (
    select
        *,
        regexp_like(raw_certificate_id, '^\\d{4}/\\d{2}$') as is_valid_format,
        (raw_certificate_id is not null) as has_id
    from raw_parsing
)

select 
    pdf_filename,
    file_timestamp,
    is_truncated,
    
    case when is_valid_format then raw_certificate_id else null end as certificate_id,
    case 
        when is_valid_format then REGEXP_SUBSTR(raw_certificate_id, '^(\\d{4})', 1, 1, 'e') 
        else null 
    end as order_id,
    
    serial_number,
    calibration_date,
    nominal_temperature_c,
    temperature_uncertainty_c,
    nominal_humidity_pct,
    humidity_uncertainty_pct,

    case 
        when clean_json is null then 'ERR_JSON_NULL'
        when not has_id then 'ERR_MISSING_ID'
        when not is_valid_format then 'ERR_INVALID_FORMAT'
        when is_truncated then 'WARN_TRUNCATED'
        else 'OK'
    end as ai_status

from validation