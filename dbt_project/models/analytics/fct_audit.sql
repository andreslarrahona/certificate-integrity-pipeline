{{ config(
    materialized='table',
    schema='analytics',
    tags=['compliance', 'iso17025', 'audit']
) }}

with valid_certificates as (
    select * from {{ ref('stg_pdf__parsed') }}
    where ai_status = 'OK'
),

orders as (
    select * from {{ ref('stg_orders') }}
),

instruments as (
    select * from {{ ref('dim_instruments') }}
),

user_milestones as (
    select * from {{ ref('int_orders_pivoted') }}
),

temp_limits as (
    -- Structural fix: Aggregation guarantees exactly 1 row, preventing CROSS JOIN fan-out.
    select 
        max(lower_limit) as lower_limit, 
        max(upper_limit) as upper_limit 
    from {{ ref('ambiental_limits') }} 
    where magnitude = 'temperature' and procedure_version = 'v00'
),

humidity_limits as (
    select 
        max(lower_limit) as lower_limit, 
        max(upper_limit) as upper_limit 
    from {{ ref('ambiental_limits') }} 
    where magnitude = 'humidity' and procedure_version = 'v00'
),

historical_uncertainties as (
    select * from {{ ref('ambiental_u') }}
)

select
    -- Primary Identifiers
    c.order_id,
    c.pdf_filename,
    i.instrument_brand,
    i.instrument_model,

    -- 1. ISO 17025: Duty Segregation
    case 
        when h.user_calibrated = h.user_approved then false
        when h.user_calibrated is null or h.user_approved is null then null
        else true
    end as is_duty_segregation_compliant,

    -- 2. Internal Process: Missing Entry Note
    case 
        when o.entry_note_id is null and o.is_onsite_calibration = false then true
        else false
    end as has_missing_entry_note,

    -- 3. Consistency: Calibration Date
    case 
        when c.calibration_date = o.calibrated_at::date then true
        else false
    end as is_date_synchronized,

    -- 4. Consistency: Serial Number
    case 
        when trim(lower(c.serial_number)) = trim(lower(i.serial_number)) then true
        else false
    end as is_serial_synchronized,

    -- 5. Procedure: Environmental Limits
    case 
        when c.nominal_temperature_c between t.lower_limit and t.upper_limit then true
        else false
    end as is_temp_within_limits,

    case 
        when c.nominal_humidity_pct between hum.lower_limit and hum.upper_limit then true
        else false
    end as is_humidity_within_limits,

    -- 6. Technical Validity: Historical Temperature Uncertainty
    case 
        when c.temperature_uncertainty_c is null then false -- ISO 17025 violation: missing uncertainty
        when u.min_temp_u is null then null -- Master data missing for this date
        when c.temperature_uncertainty_c >= u.min_temp_u then true
        else false
    end as is_temp_uncertainty_valid,

    -- 7. Technical Validity: Historical Humidity Uncertainty
    case 
        when c.humidity_uncertainty_pct is null then false -- ISO 17025 violation: missing uncertainty
        when u.min_hum_u is null then null -- Master data missing for this date
        when c.humidity_uncertainty_pct >= u.min_hum_u then true
        else false
    end as is_hum_uncertainty_valid

from valid_certificates as c
left join orders as o 
    on o.order_id = c.order_id
left join instruments as i 
    on i.instrument_id = o.instrument_id
left join user_milestones as h 
    on h.order_id = c.order_id
cross join temp_limits as t
cross join humidity_limits as hum
-- SCD Type 2 Join for Historical Uncertainty Limits
left join historical_uncertainties as u
    on c.calibration_date >= u.valid_from::date
    and c.calibration_date <= u.valid_to::date