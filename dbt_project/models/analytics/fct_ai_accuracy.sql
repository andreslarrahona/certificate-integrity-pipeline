{{ config(
    materialized='table',
    schema='analytics',
    tags=['mlops', 'validation']
) }}

with ground_truth as (
    select 
        pdf_filename,
        true_serial_number,
        true_temperature,
        true_humidity
    from {{ ref('golden_dataset') }}
),

ai_predictions as (
    select * from {{ ref('stg_pdf__parsed') }}
    where ai_status = 'OK'
),

evaluation_matrix as (
    select
        gt.pdf_filename,
        
        -- Serial Number Evaluation
        gt.true_serial_number,
        ai.serial_number as predicted_serial_number,
        coalesce(trim(lower(gt.true_serial_number)) = trim(lower(ai.serial_number)), false) as is_serial_match,

        -- Temperature Evaluation (Numerical Tolerance 0.1)
        gt.true_temperature,
        ai.nominal_temperature_c as predicted_temperature,
        coalesce(abs(gt.true_temperature - ai.nominal_temperature_c) <= 0.1, false) as is_temperature_match,

        -- Humidity Evaluation (Numerical Tolerance 0.1)
        gt.true_humidity,
        ai.nominal_humidity_pct as predicted_humidity,
        coalesce(abs(gt.true_humidity - ai.nominal_humidity_pct) <= 0.1, false) as is_humidity_match

    from ground_truth as gt
    left join ai_predictions as ai 
        on gt.pdf_filename = ai.pdf_filename
)

select * from evaluation_matrix