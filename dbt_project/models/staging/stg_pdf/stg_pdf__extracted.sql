{{ config(
    materialized='incremental',
    unique_key='pdf_filename',
    schema='staging',
    tags=['llm', 'extraction', 'cost_center']
) }}

with new_files as (
    select 
        *,
        length("raw_text") as raw_text_length
    from {{ source('centec_raw', 'CERTIFICATES_TEXT_BASE') }}
    
    {% if is_incremental() %}
        where "pdf_filename" not in (select "pdf_filename" from {{ this }})
    {% endif %}
    
    limit {{ var('cortex_batch_limit', 100) }}
),
/* DESIGN NOTE: 
   In production, the prompt logic should reside in a dbt macro or metadata table. 
   Stored here for audit transparency and portfolio readability.
*/

cortex_inference as (
    select 
        pdf_filename, 
        file_timestamp, 
        is_truncated,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            CONCAT(
                '### OUTPUT RULES:
                - Output ONLY the JSON object. 
                - No conversational text, no additional comments, no markdown blocks.
                - If a value is missing, use null.
                - CRITICAL: DO NOT include units or symbols (like °C, %, ±) in the output. ONLY the numerical value.
                - Use DOT (.) as decimal separator. REPLACE commas with dots.

                ### EXTRACTION SCHEMA:
                {
                    "certificate_id": "string (format NNNN/YY)",
                    "order_id": "string (extract the digits before the "/" from the certificate_id)",
                    "serial_number": "string",
                    "calibration_date": "string (DD/MM/YYYY)",
                    "raw_temperature": "string (nominal value ONLY, NO °C)",
                    "raw_temp_u": "string (uncertainty value ONLY, NO ±, NO °C)",
                    "raw_humidity": "string (nominal value ONLY, NO %)",
                    "raw_hum_u": "string (uncertainty value ONLY, NO ±, NO %)"
                }

                ### TEXT TO PROCESS:
                ', 
                LEFT("raw_text", 10000)
            )
        ) as llm_raw_response
    from new_files
)

select
    pdf_filename,
    file_timestamp,
    is_truncated,
    try_parse_json(regexp_substr(llm_raw_response, '\\{.*?\\}', 1, 1, 's')) as clean_json,
    llm_raw_response,
    current_timestamp() as _processed_at
from cortex_inference