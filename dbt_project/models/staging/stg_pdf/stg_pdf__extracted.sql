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
        "pdf_filename", 
        "file_date" as file_timestamp, 
        raw_text_length > 10000 as is_truncated,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-8b',
            CONCAT(
                '### ROLE: Expert in ISO 17025 metrology data extraction.
                ### TASK: Generate a pure JSON object with structured data from the attached text.

                ### FIELD RULES:
                1. certificate_id: Look for "Certificado n°:", "CERTIFICADO N°:", "Informe de Referencia n:", "CERTIFICADO DE CALIBRACION N°:", "CERTIFICADO DE VERIFICACIÓN N°:" or similar. 
                   MUST follow the NNNN/YY format (e.g., 4341/23). If it does not meet this format, use null.
                2. order_id: Extract the first 4 digits of the certificate_id (e.g., If the cert is 4341/23, the order is 4341).
                3. serial_number: Look for "n° de identificación:", "SN.:", "N° de serie:", "S/N:" or similar.
                4. calibration_date: Prioritize "Fecha de calibración/verificación/ensayos". IGNORE the certificate issue date. Format: DD/MM/YYYY.
                5. nominal_temperature_c / nominal_humidity_pct: Base numerical value. Identify Temp by "°C" and Humidity by "%".
                6. temperature_uncertainty_c / humidity_uncertainty_pct: Value after the ± symbol or the word "incertidumbre" (uncertainty). 

                ### INTEGRITY RULES:
                - If a data point does not exist, use null (without quotes).
                - If there is a range (e.g., "20 a 25 °C"), use the first as nominal and null for uncertainty.
                - IMPORTANT: All numbers must use a DOT as a decimal separator. REPLACE commas with dots.
                - Do not include units (°C, %), only the float number or null.

                ### OUTPUT FORMAT (STRICT):
                Only return the JSON, without code blocks, without greetings or comments.
                {
                    "certificate_id": "string",
                    "order_id": "string",
                    "serial_number": "string",
                    "calibration_date": "string",
                    "nominal_temperature_c": float,
                    "nominal_humidity_pct": float,
                    "temperature_uncertainty_c": float,
                    "humidity_uncertainty_pct": float
                }

                ### TEXT TO PROCESS:
                ', 
                LEFT("raw_text", 10000)
            )
        ) as llm_raw_response
    from new_files
)
select
    "pdf_filename" as pdf_filename,
    file_timestamp,
    is_truncated,
    TRY_PARSE_JSON(
        REGEXP_REPLACE(
            REGEXP_SUBSTR(llm_raw_response, '\\{[\\s\\S]*?\\}', 1, 1, 's'),
            '^```json|```$', 
            ''
        )
    ) AS clean_json,
    llm_raw_response,
    CURRENT_TIMESTAMP() as _processed_at
from cortex_inference