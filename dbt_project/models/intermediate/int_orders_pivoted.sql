{{ config(
    materialized='table',
    schema='intermediate',
    tags=['transformation', 'milestones']
) }}

with state_changes as (
    select 
        order_id, 
        state_id, 
        changed_at, 
        user_id 
    from {{ ref('stg_order_state_changes') }}
),

ranked_changes as (
    select 
        *,
        row_number() over (partition by order_id, state_id order by changed_at asc) as rn_asc,
        row_number() over (partition by order_id, state_id order by changed_at desc) as rn_desc
    from state_changes
),

milestones as (
    select 
        order_id,
        
        -- TIMESTAMPS
        -- Hardcoded transaction state mapping: 
        -- 1: Entered, 2: In Process, 3: Calibrated, 4: Emitted/Notified, 5: Uploaded, 6: Canceled, 8: Approved
        min(case when state_id = '1' then changed_at end) as ts_entered,
        min(case when state_id = '2' then changed_at end) as ts_in_process,
        min(case when state_id = '3' then changed_at end) as ts_calibrated,
        max(case when state_id = '4' then changed_at end) as ts_emitted,
        max(case when state_id = '8' then changed_at end) as ts_approved,
        min(case when state_id = '5' then changed_at end) as ts_uploaded,
        min(case when state_id = '6' then changed_at end) as ts_canceled,
        
        -- USERS
        -- Pivot aggregation hack: extracts the specific user for the first/last time a state was reached
        min(case when state_id = '1' then user_id end) as user_entered,
        min(case when state_id = '2' then user_id end) as user_in_process,
        max(case when state_id = '3' and rn_asc = 1 then user_id end) as user_calibrated, 
        max(case when state_id = '4' and rn_desc = 1 then user_id end) as user_emitted,
        max(case when state_id = '8' and rn_desc = 1 then user_id end) as user_approved,
        max(case when state_id = '5' and rn_asc = 1 then user_id end) as user_uploaded
        
    from ranked_changes
    group by 1
)

select
    *,
    -- Calculating fractional days using seconds to preserve precision and avoid rounding errors
    datediff('second', ts_entered, ts_in_process) / 86400.0 as days_entered,
    datediff('second', ts_in_process, ts_calibrated) / 86400.0 as days_in_process,
    datediff('second', ts_calibrated, ts_emitted) / 86400.0 as days_to_emit,
    datediff('second', ts_emitted, ts_approved) / 86400.0 as days_to_approve,
    datediff('second', ts_approved, ts_uploaded) / 86400.0 as days_to_upload,
    datediff('second', ts_calibrated, ts_uploaded) / 86400.0 as days_certificate_delay
from milestones