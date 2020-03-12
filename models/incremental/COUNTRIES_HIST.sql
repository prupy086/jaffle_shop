{{
    config(
        materialized='incremental'
    )
}}

select
    *,
    my_slow_function(my_column)
    
from raw_app_data.events

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}