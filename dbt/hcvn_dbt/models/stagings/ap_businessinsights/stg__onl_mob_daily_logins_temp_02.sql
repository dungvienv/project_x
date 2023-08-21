with 
final as (
    select * from {{ source('AP_BUSINESSINSIGHTS','ONL_MOB_DAILY_LOGINS_TEMP_02') }}
)

select * from final