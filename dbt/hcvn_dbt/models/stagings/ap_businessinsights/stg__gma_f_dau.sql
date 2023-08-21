with 
final as (
    select * from {{ source('AP_BUSINESSINSIGHTS','GMA_F_DAU') }}
)

select * from final