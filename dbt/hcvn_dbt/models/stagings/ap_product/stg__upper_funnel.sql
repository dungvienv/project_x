with 
final as (
    select * from {{ source('AP_PRODUCT','MKT_UPPER_FUNNEL') }}
)

select * from final