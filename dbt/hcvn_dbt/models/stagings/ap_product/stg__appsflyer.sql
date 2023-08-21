with 
final as (
    select * from {{ source('AP_PRODUCT','MKT_APPSFLYER') }}
)

select * from final