with 
final as (
    select * from {{ source('AP_PRODUCT','CCX_LEAD_DATA') }}
)

select * from final