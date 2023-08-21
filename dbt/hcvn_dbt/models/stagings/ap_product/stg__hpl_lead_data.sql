with 
final as (
    select * from {{ source('AP_PRODUCT','HPL_LEAD_DATA') }}
)

select * from final