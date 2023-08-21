with
final as (
    select  * from {{ source('AP_PRODUCT','WEBSCORE_LEAD_TEMP') }}
)

select * from final