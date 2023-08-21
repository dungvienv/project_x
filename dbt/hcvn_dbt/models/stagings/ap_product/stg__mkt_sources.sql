with 
final as (
    select * from {{ source('AP_PRODUCT','IMP_ALL_MKT_SOURCE') }}
)

select * from final