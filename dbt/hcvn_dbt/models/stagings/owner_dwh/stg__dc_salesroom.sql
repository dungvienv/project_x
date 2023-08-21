with 
final as (
    select * from {{ source('OWNER_DWH','DC_SALESROOM') }}
)

select * from final