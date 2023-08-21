with 
final as (
    select * from {{ source('OWNER_DWH','DC_CLIENT') }}
)

select * from final