with 
final as (
    select * from {{ source('OWNER_DWH','DC_SELLER') }}
)
select * from final 