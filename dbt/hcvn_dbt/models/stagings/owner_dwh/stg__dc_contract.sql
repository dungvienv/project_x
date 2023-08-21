with 
final as (
    select * from {{ source('OWNER_DWH','DC_CONTRACT') }}
)
select * from final