with 
final as (
    select * from {{ source('OWNER_DWH','DC_HCO_CONTRACTS') }}
)
select * from final