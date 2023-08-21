with 
final as (
    select * from {{ source('OWNER_DWH','DC_EMPLOYEE') }}
)
select * from final