with 
final as (
    select * from {{ source('OWNER_DWH','DC_CREDIT_CASE') }}
)

select * from final