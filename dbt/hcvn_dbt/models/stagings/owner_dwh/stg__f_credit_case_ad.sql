with 
final as (
    select * from {{ source('OWNER_DWH','F_CREDIT_CASE_AD') }}
)
select * from final