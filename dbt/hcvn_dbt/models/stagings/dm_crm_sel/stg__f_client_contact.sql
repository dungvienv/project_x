with 
final as (
    select * from {{ source('DM_CRM_SEL','F_CLIENT_CONTACT') }}
)

select * from final