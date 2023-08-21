with 
final as (
    select * from {{ source('OWNER_DWH','DC_LDP_CUSTOMER') }}
)

select * from final 
where DATE_EUPDATED_AT >= sysdate-90