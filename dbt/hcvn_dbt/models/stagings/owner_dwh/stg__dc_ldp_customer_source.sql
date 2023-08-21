with 
final as (
    select * from {{ source('OWNER_DWH','DC_LDP_CUSTOMER_SOURCE') }}
)

select * from final 
where DTIME_CREATED_AT >= sysdate-90