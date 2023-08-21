with 
final as (
    select * from {{ source('OWNER_DWH','F_STORE_OFFER_LIMIT_AD') }}
)
select * from final 
where DTIME_MODIFIED >= sysdate-190