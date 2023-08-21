with
final as (
    select * from {{ source('DM_CRM','FT_CRM_OFFER') }}
)

select * from final
where dtime_updated >= sysdate-190