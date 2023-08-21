with
final as (
    select * from {{ source('DM_CRM','FT_CRM_APPL_2_OFFER') }}
)

select * from final
where dtime_updated >= sysdate-90