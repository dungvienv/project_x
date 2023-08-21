with 
src__appsflyer_installs as (select * from {{ ref('stg__ab__appsflyer_installs') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.appsflyer_id','a.event_time','a.media_source']) }} as dbt_key_id
    from src__appsflyer_installs a
)
select * from final