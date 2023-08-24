with 
src__appsflyer_inapps as (select * from {{ ref('stg__appsflyer__inapps') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.appsflyer_id','a.event_time','a.media_source']) }} as dbt_key_id
    from src__appsflyer_inapps a
)
select * from final