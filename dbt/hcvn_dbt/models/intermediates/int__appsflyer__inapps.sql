with 
src__appsflyer_inapps as (select * from {{ ref('stg__appsflyer__inapps') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.appsflyer_id','a.customer_user_id','a.advertising_id','a.install_time','a.event_time','a.media_source','a.is_primary_attribution']) }} as dbt_key_id
    from src__appsflyer_inapps a
)
select * from final