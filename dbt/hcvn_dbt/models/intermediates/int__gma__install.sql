with 
src__appsflyer as (select * from {{ ref('stg__appsflyer') }}),
src__gma__first_login as (select * from {{ ref('int__gma__first_login') }}),


base as (
  select distinct 
    trunc(event_time) as install_date,
    appsflyer_id,
    coalesce(case when media_source = 'XNA' then null else media_source end ,af_prt) as media_source,
    campaign,
    platform,
    row_number() over (partition by appsflyer_id order by event_time desc) as install_rnk  
  from src__appsflyer
  where event_name = 'install'

  {% if is_incremental() %}
  and event_time >= sysdate - 15
  {% endif %}
),

final as (
  select
    a.install_date,
    a.appsflyer_id,
    coalesce(case when a.media_source = 'XNA' or a.media_source = 'organic' then null else a.media_source end, b.media_source) as media_source,
    coalesce(case when a.campaign = 'XNA' then null else a.campaign end, b.campaign) as campaign,
    case 
        when b.appsflyer_id is not null then 1 else 0 
    end as has_logged_in,
    a.PLATFORM, 
    b.CUSTOMER_TYPE, 
    b.CONTRACT_STATUS, 
    b.CAPP_TO_GMA, 
    b.CUID
  from base a
  left join src__gma__first_login b
  on a.appsflyer_id = b.appsflyer_id
  {{ dbt_utils.generate_surrogate_key(['a.install_date','a.appsflyer_id','a.media_source']) }} as dbt_key_skp_gma_install
  {% if is_incremental() %}
  and b.login_date >= sysdate - 15
  {% endif %}

)
select 
  final.*,
  
from final 