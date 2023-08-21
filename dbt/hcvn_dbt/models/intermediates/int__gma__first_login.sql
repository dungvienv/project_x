with 
src__gma_f_dau as (select * from {{ ref('stg__gma_f_dau') }}),
src__capp_dau as (select * from {{ ref('stg__onl_mob_daily_logins_temp_02') }}),
src__bod0_2_application as (select * from {{ ref('mart__crm__rep_bod0_2_application') }}),
src__appsflyer as (select * from {{ ref('stg__appsflyer') }}),

base as (
    select 
        a.*, 
        row_number() over (partition by sourceid order by login_date) as rnk 
    from src__gma_f_dau a 
    where sourceid is not null
    
    {% if is_incremental() %}
    and a.login_date >= sysdate - 15
    {% endif %}
),

capp_login as (
    SELECT  
        MAX(login_date) AS last_capp_login_date, cuid
    FROM src__capp_dau
    WHERE login_date >= date'2023-01-01'
    GROUP BY cuid
),

bod0_2_application as (
    select 
        KEY_ID_CUID as cuid,
        DATE_BOD01,
        DATE_SIGN,
        PROC_BOD01_CHANNEL
    from src__bod0_2_application
    where PROC_BOD01_CHANNEL in ('TLS','SA')
    {% if is_incremental() %}
    and DATE_BOD01 >= sysdate - 15
    {% endif %}
),

gma_final as (
    SELECT
        t1.*
        , t2.last_capp_login_date
        , t3.PROC_BOD01_CHANNEL
    FROM base t1
    LEFT JOIN capp_login t2             ON cast(t1.cuid as varchar2(38)) = cast(t2.cuid as varchar2(38))
    LEFT JOIN bod0_2_application t3     ON cast(t1.cuid as varchar2(38)) = cast(t3.cuid as varchar2(38))
                                        AND t1.login_date between t3.date_bod01 and t3.date_sign           
    WHERE t1.rnk = 1
),

af_final as (
    select DISTINCT 
        coalesce(case when media_source = 'XNA' then null else media_source end, af_prt) as media_source,
        event_time,
        appsflyer_id,
        customer_user_id,
        case when CAMPAIGN = 'XNA' then null else CAMPAIGN end as CAMPAIGN, 
        case when AF_ADSET = 'XNA' then null else AF_ADSET end as AF_ADSET, 
        case when AF_AD = 'XNA' then null else AF_AD end as AF_AD, 
        case when AF_SUB1 = 'XNA' then null else AF_SUB1 end as AF_SUB1,
        case when PLATFORM = 'XNA' then null else PLATFORM end as PLATFORM,
        row_number() over 
                        (partition by trunc(event_time),customer_user_id
                        order by    trunc(event_time),
                        case media_source when 'organic' then 1 else 0 end,
                        case platform when 'android' then 0 when 'ios' then 1 else 9 end,
                        event_time) as login_rnk    --Make sure the keyid is unique by prioritize non-organic and android sources
    from src__appsflyer
    where customer_user_id is not null
    and event_name = 'DAS001' 
    and coalesce(case when media_source = 'XNA' then null else media_source end, af_prt) != 'organic'
    and coalesce(case when media_source = 'XNA' then null else media_source end, af_prt) is not null
    
    {% if is_incremental() %}
    and event_time >= sysdate - 15
    {% endif %}
), 

final as (
    select distinct 
      gma.login_date
    , gma.CUID
    , gma.sourceid
    , gma.CUSTOMER_TYPE
    , gma.CONTRACT_STATUS
    , af.appsflyer_id
    , af.campaign
    , af.af_adset
    , af.af_ad
    , af.af_sub1
    , af.platform
    , case  
        when (af.media_source is null or af.media_source = 'organic') then
            case 
                when gma.PROC_BOD01_CHANNEL = 'SA' then 'POS'
                when gma.PROC_BOD01_CHANNEL = 'TLS' then 'TLS'
                when gma.PROC_BOD01_CHANNEL is null then 'ASO'
            end
        else af.media_source end as media_source
    , case when last_capp_login_date is not null then 1 else 0 end as CAPP_TO_GMA                                                           
    from gma_final gma 
    left join af_final af on gma.login_date = trunc(af.event_time) and gma.sourceid = af.customer_user_id and af.login_rnk = 1
)

select distinct
    login_date, 
    platform, 
    CUSTOMER_TYPE, 
    CONTRACT_STATUS,
    media_source, 
    campaign, 
    af_adset, 
    af_ad, 
    af_sub1,
    CAPP_TO_GMA,
    cuid,
    appsflyer_id,
    {{ dbt_utils.generate_surrogate_key(['login_date','appsflyer_id','media_source']) }} as dbt_key_skp_gma_first_login
from final