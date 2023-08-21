WITH 
src__appsflyer as (select * from {{ ref('stg__appsflyer') }}),
src__gma_f_dau as (select * from {{ ref('stg__gma_f_dau') }}),
src__webscore_lead as (select * from {{ ref('stg__webscore_lead_temp') }}),
src__ccx_lead as (select * from {{ ref('stg__ccx_lead_data') }}),
src__bod0_2_application as (select * from {{ ref('mart__crm__rep_bod0_2_application') }}),
scr__mkt_sources as (select * from {{ ref('stg__mkt_sources') }}),


appsflyer as (
    select 
        'appsflyer' as source_system,
        event_time as lead_timestamp,
        cuid,
        media_source,
        af_adset as utm_medium,
        campaign as utm_campaign,
        null as utm_term,
        af_ad as utm_content,
        null as page
    from src__appsflyer a
    inner join src__gma_f_dau b
    on trunc(a.event_time) = b.login_date 
        and a.customer_user_id = b.sourceid 
        and a.event_name = 'DAS001'
        and b.cuid is not null
        and a.media_source is not null
        and a.media_source != 'organic'
),

cl_ldp as (
    select  
        'cashloan_ldp' as source_system,
        w.created_on as lead_timestamp,
        w.id_cuid as cuid,
        w.UTM_SOURCE as media_source,
        case when w.UTM_MEDIUM = 'XNA' then null else w.UTM_MEDIUM end as UTM_MEDIUM,
        case when w.UTM_CAMPAIGN = 'XNA' then null else w.UTM_CAMPAIGN end as UTM_CAMPAIGN,
        case when w.UTM_TERM = 'XNA' then null else w.UTM_TERM end as UTM_TERM,
        case when w.UTM_CONTENT = 'XNA' then null else w.UTM_CONTENT end as UTM_CONTENT,
        w.PAGE
    from src__webscore_lead w
    where w.UTM_SOURCE is not null 
        and  w.UTM_SOURCE != 'XNA'
),

cc_ldp as (
    select distinct
        'cc_ldp' as source_system,
        a.lead_timestamp, 
        a.cuid, 
        a.utm_source as media_source, 
        a.utm_medium, 
        a.utm_campaign, 
        a.utm_term, 
        a.utm_content,
        null as page
    from src__ccx_lead a
    inner join scr__mkt_sources b on upper(a.utm_source) = upper(b.utm_source) and b.platform = 'Web'
    where a.UTM_SOURCE is not null 
        and  a.UTM_SOURCE != 'XNA'
),

LOWER_FUNNEL AS (
    SELECT DISTINCT
        DBT_KEY_SKP_BOD0_2_APPLICATION,
        COALESCE(DATE_BOD0,DATE_BOD01_BOD02) AS DATE_BOD0, 
        DATE_BOD01, 
        DATE_BOD02,
        DATE_SIGN,
        ONL_BOD0_OFFER_TYPE, 
        PROC_BOD0_CHANNEL , 
        PROC_BOD01_CHANNEL , 
        PROC_BOD02_CHANNEL, 
        PROC_SIGN_CHANNEL , 
        PROD_CODE_GR,
        ONL_BOD0_F_APPROVED, 
        APL_F_BOD1_BASE, 
        APL_F_BOD1_APPROVE,
        APL_F_BOD2_BASE,  
        APL_F_BOD2_APPROVE,
        FIN_FINAL_VOLUME as VOLUME,
        CLI_F_NEW_EXISTENT_BOD0 as CUSTOMER_TYPE,
        CAST(KEY_ID_CUID AS VARCHAR2(100)) as CUID,
        ONL_BOD0_KEY, 
        KEY_BSL_CONTRACT_NUMBER,
        ONL_OFFER_ID,
        CASE WHEN DATE_CANCEL IS NULL THEN 0 ELSE 1 END F_CANCEL, 
        CASE WHEN PROC_ONL_SA_CODE IS NOT NULL THEN 1 ELSE 0 END F_PROC_ONL_SA_CODE,
        CASE 
           WHEN (PROC_BOD0_CHANNEL IN ('BNPL', 'GMA','Mobile App','MOMO', 'Web') AND PROC_ONL_SA_CODE IS NULL) THEN 1
           WHEN (PROC_BOD0_CHANNEL IN ('Auto') AND PROC_BOD01_CHANNEL IN ('GMA','Mobile App')) THEN 1 
           ELSE 0 
        END F_INI_ONL_SALE,
        case 
           when (PROC_BOD0_CHANNEL in ('Web','Web CC') and (PROC_ONL_SA_CODE is null or PROC_ONL_SA_CODE = 'BNPL_Onboarding')) then 'Web'
           when (PROC_BOD0_CHANNEL in ('Mobile App') and (PROC_ONL_SA_CODE is null or PROC_ONL_SA_CODE = 'BNPL_Onboarding')) then 'CAPP'
           when (PROC_BOD0_CHANNEL in ('GMA') and (PROC_ONL_SA_CODE is null or PROC_ONL_SA_CODE = 'BNPL_Onboarding')) then 'HAPP'
           when (PROC_BOD0_CHANNEL in ('BNPL','MOMO') and (PROC_ONL_SA_CODE is null or PROC_ONL_SA_CODE = 'BNPL_Onboarding')) then 'Others'
           when (PROC_BOD0_CHANNEL in ('Auto') and PROC_BOD01_CHANNEL in ('Mobile App')) then 'CAPP'
           when (PROC_BOD0_CHANNEL in ('Auto') and PROC_BOD01_CHANNEL in ('GMA')) then 'HAPP'
           else 'Others' 
        end PLATFORM_ONL_SALE,
        case 
            when ONL_BOD0_OFFER_TYPE in ('ACL','ACLX','CLX') then 'Cashloan'
            when ONL_BOD0_OFFER_TYPE in ('CCX','CC_SC','CC_EC') then 'Card'
            when ONL_BOD0_OFFER_TYPE in ('BNPL') then 'Home Paylater'
            else ONL_BOD0_OFFER_TYPE 
        end product_type,
        LEAST(
            CASE 
               WHEN (PROC_BOD0_CHANNEL IN ( 'GMA') AND PROC_ONL_SA_CODE IS NULL) THEN TRUNC(DATE_BOD0)
               WHEN (PROC_BOD0_CHANNEL IN ('Auto') AND PROC_BOD01_CHANNEL IN ('GMA')) THEN TRUNC(DATE_BOD01)
            END
        ) AS DATE_GMA_FIRST_TOUCHPOINT
    FROM src__bod0_2_application
    WHERE 1=1
    and (ONL_BOD0_OFFER_TYPE  in ('ACL','ACLX','CLX','CLX_CB','CCX','CC_SC','CC_EC','BNPL') or PROD_CODE_GR in ('ACL','ACLX','CLX','CLX_CB','CCX','CC_SC','CC_EC','BNPL'))
),

clx_happ as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        b.*
    from lower_funnel a 
    left join appsflyer b
    on a.cuid = b.cuid
        and trunc(a.date_gma_first_touchpoint) = trunc(b.lead_timestamp)
        and a.PLATFORM_ONL_SALE = 'HAPP'
        and a.product_type = 'Cashloan'
),

clx_web as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        b.*
    from lower_funnel a 
    left join cl_ldp b
    on a.cuid = b.cuid
        and trunc(a.date_bod0) between trunc(b.lead_timestamp) and trunc(b.lead_timestamp) + interval '3' day
        and a.PLATFORM_ONL_SALE = 'Web'
        and a.product_type = 'Cashloan' 
),

ccx_happ as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        b.*
    from lower_funnel a 
    left join appsflyer b
    on a.cuid = b.cuid
        and trunc(a.date_gma_first_touchpoint) = trunc(b.lead_timestamp)
        and a.PLATFORM_ONL_SALE = 'HAPP'
        and a.product_type = 'Card'
),

ccx_web as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        b.*
    from lower_funnel a 
    left join cc_ldp b
    on a.cuid = b.cuid
        and trunc(a.date_bod0) = trunc(b.lead_timestamp)
        and a.PLATFORM_ONL_SALE = 'Web'
        and a.product_type = 'Card' 
),

clx_capp as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        'capp' as source_system,
        null as lead_timestamp, 
        null as cuid, 
        null as media_source, 
        null as utm_medium, 
        null as utm_campaign, 
        null as utm_term, 
        null as utm_content,
        null as page
    from lower_funnel a 
    where a.PLATFORM_ONL_SALE = 'CAPP'
        and a.product_type = 'Cashloan'
),

clx_others as (
    select 
        a.DBT_KEY_SKP_BOD0_2_APPLICATION,
        'others' as source_system,
        null as lead_timestamp, 
        null as cuid, 
        null as media_source, 
        null as utm_medium, 
        null as utm_campaign, 
        null as utm_term, 
        null as utm_content,
        null as page
    from lower_funnel a 
    where a.PLATFORM_ONL_SALE not in ('CAPP','HAPP','Web')
        and a.product_type = 'Cashloan'
),
union_df as (
    select a.*, row_number() over(partition by DBT_KEY_SKP_BOD0_2_APPLICATION order by SOURCE_SYSTEM, lead_timestamp desc) as lead_seq
    from (
        select * from clx_happ
        union all 
        select * from clx_capp
        union all 
        select * from clx_web
        union all 
        select * from clx_others
        union all 
        select * from ccx_happ
        union all 
        select * from ccx_web
    ) a
),
final as (
    select DBT_KEY_SKP_BOD0_2_APPLICATION, SOURCE_SYSTEM, LEAD_TIMESTAMP, CUID, MEDIA_SOURCE, UTM_MEDIUM, UTM_CAMPAIGN, UTM_TERM, UTM_CONTENT, PAGE
    from union_df 
    where lead_seq = 1
)
select * from final

