WITH 
hpl_lead_data as (
SELECT
    JSON_VALUE(json_data, '$."phone_number"') AS phone_number,
    to_date(JSON_VALUE(json_data, '$."lead_timestamp"'),'YYYY-MM-DD HH24:MI:SS') AS lead_timestamp,
    JSON_VALUE(json_data, '$."system_source"') AS system_source,
    JSON_VALUE(json_data, '$."utm_id"') AS utm_id,
    JSON_VALUE(json_data, '$."utm_source"') AS utm_source,
    JSON_VALUE(json_data, '$."utm_medium"') AS utm_medium,
    JSON_VALUE(json_data, '$."utm_campaign"') AS utm_campaign,
    JSON_VALUE(json_data, '$."utm_term"') AS utm_term,
    JSON_VALUE(json_data, '$."utm_content"') AS utm_content,
    JSON_VALUE(json_data, '$."clickid"') AS clickid
FROM custom_raw_hpl_lead
),
APPL AS
(select
    coalesce(s.id_cuid, c.cuid) cuid,
    coalesce(cast(s.scoring_id as varchar(20)), cast(c.CONTRACT_ACCOUNT_NUMBER as varchar(20))) scoring_id,
    c.skp_credit_case,c.CONTRACT_ACCOUNT_NUMBER,
    coalesce(s.DATE_SCORING_EVENT, c.date_bod2_enter,c.date_signed) AS date_scoring,
    c.date_bod2_enter DATE_2BOD, c.DATE_APPROVED date_2bod_approved, c.date_signed ,
    case when s.DATE_SCORING_EVENT is null and c.date_bod2_enter is not null then 1 else s.FLAG_SCORING_APPROVED_BNPL end FLAG_SCORING_APPROVED_BNPL,
    case when c.contract_status IN ('Active','Signed') then 1 else 0 end as f_sign_active_contract ,
    case when c.salesroom_code_onboarding = '013124' then 'SAO'
 when c.salesroom_code_onboarding = '012657' then 'TIKI'
 when c.salesroom_code_onboarding = '223348' then 'BAOKIM'
 when c.salesroom_code_onboarding = '223201' then 'ONEPAY'
 when s.apl_bod0_trigger_source = 'BNPL_SAO' then 'SAO'
 when s.apl_bod0_trigger_source = 'BNPL_TIKI' then 'TIKI'
 when s.apl_bod0_trigger_source = 'BNPL_BAOKIM' then 'BAOKIM'
 when s.apl_bod0_trigger_source = 'BNPL_ONEPAY' then 'ONEPAY'
 end as partner--,
from ap_crm.tb_rep_bnpl_scoring_wtf s --CLIENT_NEW_EXISTING
full outer join ap_crm.tb_rep_bnpl_application_wtf c
on s.offer_id_acc_registration_aft_scoring = c.offer_id
and s.apl_bod0_trigger_source in ('BNPL_TIKI', 'BNPL_ONEPAY', 'BNPL_BAOKIM', 'BNPL_SAO')),
sao as(
select
cuid, scoring_id, skp_credit_case, CONTRACT_ACCOUNT_NUMBER,
Date_scoring, DATE_2BOD, date_2bod_approved, date_signed ,
FLAG_SCORING_APPROVED_BNPL, f_sign_active_contract--,customer_type
from APPL
where partner = 'SAO'
and trunc(DATE_SCORING) >= date'2023-01-01') ,

app_source as
(select
  dateid, cuid, --sourceid,
  media_source, clickid-- af_adset, campaign, af_ad
  from
  (select
  trunc(a.event_time) as dateid, event_time,
  b.cuid, --b.sourceid,
  case when a.media_source = 'XNA' then null else a.media_source end media_source,
  case when a.click_id = 'XNA' then null else a.click_id end clickid,
  row_number() over (partition by cuid,trunc(a.event_time) order by a.event_time desc) as source_seq
  from ap_product.mkt_appsflyer a
  join ap_businessinsights.gma_f_dau b
  on trunc(a.event_time) = b.login_date and a.customer_user_id = b.sourceid
  where event_name = 'DAS001' and b.cuid is not null
  and trunc(a.event_time) >= date'2023-01-01'
  and trunc(login_date) >= date'2023-01-01'
  and a.media_source in ('accesstradevn_int'))
where source_seq =1 and length(cuid) < 13),

hpl_lead as
( select
    trunc(lead_date) lead_date, a.cuid, platform, utm_source,
    media_source, a.utm_term, a.utm_content,
    Case when utm_source in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP') then b.clickid else a.clickid end clickid
    from
  (select
    lead_timestamp lead_date,
    case when replace(a.utm_source,'direct_','') in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP') then 'HAPP' else 'Web' end platform,
    replace(a.utm_source,'direct_','') utm_source, a.clickid,
    a.utm_term, a.utm_content,
    b.id_cuid cuid,
    row_number() over(partition by b.id_cuid, trunc(a.lead_timestamp) order by a.lead_timestamp desc) seq
   from hpl_lead_data a
   inner join dm_crm_sel.f_client_contact b
   on a.phone_number = b.text_phone_mobile_primary
   where trunc(a.lead_timestamp) >= date'2023-01-01'
   and replace(a.utm_source,'direct_','') in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP','AC')
   group by
    lead_timestamp,
    case when replace(a.utm_source,'direct_','') in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP') then 'HAPP' else 'Web' end ,
    a.utm_source, a.clickid,
    a.utm_term, a.utm_content,
    b.id_cuid) a
  left join app_source b
  on trunc(a.lead_date) = b.dateid and a.cuid = b.cuid and a.utm_source in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP')
where a.seq = 1 and a.cuid != '-1'
and (case when utm_source =  'AC' then 1
  when utm_source in ('GMA', 'HomeApp','HAPP_BLOGPOST','MKT_HAPP') and media_source = 'accesstradevn_int' then 1
  else 0 end)  =1)
select
a.Date_scoring created_on,
b.platform,
case when  b.platform = 'HAPP' then b.media_source else b.utm_source end source,
b.utm_term, b.utm_content,
b.clickid,
case when a.Date_scoring  is not null then 1 else 0 end f_lead,
case when a.FLAG_SCORING_APPROVED_BNPL = 1 then 1 else 0 end f_qualified_lead,
case when a.date_signed is not null then 1 else 0 end  f_contract
from sao a
join hpl_lead b
on a.Date_scoring = b.lead_date and a.cuid = b.cuid