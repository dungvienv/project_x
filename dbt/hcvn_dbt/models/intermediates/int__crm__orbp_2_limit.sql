with 
src__crm_appl as(select * from {{ ref('int__crm__appl') }}),
src__dc_salesroom as(select * from {{ ref('stg__dc_salesroom') }}),
src__dc_seller as(select * from {{ ref('stg__dc_seller') }}),
src__crm_orbp as(select * from {{ ref('int__crm__orbp') }}),
src__crm_offer as(select * from {{ref('int__crm__offer')}}),
src__crm_map_trigger_appl as (select * from {{ ref('int__crm__map_trigger_appl') }}),
src__dc_employee as(select * from {{ ref('stg__dc_employee') }}),
src__dc_ldp_customer as (select * from {{ ref('stg__dc_ldp_customer') }}),
src__dc_ldp_customer_source as (select * from {{ ref('stg__dc_ldp_customer_source') }}),
src__f_store_offer_limit_ad as (select * from {{ ref('stg__f_store_offer_limit_ad') }}),
acl_lp AS --before the time SA CODE was record to vector, using LDP tables to detect SA CODE
(
  select C.DATE_CREATED_AT,
    C.ID_CUID,
    case
      when C.CODE_SA != 'XNA' then C.CODE_SA
    end as CODE_SA,
    MAX(S.TEXT_UTM_SOURCE) UTM_SOURCE
  from src__dc_ldp_customer C
    LEFT JOIN src__dc_ldp_customer_source S ON C.ID_CUSTOMER = S.ID_CUSTOMER
    AND C.DATE_CREATED_AT = S.DTIME_CREATED_AT
  where C.ID_CUID != 'XNA'
    AND C.DATE_CREATED_AT >= (date '2019-10-01')
    and C.DATE_CREATED_AT < date '2021-07-14'
  group by C.DATE_CREATED_AT,
    C.ID_CUID,
    case
      when C.CODE_SA != 'XNA' then C.CODE_SA
    end
),

apl_bod0_lp as ---map SA CODE and Flag Agent consult for BOD0 in web and mobile for before '2021-07-14' and after '2021-07-14'
(
  SELECT O.SKF_APPROVAL_PROCESS_HEAD,
    o.apl_bod0_dtm,
    o.id_cuid,
    t.DATE_CREATED_AT,
    case
      when o.apl_bod0_dtm < date '2021-07-14' then case
        when SubStr(t.code_sa, 1, 3) in (
          'FIC',
          'GRS',
          'VFS',
          'FIS',
          'H2G',
          'MST',
          'TCF'
        ) then SubStr(t.code_sa, 1, 3)
        when t.code_sa is not null then 'SA'
      end
      when o.apl_bod0_dtm >= date '2021-07-14' then case
        when SubStr(o.APL_BOD0_SA_CODE, 1, 3) in (
          'FIC',
          'GRS',
          'VFS',
          'FIS',
          'H2G',
          'MST',
          'TCF'
        ) then SubStr(o.APL_BOD0_SA_CODE, 1, 3)
        when o.APL_BOD0_SA_CODE not in (
          'osbuser',
          '0',
          'GMA',
          'BNPL_Onboarding'
        ) --Th add BNPL_Onboarding on 13 June 2023
        and e.SKP_EMPLOYEE is not null then 'SA'
      end
    end as PROC_F_ONL_SA_CONSULT,
    case
      when o.apl_bod0_dtm < date '2021-07-14' then t.code_sa
      when o.apl_bod0_dtm >= date '2021-07-14' then decode(
        o.APL_BOD0_SA_CODE,
        'osbuser',
        null,
        e.CODE_EMPLOYEE
      )
    end as PROC_ONL_SA_CODE,
    row_number() over(
      partition by o.skf_approval_process_head
      order by t.DATE_CREATED_AT desc
    ) rank_lp
  FROM src__crm_orbp O
    left join acl_lp T ON T.ID_CUID = O.ID_CUID
    AND T.DATE_CREATED_AT <= o.apl_bod0_dtm
    and o.apl_bod0_dtm <= T.DATE_CREATED_AT + 1
    left join src__dc_employee e on o.APL_BOD0_SA_CODE = e.code_employee
  where 1 = 1
    AND O.APL_BOD0_DTM >= (DATE '2021-05-25') --ACLD start date
    AND O.APL_BOD0_TRIGGER_SOURCE in (
      'WEB',
      'CAPP',
      'TLSLP',
      'GMA',
      'LP_CC'
    )
),

--Th add LP_CC on 13 June 2023
pos_trigger as 
(
  select skp_client,
    skp_credit_case,
    apl_contract_num,
    pos.APL_APPROVE_DTM,
    SKP_SALESROOM_APPL_CREATED,
    sr.SKP_BUSINESS_MODEL_TYPE
  from src__crm_appl pos
    left join src__dc_salesroom sr -- get shop trigger ORBP
    on pos.SKP_SALESROOM_APPL_CREATED = sr.SKP_SALESROOM
    left join src__dc_seller sl on sr.skp_seller = sl.skp_seller
  where pos.SKP_CREDIT_TYPE = 1
    and pos.APL_APPROVE_DTM is not null
),

tb_scoring_temp as 
(
  select --+ USE_HASH(offer) FULL(r) FULL (l) FULL(soffer)
    r.apl_bod0_dtm, 
    r.apl_bod0_trigger_source, 
    r.apl_bod0_trigger_type, 
    r.skf_approval_process_head, 
    case
      when r.apl_bod0_channel in ('WEB')
            and substr(lp.PROC_ONL_SA_CODE, 1, 3) in
            ('FIC', 'GRS', 'VFS', 'FIS', 'H2G', 'MST', 'TCF') then
        'DSA'
      else r.apl_bod0_channel
    end as apl_bod0_channel,
    r.id_cuid, 
    case
      when r.apl_bod0_offer_name like '%CLX_CB%' then 'CLX_CB'
      when r.apl_bod0_offer_name like 'CBX%' then 'CLX_CB'
      else r.apl_bod0_offer_type
    end as new_apl_bod0_offer_type, 
    r.apl_f_bod0_approved, 
    r.apl_f_bod0_otp, 
    r.ca_off_id, 
    row_number() over(
      partition by r.skf_approval_process_head,
      (
        case
          when r.apl_bod0_offer_name like '%CLX_CB%' then 'CLX_CB'
          when r.apl_bod0_offer_name like 'CBX%' then 'CLX_CB'
          else r.apl_bod0_offer_type
        end
      )
      order by (
          case
            when r.apl_f_bod0_approved = 1 then 1
            else 2
          end
        ) asc,
        r.apl_bod0_dtm desc,
        (
          case
            when pos.APL_CONTRACT_NUM = r.apl_contract_num then 1
            else 2
          end
        ) asc --- incase client has multiple pos approved, get the lastest one before BOD0
    ) as rnk_by_product, 
    
    case
      when r.APL_LAST_ACTIVE_DT is null then 'New'
      when r.APL_LAST_ACTIVE_DT is not null then case
        when r.apl_bod0_offer_type = 'ACL' then --ACL 0BOD
        case
          when r.CA_ID is not null
          and r.CA_OFF_ID is not null then 'Existing'
          else 'Existing'
        end
        when nvl(r.apl_bod0_offer_type,'XNA') != 'ACL' then case
          when r.CA_OFF_ID is not null
          and nvl(offer.ca_type,'XNA') != 'SAI' then 'Existing'
          when r.CA_OFF_ID is not null
          and offer.ca_type = 'SAI' then 'Existing'
          when r.CA_OFF_ID is null then --- when recal orbp, client has no offer
          'Existing' --                    when offer.CA_OFF_ID is null then --- when recal orbp, client has offer but it is clip offer so our offer table is exclude these offer
          --                   'Existing - No Offer'
          when offer.CA_OFF_ID is null then 'Existing'
          else 
            'Existing'
        end
      end
    end as CLI_F_NEW_EXISTENT_BOD0, 
    PROC_F_ONL_SA_CONSULT as PROC_F_ONL_SA_CONSULT 
,
    lp.PROC_ONL_SA_CODE 
,
    l.ID_SOURCE as onl_limit_id
,
    r.skp_client as key_skp_client, 
    r.apl_f_bod0_base
  from src__crm_orbp r
  left join apl_bod0_lp lp on r.skf_approval_process_head = lp.skf_approval_process_head --and    lp.rank_bod0 = 1
    and lp.rank_lp = 1 -- and    r.APL_BOD0_OFFER_TYPE = 'ACL' --ACL 0BOD --get for all product type
    AND r.APL_BOD0_DTM >= (DATE '2021-05-25') --ACL 0BOD start date
  left join src__f_store_offer_limit_ad l on r.id_limit_bulk = l.ID_LIMIT_BULK
        and    r.output_limit_type = l.CODE_TYPE_LIMIT
        and    r.apl_f_bod0_approved = 1
        and    l.CODE_SUB_TYPE_LIMIT in ('ONLINE', 'MASTER')
  left join src__crm_offer offer on r.ca_off_id = offer.ca_off_id
  left join pos_trigger pos on pos.SKP_CLIENT = r.SKP_CLIENT
  and pos.APL_APPROVE_DTM <= r.apl_bod0_dtm + 1 / (24 * 60) -- +1 mins incase gaps with ORBP        
    
  where r.apl_bod0_dtm >= (date '2019-10-01')
    and r.apl_bod0_offer_type != 'XNA' --adding 20-Feb-2023        
),

final as(
  select 
    trunc(l.apl_bod0_dtm) as date_bod0,
    l.apl_bod0_dtm as dtm_bod0, 
    l.id_cuid as onl_id_cuid, 
    l.apl_bod0_trigger_source as onl_bod0_trigger_source, 
    l.apl_bod0_trigger_type as onl_bod0_trigger_type, 
    l.skf_approval_process_head as onl_bod0_key, 
    l.new_apl_bod0_offer_type as onl_bod0_offer_type, 
    l.apl_f_bod0_approved as onl_bod0_f_approved, 
    l.apl_f_bod0_otp as onl_bod0_f_otp, 
    trg.TRIGGER_BOD1_CHANNEL,
    case
      when l.apl_bod0_channel = 'TLS' then 'TLS'
      when l.apl_bod0_channel = 'SA' then 'SA'
      when l.apl_bod0_channel = 'MOB_APP' then 'Mobile App'
      when l.apl_bod0_channel = 'ACL_VIRTUAL' then 'ACL_VIRTUAL'
      when l.apl_bod0_channel = 'WEB' then 'Web'
      when l.apl_bod0_channel = 'WEB_TLS' then 'Web TLS'
      when l.apl_bod0_channel = 'WEB_CC' then 'Web CC'
      when l.apl_bod0_channel = 'Icash' then 'Auto'
      else l.apl_bod0_channel
    end proc_bod0_channel, 
    o.ca_off_id as onl_offer_id, 
    l.CLI_F_NEW_EXISTENT_BOD0, 
    l.PROC_F_ONL_SA_CONSULT, 
    l.PROC_ONL_SA_CODE, 
    l.key_skp_client, 
    nvl(apl_f_bod0_base, 1) as ONL_BOD0_F_BASE, 
    trg.TRIGGER_SKP_CREDIT_TYPE 

  from tb_scoring_temp l
  left join src__crm_offer o --map product offer after ORBP
  on (
    l.onl_limit_id = o.ca_lim_id
    and o.ca_f_orbp not like '%SCORING%'
    and 
    case
      when o.ca_type != 'BNPL' then 1
      when o.ca_type = 'BNPL'
      and o.CA_OFFER_TYPE_CODE = 'BNPL_ACCOUNT_OFFER' then 1
      else 0
    end = 1
  )
  left join src__crm_map_trigger_appl trg on trg.skf_approval_process_head = l.skf_approval_process_head
  where 1 = 1
    and rnk_by_product = 1
)
select *
from final