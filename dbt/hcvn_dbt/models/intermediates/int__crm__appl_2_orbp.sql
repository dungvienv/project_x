with 
src__dc_employee as(select * from {{ ref('stg__dc_employee') }}),
src__crm_appl as(select * from {{ ref('int__crm__appl') }}),
src__crm_appl_2_offer as(select * from {{ ref('int__crm__appl_2_offer') }}),

final as (
  select
    /*+USE_HASH(A NW)*/
    a.skp_credit_case as key_skp_credit_case, 
    a.skp_salesroom_appl_created as key_skp_salesroom_appl_created, 
    a.id_cuid as key_id_cuid, 
    ap.ca_off_id as apl_off_id,
    ap.SKF_APPROVAL_PROCESS_HEAD as key_skf_head_bod0,
    a.apl_prod_code_gr as prod_code_gr, 
    trunc(a.apl_bod01_bod02_dtm) as date_bod01_bod02, 
    trunc(a.apl_bod01_dtm) as date_bod01, 
    trunc(a.apl_bod02_dtm) as date_bod02, 
    trunc(a.apl_sign_dtm) as date_sign, 
    trunc(a.apl_cancel_dtm) as date_cancel, 
    round((apl_bod02_dtm - apl_bod01_bod02_dtm) * 24 * 60) as mins_bod1_bod2, 
    a.apl_f_bod1_base as apl_f_bod1_base, 
    a.apl_f_bod1_approve as apl_f_bod1_approve, 
    a.apl_f_bod2_base as apl_f_bod2_base, 
    a.apl_f_bod2_approve as apl_f_bod2_approve, 
    a.apl_tenor as fin_tenor, 
    a.apl_vol as fin_final_volume, 
    case
      when apl_bod01_channel = 'CLW_ONLINE'
      and SubStr(CONS.code_employee, 1, 3) in (
        'FIC',
        'GRS',
        'VFS',
        'FIS',
        'H2G',
        'MST',
        'TCF'
      ) then 'DSA'
      when apl_bod01_channel = 'CLW_ONLINE' then 'Web' -- 'CLW Online'
      when apl_bod01_channel = 'MOBILE_APP' then 'Mobile App'
      when apl_bod01_channel = 'OTHER' then 'Others'
      when apl_bod01_channel = 'TLS_ACL' then 'TLS'
      when apl_bod01_channel = 'TLS_BT' then 'TLS'
      else apl_bod01_channel
    end as proc_bod01_channel, 
    decode(
      apl_bod02_channel,
      'CLW_ONLINE',
      'Web',
      'MOBILE_APP',
      'Mobile App',
      'OTHER',
      'Others',
      apl_bod02_channel
    ) as proc_bod02_channel, 
    case
      when a.apl_sign_channel = 'SA' then 'With_SA'
      when a.apl_sign_channel = 'ESIGN' then 'Without_SA'
    end proc_sign_channel, 
    a.apl_contract_num as KEY_BSL_CONTRACT_NUMBER, 
    a.skp_client as KEY_SKP_CLIENT, 
    CONS.code_employee as bod01_code_employee_consult, 
    a.APL_CONSULT_CHANNEL 

    from src__crm_appl a
    left join src__crm_appl_2_offer ap on a.skp_credit_case = ap.skp_credit_case 
      and (case
            when ap.ca_start_dt < date '2019-10-09' then
            ap.rk_apl_by_ca_start_dt
            else
            1
          end) = 1 
      and ap.ca_start_dt >= sysdate - 190 ---BNPL valid 180
      and ap.ca_start_dt < sysdate + 100
    left join src__dc_employee CONS on CONS.skp_employee = a.apl_consult_skp_employee
  where 1 = 1
    and a.skp_credit_type > 0 -- exclude unidentify sredit type (-1)
    and a.skp_credit_case ! = 203056350
)
select * from final