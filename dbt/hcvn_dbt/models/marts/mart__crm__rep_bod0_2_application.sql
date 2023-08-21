with 
src__crm_orbp_2_limit as (select * from {{ ref('int__crm__orbp_2_limit') }}),
src__crm_appl_2_orbp as (select * from {{ ref('int__crm__appl_2_orbp') }}),
final as (
   select distinct----- BOD0 Part
      scr.date_bod0, 
      scr.dtm_bod0,
      scr.onl_bod0_trigger_source, 
      scr.onl_bod0_trigger_type,
      scr.onl_bod0_key, 
      scr.onl_bod0_offer_type, 
      scr.onl_bod0_f_approved, 
      scr.onl_bod0_f_otp, 
      SCR.proc_bod0_channel AS proc_bod0_channel, 
      scr.CLI_F_NEW_EXISTENT_BOD0, 
      scr.PROC_F_ONL_SA_CONSULT, 
      scr.PROC_ONL_SA_CODE, 
      scr.onl_bod0_f_base,
      SCR.ONL_OFFER_ID, 
   CASE
      WHEN scr.proc_bod0_channel = 'Auto'
      and scr.TRIGGER_SKP_CREDIT_TYPE in (2, 3) THEN scr.TRIGGER_BOD1_CHANNEL
      ELSE scr.proc_bod0_channel
   END AS PROC_BOD0_CHANNEL_NEW,
      -----Application Part
      apl.key_skp_credit_case, 
      nvl(apl.key_id_cuid, scr.onl_id_cuid) as key_id_cuid, 
      apl.prod_code_gr,  
      apl.date_bod01_bod02, 
      apl.date_bod01, 
      apl.date_bod02, 
      apl.date_sign, 
      apl.date_cancel, 
      apl.mins_bod1_bod2, 
      apl.apl_f_bod1_base, 
      apl.apl_f_bod1_approve, 
      apl.apl_f_bod2_base, 
      apl.apl_f_bod2_approve, 
      apl.fin_tenor,
      apl.fin_final_volume, 
      apl.proc_bod01_channel, 
      apl.proc_bod02_channel, 
      apl.proc_sign_channel, 
      apl.KEY_BSL_CONTRACT_NUMBER, 
      apl.key_skp_salesroom_appl_created, 
      nvl(apl.key_skp_client, scr.key_skp_client) as key_skp_client, 
   case
      when scr.onl_bod0_trigger_source in ('WEB', 'CAPP', 'WEB_TLS', 'GMA', 'LP_CC')
      and scr.proc_f_onl_sa_consult is null then 1
      when scr.ONL_BOD0_TRIGGER_SOURCE like 'BNPL%' then 1
      when scr.ONL_BOD0_TRIGGER_SOURCE = 'MOMO' then 1
      when scr.PROC_BOD0_CHANNEL = 'Auto'
      and apl.proc_bod01_channel in ('GMA', 'Mobile App', 'Web')
      and apl.APL_CONSULT_CHANNEL not in ('SA', 'DSA', 'TLS') then 1
      when apl.proc_bod01_channel = 'EPOS'
      and apl.bod01_code_employee_consult in ('EPOS_User', 'OpenAPI_User', 'EPOS2_User') then 1
      else 0
   end as DIGITAL_FLAG_INITIATED, 
      apl.APL_CONSULT_CHANNEL,
      {{ dbt_utils.generate_surrogate_key(['scr.onl_bod0_key','scr.onl_bod0_offer_type','scr.onl_offer_id','apl.key_skp_credit_case']) }} as dbt_key_skp_bod0_2_application
   from src__crm_orbp_2_limit scr
   full join src__crm_appl_2_orbp apl on
      scr.onl_bod0_key = apl.key_skf_head_bod0
      and scr.onl_offer_id = apl.apl_off_id
)
select distinct * from final
{% if is_incremental() %}
where
(date_bod01_bod02 >= (select max(date_bod01_bod02) from {{ this }}) and date_bod01_bod02 <= sysdate)
or 
(date_bod0 >= (select max(date_bod0) from {{ this }}) and date_bod0 <= sysdate)

{% endif %}