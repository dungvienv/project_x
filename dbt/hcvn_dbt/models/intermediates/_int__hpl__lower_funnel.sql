SELECT  
 scoring_id, CONTRACT_ACCOUNT_NUMBER, cuid,
 date_scoring dateid,CUSTOMER_TYPE,
 FLAG_SCORING_APPROVED_BNPL  f_approved_0bod, 
 case when DATE_2BoD is not null then 1 else 0 end f_2BOD,
 case when DATE_2BoD_APPROVED is not null then 1 else 0 end f_approved_2bod,
 CASE when DATE_SIGNED is not null and F_SIGN_ACTIVE_CONTRACT =1 then 1 else 0 end f_signed,
 case when UTM_SOURCE is null then 'Web' else platform end platform, 
 case when upper(substr(media_channel,1,5)) = 'BRAND' then 'Brand' else media_channel end media_channel,
 UTM_SOURCE, MEDIA_SOURCE, UTM_MEDIUM, UTM_CAMPAIGN, UTM_TERM, UTM_CONTENT
from ap_product.hpl_source_application
where f_mkt =1 