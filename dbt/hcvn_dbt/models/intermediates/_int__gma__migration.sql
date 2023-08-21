with migrated_to_gma as
(select
    trunc(DATEID) as DATEID
    ,sum(cUID_CNT) AS migrated_cnt
from ap_product.mkt_agg_gma
where capp_to_gma = '1'
group by trunc(DATEID))

, capp_user_base as
(select
trunc(last_capp_login_date) as login_date,
count (distinct cuid) as total_capp_cnt
from
(
  SELECT
       MAX(login_date) AS last_capp_login_date,
       cuid
   FROM
       ap_businessinsights.onl_mob_daily_logins_temp_02
    WHERE login_date >= date'2022-08-01'
   GROUP BY
       cuid
)
group by trunc(last_capp_login_date))

select
a.dateid,
a.migrated_cnt,
b.total_capp_cnt,
sum(a.migrated_cnt) over(order by a.dateid asc) acc_migrated_cnt,
sum(b.total_capp_cnt) over(order by a.dateid asc) acc_capp_cnt
from migrated_to_gma a
inner join capp_user_base b
on a.DATEID= b.login_date