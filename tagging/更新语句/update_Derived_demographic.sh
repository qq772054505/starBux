#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
use u_analysis_app;
WITH temp_daigou AS(
select distinct loy_member_id FROM u_analysis_ods.SIEBEL_DELIVERYACCOUNT
)
,temp_fraud as (
SELECT distinct member_id as loy_member_id FROM u_analysis_ods.temp_bound_svc_fraud
)
,temp_mem AS (SELECT t1.MEMBER_ID,level,Trans,Amount
FROM u_analysis_dw.siebel_member t1 
LEFT JOIN (select member_id,cast(SUM(VALID) as int) Trans,sum(cast(TOTAL_AMT as decimal(18,2))) Amount FROM u_analysis_dw.siebel_cx_order where valid<>0 AND pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}' group by member_id) t2 ON t1.MEMBER_ID = t2.MEMBER_ID
)
insert overwrite table u_analysis_app.member_profile_derived_demographic partition(pdate='${td}')
select a.member_id as member_id,
case when b.mem_type_cd='Employee' then 1 else 0 end parter,
case when c.loy_member_id is not null then 1 else 0 end fraud,
case when d.loy_member_id is not null then 1 else 0 end daigou,
level,
case 
when d.loy_member_id is not null and trans>0 then 'Daigou Active'
when d.loy_member_id is not null then 'Daigou Inactive'
when trans between 0 and 2 and level='Gold' then 'Gold 9K'
when trans between 3 and 5 and level='Gold' then 'Gold 14K'
when trans between 6 and 11 and level='Gold' then 'Gold 18K'
when trans between 12 and 23 and level='Gold' then 'Gold 24K'
when trans >=24 and level='Gold' then 'Black Gold'
when c.loy_member_id is not null and trans>0 then 'Fraud Active'
when c.loy_member_id is not null then 'Fraud Inactive'
when level='Gold' then 'Gold inactive' 
when level='Green' and trans>0 then 'Green Active'
when level='Green' then 'Green Inactive'
when level='Welcome' and trans>0 then 'Welcome Active'
when level='Welcome' then 'Welcome Inactive'
END sub_tier
from temp_mem a 
left join u_analysis_temp.siebel_vw_s_loy_member b on a.member_id=b.row_id and b.status_cd='Active' 
left join temp_fraud c    on a.member_id=c.loy_member_id
left join temp_daigou d  on a.member_id=d.loy_member_id;"