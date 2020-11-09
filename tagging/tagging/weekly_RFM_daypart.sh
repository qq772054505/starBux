#!/bin/bash
td=`date +%Y-%m-%d`

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
with temp_day_part_diversity as
(select member_id,count(distinct p.week_part_day_part) day_part_diversity from  
(select member_id,concat(case when date_format(order_dt,'u') between 1 and 5 then 'workday' else 'weekend' end,
case 
when hour(order_dt) between 2 and 10 then 'Morning'
when hour(order_dt) between 11 and 13 then 'Noon'
when hour(order_dt) between 14 and 16 then 'Afternoon'
else 'Dinner' end) as week_part_day_part
from u_analysis_dw.siebel_cx_order where pdate  between date_add(current_date(),-91) and date_add(current_date(),-1)) p  group by member_id)
,temp_brand_love_index as
(select  OI.MEMBER_ID member_id,
count(distinct OI.X_CATEGORY_CD) CATEGORIES
from  u_analysis_dw.SIEBEL_ORDER_ITEMS OI 
where (OI.X_B3G1_FLG='Y' or OI.X_DEPARTMENT_CD in('FESTIVAL FOOD','FOOD','MERCHANDISE','WHOLE BEAN'))
and  pdate between date_add(current_date(),-91) and date_add(current_date(),-1)
group by OI.MEMBER_ID)
,temp_rfm as
(select member_id,
cast(SUM(VALID) as int)  Trans,
sum(cast(TOTAL_AMT as decimal(18,2)))  Spend,
cast(sum(cast(TOTAL_AMT as decimal(18,2)))/sum(1) as decimal(18,2))  average_at,
max(cast(total_amt as decimal(18,2)))   highest_at
from u_analysis_dw.siebel_cx_order   
where valid<>0 and pdate between date_add(current_date(),-91) and date_add(current_date(),-1)
group by member_id)
insert overwrite table u_analysis_app.member_purchase_behavior_RFM_daypart partition(pdate='${td}',type='P3M')  
select a.*,b.day_part_diversity,c.CATEGORIES from temp_rfm a 
join temp_day_part_diversity b on a.member_id=b.member_id
left join temp_brand_love_index   c on a.member_id=c.member_id;"