#!/bin/bash
td=`date +%Y-%m-%d`

echo $td

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
insert overwrite table u_analysis_temp.temp_hub_store 
select member_id,srv_prov_ou_id,max(order_dt) last_order_dt,sum(cast(valid as int)) as transact 
from u_analysis_dw.siebel_cx_order 
where valid<>0 
group by member_id,srv_prov_ou_id;
with temp_hub_store_num as(
select member_id,count(srv_prov_ou_id) hub_store_num from u_analysis_temp.temp_hub_store  p where p.transact>=2 group by member_id)
,temp_hub_store_member_id as(
select distinct member_id from u_analysis_temp.temp_hub_store 
) 
,temp_last_hub_store as( 
select member_id,srv_prov_ou_id as last_transact_hub_store,trade_area as last_transact_hub_store_type from (
select a.member_id,a.srv_prov_ou_id,a.last_order_dt,row_number() over(partition by a.member_id order by a.last_order_dt desc) rank,b.trade_area  
from u_analysis_temp.temp_hub_store a 
join u_analysis_dw.siebel_store b on b.store_id=a.srv_prov_ou_id and a.transact>=2)p 
where rank=1 )
insert overwrite table u_analysis_app.member_purchase_behavior_store  partition(pdate='${td}')
select a.member_id,case when c.hub_store_num is null then 0 else c.hub_store_num end,d.last_transact_hub_store,d.last_transact_hub_store_type
--join u_analysis_dw.siebel_store b on a.srv_prov_ou_id=b.store_id 
from  temp_hub_store_member_id a 
left join temp_hub_store_num c on a.member_id=c.member_id
left join temp_last_hub_store d on a.member_id=d.member_id;"


