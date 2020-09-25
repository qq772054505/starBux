#!/bin/bash
td=`date +%Y-%m-%d`
p3m=`date -d '-91 day' +%Y-%m-%d`

echo $td
echo $p3m


#all
lastpdate=`/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 4g --conf spark.sql.broadcastTimeout=7200 --conf spark.port.maxRetries=1000 -e "select max(distinct pdate) from u_analysis_app.member_purchase_behavior_recency where channel='all';"| tail -1`

echo $lastpdate

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
use u_analysis_temp;
with temp_order_recency as 
(select * from u_analysis_Dw.siebel_cx_order where pdate>='${p3m}' and pdate <'${td}' and  valid=1 )
insert overwrite table u_analysis_temp.temp_recency
select * from( 
select member_id,row_id,recency from u_analysis_app.member_purchase_behavior_recency where pdate='${lastpdate}' and channel='all'
union all 
select member_id,row_id,order_dt as recency from temp_order_recency)p;
insert overwrite table u_analysis_app.member_purchase_behavior_recency	 partition(pdate='${td}',channel='all')
select member_id,order_id,date_format(recency,'yyyy-MM-dd hh:mm:ss') from(
select *,row_number() over (partition by member_id  order by recency desc) rank from u_analysis_temp.temp_recency)p where rank=1;


with temp_Avg_transaction_interval as 
(select member_id,cast(avg(datediff(order_dt,last_order_dt))as decimal(10,2)) as avg_transaction_interval from(
select member_id,order_dt,lead(order_dt) over(partition by member_id order by order_dt desc) last_order_dt from u_analysis_dw.siebel_cx_order where pdate between date_add(current_date(),-91) and current_date()  and  valid=1) t1
group by member_id)
insert overwrite table u_analysis_app.member_purchase_behavior_RFM_daily partition(pdate='${td}')
select a.member_id,a.recency_days,b.avg_transaction_interval,cast((b.avg_transaction_interval/a.recency_days) as decimal(10,5))  as Personal_Active_index 
from  (select member_id,datediff(current_date(),recency) as recency_days from u_analysis_app.member_purchase_behavior_recency where channel='all' and pdate='${td}') a 
left join temp_Avg_transaction_interval b on  a.member_id=b.member_id;"


