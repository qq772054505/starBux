#!/bin/bash
td=`date +%Y-%m-%d`
p3m=`date -d '-91 day' +%Y-%m-%d`

echo $td
echo $p3m


#all
lastpdate=`   /usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 4g --conf spark.sql.broadcastTimeout=7200 --conf spark.port.maxRetries=1000 -e "select max(distinct pdate) from u_analysis_app.member_purchase_behavior_recency where channel='all';"      | tail -1`

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
insert overwrite table u_analysis_app.member_purchase_behavior_recency partition(pdate='${td}',channel='all')
select member_id,order_id,date_format(recency,'yyyy-MM-dd hh:mm:ss') from(
select *,row_number() over (partition by member_id  order by recency desc) rank from u_analysis_temp.temp_recency)p where rank=1;"




#mop
lastpdate=`/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 4g --conf spark.sql.broadcastTimeout=7200 --conf spark.port.maxRetries=1000 -e "select max(distinct pdate) from u_analysis_app.member_purchase_behavior_recency where channel='mop';"| tail -1`

echo $lastpdate

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
use u_analysis_temp;
with temp_order_recency as 
(select * from u_analysis_Dw.siebel_cx_order where pdate>='${p3m}' and pdate <'${td}' and  valid=1 and commit_type_cd in('MOP','Ali Koubei MOP','APP MOP PreOrder Today'))
insert overwrite table u_analysis_temp.temp_recency
select * from( 
select member_id,row_id,recency from u_analysis_app.member_purchase_behavior_recency where pdate='${lastpdate}' and channel='mop'
union all
select member_id,row_id,order_dt as recency from temp_order_recency)p;
insert overwrite table u_analysis_app.member_purchase_behavior_recency partition(pdate='${td}',channel='mop')
select member_id,order_id,date_format(recency,'yyyy-MM-dd hh:mm:ss') from(
select *,row_number() over (partition by member_id  order by recency desc) rank from u_analysis_temp.temp_recency)p where rank=1;"





#mod
lastpdate=`/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 4g --conf spark.sql.broadcastTimeout=7200 --conf spark.port.maxRetries=1000 -e "select max(distinct pdate) from u_analysis_app.member_purchase_behavior_recency where channel='mod';"| tail -1`

echo $lastpdate

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
use u_analysis_temp;
with temp_order_recency as 
(select * from u_analysis_Dw.siebel_cx_order where pdate>='${p3m}' and pdate <'${td}' and  valid=1 and commit_type_cd in('App Delivery','App Return','Delivery MO','Wechat Delivery','Wechat Delivery RTN','Eleme','Eleme Return'))
insert overwrite table u_analysis_temp.temp_recency
select * from( 
select member_id,row_id,recency from u_analysis_app.member_purchase_behavior_recency where pdate='${lastpdate}' and channel='mod'
union all
select member_id,row_id,order_dt as recency from temp_order_recency)p;
insert overwrite table u_analysis_app.member_purchase_behavior_recency partition(pdate='${td}',channel='mod')
select member_id,order_id,date_format(recency,'yyyy-MM-dd hh:mm:ss') from(
select *,row_number() over (partition by member_id  order by recency desc) rank from u_analysis_temp.temp_recency)p where rank=1;"













