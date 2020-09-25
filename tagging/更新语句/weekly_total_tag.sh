#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
with temp_click as(
     SELECT * FROM u_analysis_dw.p3m_communication where status>0
 ),temp_channel as (
     select member_id,channel,c,row_number() over(partition by member_id order by c desc)rk FROM
     (select member_id,channel,count(1) c FROM temp_click group by member_id,channel) having rk=1 
 ),temp_dayparts as (
     select member_id,dayparts,c,row_number() over(partition by member_id order by c desc)rk FROM
     (select member_id,dayparts,count(1) c FROM temp_click group by member_id,dayparts) having rk=1 
 ),temp_channel_dayparts as (
select member_id,channel,dayparts,c,row_number() over(partition by member_id,channel order by c desc)rk FROM
     (select member_id,channel,dayparts,count(1) c FROM temp_click group by member_id,channel,dayparts) having rk=1 
 )
INSERT OVERWRITE TABLE u_analysis_app.communication_total_tag partition (pdate='${td}')
SELECT t1.member_id,t2.channel,t3.dayparts,t4.dayparts,t5.dayparts,t6.dayparts FROM (select distinct member_id FROM temp_click) t1
left join temp_channel t2 on t1.member_id=t2.member_id
left join temp_dayparts t3 on t1.member_id=t3.member_id
left join temp_channel_dayparts t4 on t1.member_id=t4.member_id and t4.channel=0
left join temp_channel_dayparts t5 on t1.member_id=t5.member_id and t5.channel=1
left join temp_channel_dayparts t6 on t1.member_id=t6.member_id and t6.channel=2;"