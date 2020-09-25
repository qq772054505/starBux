#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
--communication
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.optimize.sort.dynamic.partition=true;
set spark.sql.shuffle.partitions=400;

with temp_sms as(
SELECT member_id,a.*,row_number() over(partition by member_id,match_id,campaign_name order by click desc,t)rk FROM 
(SELECT mobile as match_id,campaign_name,deliver_date as t,0 as click FROM u_analysis_ods.sms_send where campaign_name<>'campaignName' and deliver_date>=DATE_ADD('${td}',-91)
UNION ALL
select mobile as match_id,campaign_name,click_date as t,1 as click FROM u_analysis_ods.sms_click where campaign_name<>'campaignName' and click_date>=DATE_ADD('${td}',-91)
) a 
JOIN u_analysis_dw.siebel_member b 
on a.match_id=b.cell_ph_num
having rk=1
),temp_push as(
SELECT member_id,recipient as match_id,messageid,reportTime as t,case when status='2001' then 0 else 1 END click FROM (select *,row_number() over(partition by recipient,messageid,pdate order by status desc,reportTime)rk FROM u_analysis_ods.callback_data where pdate>= DATE_ADD('${td}',-90) and requestTime>=DATE_ADD('${td}',-91) and requestTime<'${td}'--当天分区存的T+1
AND status in ('2001','2002') having rk=1) t1 join u_analysis_dw.siebel_member t2 on t1.recipient=t2.member_id
),
temp_email as (SELECT member_id,email as match_id,concat(dmdcampaignname,DMDmailingName) messageid,t,click FROM (select *,case when dmdtype='Open' then 1 else 2 END click,from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') t,row_number() over(partition by dmdcampaignname,DMDmailingName order by case when dmdtype='Open' then 2 else 1 END desc,from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'))rk FROM u_analysis_ods.edm_open_click_2 where pdate>= DATE_ADD('${td}',-91) and from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')>=DATE_ADD('${td}',-91)) t1 join u_analysis_dw.siebel_member t2 on t1.email=t2.email_addr
)
INSERT OVERWRITE TABLE u_analysis_dw.p3m_communication
SELECT tb.*,case 
when hour(event_time) between 2 and 10 then '0'
when hour(event_time) between 11 and 13 then '1'
when hour(event_time) between 14 and 16 then '2'
ELSE '3' END dayparts FROM
(SELECT member_id,match_id,'0' channel,click,campaign_name str_1,'' str_2,'' str_3,t event_time,'${td}' run_date FROM temp_sms
UNION ALL
SELECT member_id,match_id,'1',click,messageid,'','',t,'${td}' FROM temp_push
UNION ALL
SELECT member_id,match_id,'2',click,messageid,'','',t,'${td}' FROM temp_email) tb;




--p3m
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}' as pdate,channel,'p3m' as type from 
 (SELECT member_id,count(1) send,sum(case when status>0 then 1 end) ttl,
sum(case when dayparts=0 and status=1 then 1 end) mor,
sum(case when dayparts=1 and status=1 then 1 end) noo,
sum(case when dayparts=2 and status=1 then 1 end) aft,
sum(case when dayparts=3 and status=1 then 1 end) din,
channel
FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-91) group by member_id,channel )t1
left join
(select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-91) and valid=1) t2
on t1.member_id=t2.member_id;







--p1m 
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}',channel,'p1m' type FROM
(SELECT member_id,count(1) send,sum(case when status>0 then 1 end) ttl,
sum(case when dayparts=0 and status=1 then 1 end) mor,
sum(case when dayparts=1 and status=1 then 1 end) noo,
sum(case when dayparts=2 and status=1 then 1 end) aft,
sum(case when dayparts=3 and status=1 then 1 end) din,
channel
FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-30) group by member_id,channel )t1
left join
(select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-30) and valid=1) t2
on t1.member_id=t2.member_id;
 
--p7d
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}',channel,'p7d' type FROM
(SELECT member_id,count(1) send,sum(case when status>0 then 1 end) ttl,
sum(case when dayparts=0 and status=1 then 1 end) mor,
sum(case when dayparts=1 and status=1 then 1 end) noo,
sum(case when dayparts=2 and status=1 then 1 end) aft,
sum(case when dayparts=3 and status=1 then 1 end) din,
channel
FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-7) group by member_id,channel )t1
left join
(select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-7) and valid=1) t2
on t1.member_id=t2.member_id;
"
