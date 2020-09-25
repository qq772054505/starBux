star&p3m_communication

set p30d=2019-05-01;

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

INSERT OVERWRITE TABLE u_analysis_dw.star_redemption_order PARTITION (pdate)
SELECT T2.ORDER_ID,
T1.ORDER_DT,
T1.COMMIT_TYPE_CD,
'STAR',
T1.SRV_PROV_OU_ID,
SUM(CASE WHEN PAY_AMT<0 THEN -1 ELSE 1 END *
CASE 
WHEN T3.CODE IN('SR-1SRedeem','SR-1SRCondi') THEN 1
WHEN T3.CODE IN('SR-9SRedeem','MSR-9SRedeem') THEN 9
WHEN T3.CODE IN('SR-15SRedeem') AND T7.PART_NUM IS NULL THEN 15 
WHEN T3.CODE IN('SR-15SRedeem') AND T7.PART_NUM IS NOT NULL THEN 3.75
--WHEN T3.CODE IN('SR-25SRedeem') THEN 25
WHEN T3.CODE IN('SR-12SRedeem') THEN 12
WHEN T3.CODE IN('SR-9+1SRedeem') THEN 10
--WHEN T3.CODE IN('SR-78OFF') THEN 25
END) AS STAR,
SUM(PAY_AMT) AS AMOUNT,
t1.pdate
FROM u_analysis_dw.siebel_cx_order t1 join
u_analysis_dw.siebel_cx_src_payment t2 
on t1.row_id=t2.order_id and t1.pdate>='${p30d}'
and t2.pdate>='${p30d}'
AND t1.valid=1
JOIN u_analysis_temp.siebel_cx_payment_type T3 ON T2.TYPE_CD = T3.TYPE AND T2.PAID_BY = T3.SOURCE_CODE AND T2.INVOICE_NUM = T3.SERIAL_NUM
AND T2.TYPE_CD = 'Discount' AND T3.CODE in('SR-1SRedeem','SR-1SRCondi','SR-9SRedeem','MSR-9SRedeem','SR-15SRedeem','SR-12SRedeem','SR-9+1SRedeem')
JOIN u_analysis_dw.SIEBEL_CX_ORDER_ITEM T4 ON T2.INTEGRATION_ID = T4.ROW_ID
JOIN u_analysis_temp.SIEBEL_S_PROD_INT T5 ON T4.PROD_ID = T5.ROW_ID 
LEFT JOIN u_analysis_ods.NOVA_MINIITEM T7 ON T5.PART_NUM = T7.PART_NUM
GROUP BY T2.ORDER_ID,T1.ORDER_DT,T1.COMMIT_TYPE_CD,T1.SRV_PROV_OU_ID,t1.pdate
HAVING SUM(CASE WHEN PAY_AMT<0 THEN -1 ELSE 1 END * ABS(cast(REQUEST_AMT as decimal(14,2)))) >0 AND AMOUNT>0
UNION ALL
select t3.row_id order_id,t3.order_dt,t3.commit_type_cd,'SR-78OFF',t3.SRV_PROV_OU_ID,25,78,t3.pdate
FROM u_analysis_app.s_mem_vchr_one_year t1 join u_analysis_temp.siebel_s_loy_txn t2 join u_analysis_dw.siebel_cx_order t3
on t1.PROD_ID IN('1-41HXQZQP', '1-41HXQZRI') and t1.status_cd='Used' and t1.consumed_txn_id = t2.row_id and t3.valid=1
and t2.orig_order_id = t3.row_id
and t2.pdate>='${p30d}'
and t3.pdate>='${p30d}'
UNION ALL
SELECT t3.row_id order_id,t3.order_dt,t3.commit_type_cd,'STAR',t3.SRV_PROV_OU_ID,stars_used,star_discount*0.01,t3.pdate
FROM u_analysis_dw.oms_order t1 join u_analysis_dw.siebel_cx_order t3 on t1.pdate>='${p30d}' and t1.channel='MOP' and t1.status='5' and t1.stars_used>0 and t1.star_discount>0 and t3.pdate>='${p30d}' and t3.valid=1 and t3.integration_id=t1.id and t3.commit_type_cd in ('MOP','Ali Koubei MOP','APP MOP PreOrder Today')
distribute by pdate,cast(rand()*2 as int);


--参加活动次数

with temp_star as(select member_id,count(distinct promo_id) participate_num FROM u_analysis_temp.siebel_s_loy_acrl_itm where process_dt>=date_add(current_date,-91) group by member_id),
temp_use as (select t2.member_id,count(distinct order_id) redeem_time,sum(redeem_num) redeem_num,sum(save_amt) save_amt,sum(total_amt) actual_pay_amt FROM (select order_id,sum(star) redeem_num,sum(star_discount) save_amt FROM u_analysis_dw.star_redemption_order where pdate>=date_add(current_date,-91) group by order_id) t1 join (select row_id,total_amt,member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-91)) t2 on t1.order_id = t2.row_id group by t2.member_id)
INSERT OVERWRITE TABLE u_analysis_app.star_tag partition(pdate='2020-08-09')
SELECT t1.member_id,t2.participate_num,t3.redeem_time,t3.redeem_num,t3.actual_pay_amt,t3.save_amt FROM 
u_analysis_dw.siebel_member t1
left join temp_star t2 on t1.member_id = t2.member_id
left join temp_use t3 on t1.member_id = t3.member_id
where t2.member_id is not null or t3.member_id is not null;

CREATE EXTERNAL TABLE `star_tag`(
  `member_id` string, 
  `participate_num` int, 
  `redeem_time` int, 
  `redeem_num` decimal(14,4), 
  `actual_pay_amt` decimal(14,2),
  `save_amt` decimal(14,2)
  )
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/public/app/star_tag'
;




--communication
set td=2020-08-09;
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
),temp_email as (SELECT member_id,email as match_id,concat(dmdcampaignname,DMDmailingName) messageid,t,click FROM (select *,case when dmdtype='Open' then 1 else 2 END click,from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') t,row_number() over(partition by dmdcampaignname,DMDmailingName order by case when dmdtype='Open' then 2 else 1 END desc,from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'))rk FROM u_analysis_ods.edm_open_click_2 where pdate>= DATE_ADD('${td}',-91) and from_unixtime(unix_timestamp(dmdlogdate,'yyyy/M/d HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')>=DATE_ADD('${td}',-91)) t1 join u_analysis_dw.siebel_member t2 on t1.email=t2.email_addr
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


drop table communication_total_tag;
CREATE EXTERNAL TABLE `u_analysis_app`.`communication_total_tag`(
  `member_id` string, 
  `prefer_channel` string, 
  `prefer_daypart` string, 
  `sms_prefer_daypart` string, 
  `push_prefer_daypart` string,
  `email_prefer_daypart` string
  )
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/public/app/communication_total_tag'
;

CREATE EXTERNAL TABLE `u_analysis_app`.`communication_detail_tag`(
  `member_id` string, 
  `send` int, 
  `click` int, 
  `morning_click` int, 
  `noon_click` int,
  `afternoon_click` int,
  `dinner_click` int,
  `click_with_purchase` string
  )
PARTITIONED BY ( 
  `pdate` string,
  `channel` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/public/app/communication_detail_tag'
;

set td=2020-08-09;
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag partition (pdate,channel)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}',channel FROM
(SELECT member_id,count(1) send,sum(case when status>0 then 1 end) ttl,
sum(case when dayparts=0 and status=1 then 1 end) mor,
sum(case when dayparts=1 and status=1 then 1 end) noo,
sum(case when dayparts=2 and status=1 then 1 end) aft,
sum(case when dayparts=3 and status=1 then 1 end) din,
channel
 FROM u_analysis_dw.p3m_communication group by member_id,channel)t1
 left join
 (select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-91) and valid=1) t2
 on t1.member_id=t2.member_id;

set td=2020-08-09;
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
left join temp_channel_dayparts t6 on t1.member_id=t6.member_id and t6.channel=2;

