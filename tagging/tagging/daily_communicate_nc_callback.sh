#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
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


-----------------------------------------------------------------------
--  功能: 更新tagging   sms,push,email的指标 
--  修改日期: 2020-11-02
--  等NC回调稳定后上线
-----------------------------------------------------------------------
--  源表:u_analysis_dw.nc_callback_data
--       u_analysis_ods.sms_send
--       u_analysis_ods.sms_click
--       u_analysis_ods.callback_data
--       u_analysis_ods.edm_open_click_2
--  中间表：u_analysis_dw.p3m_communication
--  目标表: u_analysis_app.communication_detail_tag_daily
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：monthly
-----------------------------------------------------------------------

--原先数据清洗策略是发送1条，点击成功n条，通过row_number 设置优先级，如果有点击肯定保留点击最早的一条，现在为对接u_analysis_dw.nc_callback_data ，u_analysis_dw.nc_callback_data增加每条消息推送每种状态的rownumber排序，根据 row_Number() over(partition by recipient,batchid,messageid,status order by reportTime)rk 取reportTime最早的一条记录，即获取这条记录每种状态的回调数据里的最早一条
--因此后续tagging统计也需要调整，不然会有重复统计，比如发送次数只能看click=0 的数据 原先应该没有这部分判断
--2020-10-22后走nc回调数据
with temp_nc_callback_data as 
(
    --temp_nc_callback_data 表记录每条发送内容每种状态的回调数据里的最早一条 
    select *,row_number() over(partition by recipient,batchid,messageid,status order by reportTime)rk 
    FROM u_analysis_dw.nc_callback_data 
    WHERE pdate>='2020-10-22'     and pdate>=DATE_ADD('${td}',-91)  
    and reportTime >='2020-10-22' and reportTime>=DATE_ADD('${td}',-91) and reportTime<'${td}' 
    having rk=1
)
,temp_sms as
(
    --2020-10-22前短信的数据来自u_analysis_ods.sms_send和u_analysis_ods.sms_click,2020-10-22后的数据来自u_analysis_dw.nc_callback_data
    SELECT member_id,a.* FROM 
    (
        SELECT mobile as match_id,campaign_name str_1,campaign_name messageid,deliver_date as t,0 as click FROM 
        (
            select *,row_number() over(partition by mobile,campaign_name order by deliver_date) rk from u_analysis_ods.sms_send where campaign_name<>'campaignName' and deliver_date>=DATE_ADD('${td}',-91)  and deliver_date<'2020-10-22' having rk=1  
        ) 
        UNION ALL
        select mobile as match_id,campaign_name str_1,campaign_name messageid,click_date   as t,1 as click FROM 
        (
            select *,row_number() over(partition by mobile,campaign_name order by click_date) rk FROM u_analysis_ods.sms_click where campaign_name<>'campaignName' and click_date>=DATE_ADD('${td}',-91)    and click_date<'2020-10-22'   having rk=1
        )
        UNION ALL
        select recipient as match_id,batchid str_1,messageid,reportTime as t,case when status='1001' then 0 else 1 END click FROM temp_nc_callback_data where status in ('1001','1024')  
    )a 
    JOIN u_analysis_dw.siebel_member b on a.match_id=b.cell_ph_num
)
,temp_push as
(
    --2020-10-22前push的数据来自u_analysis_ods.callback_data，2020-10-22后的数据来自u_analysis_dw.nc_callback_data
    SELECT member_id,recipient as match_id,'' str_1,messageid,reportTime as t,case when status='2001' then 0 else 1 END click FROM 
    (
        select *,row_Number() over(partition by recipient,messageid,status order by reportTime)rk FROM u_analysis_ods.callback_data where pdate>= DATE_ADD('${td}',-90) and pdate<='2020-10-22' and reportTime>=DATE_ADD('${td}',-91) and reportTime<'${td}' and reportTime<'2020-10-22' --当天分区存的T+1
        AND status in ('2001','2002') having rk=1
    ) t1 
    join u_analysis_dw.siebel_member t2 on t1.recipient=t2.member_id
    UNION ALL
    SELECT member_id,recipient match_id,batchid str_1,messageid,reportTime as t,case when status='2001' then 0 else 1 END click FROM 
    temp_nc_callback_data  t1 join u_analysis_dw.siebel_member t2 on t1.recipient=t2.member_id and t1.status in ('2001','2002')
)
,temp_email as 
(
    --2020-10-22前email的数据来自u_analysis_ods.edm_open_click_2，2020-10-22后的数据来自u_analysis_dw.nc_callback_data
    SELECT member_id,email as match_id,dmdcampaignname str_1,concat(dmdcampaignname,DMDmailingName) messageid,t,click FROM 
    (
        select *,case when dmdtype='Open' then 1 else 2 END click,dmdlogdate t,
        row_number() over(partition by dmdcampaignname,DMDmailingName,dmdtype order by dmdlogdate)rk 
        FROM u_analysis_ods.edm_open_click_2  where dmdlogdate>=DATE_ADD('${td}',-91) and dmdlogdate<'2020-10-22'
    )t1 
    join u_analysis_dw.siebel_member t2 on t1.email=t2.email_addr
    UNION ALL
    SELECT member_id,recipient as match_id,get_json_object(params,'$.DMDcampaignName') str_1,messageid,reportTime t,case when status='3001' then 0 when status='3002' then 1 else 2 END click FROM 
    temp_nc_callback_data t1 join u_analysis_dw.siebel_member t2 on t1.recipient=t2.email_addr and t1.status in ('3001','3002','3003')
)
--将三种类型的数据整合进u_analysis_dw.p3m_communication表
INSERT OVERWRITE TABLE u_analysis_dw.p3m_communication
SELECT tb.*,
case 
when hour(event_time) between 2 and 10 then '0'
when hour(event_time) between 11 and 13 then '1'
when hour(event_time) between 14 and 16 then '2'
ELSE '3' END dayparts 
FROM
(
    SELECT member_id,match_id,'0' channel,click,str_1,messageid,'' str_3,t event_time,'${td}' run_date FROM temp_sms
    UNION ALL
    SELECT member_id,match_id,'1',click,str_1,messageid,'',t,'${td}' FROM temp_push
    UNION ALL
    SELECT member_id,match_id,'2',click,str_1,messageid,'',t,'${td}' FROM temp_email
)tb;
--根据不同的计算维度的需求进行计算
--p3m
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}' as pdate,channel,'p3m' as type from
(
    SELECT member_id,
    count(1) send,
    sum(case when status>0 then 1 end) ttl,
    sum(case when dayparts=0 and status=1 then 1 end) mor,
    sum(case when dayparts=1 and status=1 then 1 end) noo,
    sum(case when dayparts=2 and status=1 then 1 end) aft,
    sum(case when dayparts=3 and status=1 then 1 end) din,
    channel
    FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-91) group by member_id,channel 
)t1
left join (select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-91) and valid=1) t2 on t1.member_id=t2.member_id;
--p1m 
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}',channel,'p1m' type FROM
(
    SELECT member_id,
    count(1) send,
    sum(case when status>0 then 1 end) ttl,
    sum(case when dayparts=0 and status=1 then 1 end) mor,
    sum(case when dayparts=1 and status=1 then 1 end) noo,
    sum(case when dayparts=2 and status=1 then 1 end) aft,
    sum(case when dayparts=3 and status=1 then 1 end) din,
    channel
    FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-30) group by member_id,channel 
)t1
left join (select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-30) and valid=1) t2 on t1.member_id=t2.member_id;
--p7d
INSERT OVERWRITE TABLE u_analysis_app.communication_detail_tag_daily partition (pdate,channel,type)
select t1.member_id,send,ttl,mor,noo,aft,din,case when t2.member_id is not null and t1.ttl>0 then 1 else 0 end,'${td}',channel,'p7d' type FROM
(
    SELECT member_id,
    count(1) send,
    sum(case when status>0 then 1 end) ttl,
    sum(case when dayparts=0 and status=1 then 1 end) mor,
    sum(case when dayparts=1 and status=1 then 1 end) noo,
    sum(case when dayparts=2 and status=1 then 1 end) aft,
    sum(case when dayparts=3 and status=1 then 1 end) din,
    channel
    FROM u_analysis_dw.p3m_communication where date(event_time)>=date_add(current_date(),-7) group by member_id,channel 
)t1
left join(select distinct member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-7) and valid=1) t2 on t1.member_id=t2.member_id;
"