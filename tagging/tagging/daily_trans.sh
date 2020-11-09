#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
-----------------------------------------------------------------------
--  功能: 每天更新关于单数的tagging指标
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_dw.siebel_member
--       u_analysis_dw.siebel_cx_order
--       u_analysis_dw.oms_order 
--  目标表: u_analysis_app.member_purchase_behavior_trans_daily
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：daily
-----------------------------------------------------------------------

--分别计算两个渠道不同时间范围的总单数
--mop p3m
insert overwrite table u_analysis_app.member_purchase_behavior_trans_daily partition (pdate='${td}',type='P3M',channel='mop')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('MOP','MOP Return','Ali Koubei MOP','Ali Koubei MOP RTN','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') and valid<>0 and pdate between date_add(current_date,-91) and date_add(current_date,-1)  group by member_id;
--mod p3m
insert overwrite table u_analysis_app.member_purchase_behavior_trans_daily partition (pdate='${td}',type='P3M',channel='mod')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('App Delivery','App Return','Delivery MO','Wechat Delivery','Wechat Delivery RTN','Eleme','Eleme Return') and valid<>0 and pdate between date_add(current_date,-91) and date_add(current_date,-1)  group by member_id;
--mop historical
insert overwrite table u_analysis_app.member_purchase_behavior_trans_daily partition(pdate='${td}',type='all',channel='mop')
select member_id,trans from u_analysis_dw.siebel_member a join 
(select user_id,count(*) trans from  u_analysis_dw.oms_order  where status='5' and channel in('ALI-MOP','MOP') group by user_id)b 
on a.member_id=b.user_id;
--mod historical
insert overwrite table u_analysis_app.member_purchase_behavior_trans_daily partition(pdate='${td}',type='all',channel='mod')
select member_id,trans from u_analysis_dw.siebel_member a join
(select user_id,count(*) trans from  u_analysis_dw.oms_order  where status='5' and channel in('MOD','ELE','WSG-MOD') group by user_id)b
on a.member_id=b.user_id;"


