#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
-----------------------------------------------------------------------
--  功能: 每天更新券余额相关的tagging指标
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_dw.core_skrit_vchr
--  目标表: u_analysis_app.member_profile_vchr_balance_daily
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：daily
-----------------------------------------------------------------------

--计算两种券的余额，关键是u_analysis_dw.core_skrit_vchr的清洗
insert overwrite table u_analysis_app.member_profile_vchr_balance_daily partition(pdate='${td}',type ='core')
select member_id,count(*) core_balance from u_analysis_dw.core_skrit_vchr where type='core' and to_date(expiration_dt)>=current_date() and status_cd='Available'   group by member_id;
insert overwrite table u_analysis_app.member_profile_vchr_balance_daily partition(pdate='${td}',type ='srkit')
select member_id,count(*) core_balance from u_analysis_dw.core_skrit_vchr where type='srkit' and to_date(expiration_dt)>=current_date() and status_cd='Available'   group by member_id;
"