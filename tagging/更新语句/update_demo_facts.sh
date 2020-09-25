#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e "
use u_analysis_app;
insert overwrite table u_analysis_app.member_profile_Demo_facts partition(pdate='${td}') 
select  member_id,age,birth_dt,case sex_mf when 'Female' then 1 when 'Male' then 0 else null end,city_tier,homecity,reg_channel_cd,submit_Dt  from u_analysis_dw.siebel_member;"