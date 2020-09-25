#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
insert overwrite table u_analysis_app.member_profile_vchr_balance_daily partition(pdate='${td}',type ='core')
select member_id,count(*) core_balance from u_analysis_dw.core_skrit_vchr where type='core' and to_date(expiration_dt)>=current_date() and status_cd='Available'   group by member_id;
insert overwrite table u_analysis_app.member_profile_vchr_balance_daily partition(pdate='${td}',type ='srkit')
select member_id,count(*) core_balance from u_analysis_dw.core_skrit_vchr where type='srkit' and to_date(expiration_dt)>=current_date() and status_cd='Available'   group by member_id;"