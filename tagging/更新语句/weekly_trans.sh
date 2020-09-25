#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
--mop p1m
insert overwrite table u_analysis_app.member_purchase_behavior_trans partition (pdate='${td}',type='P1M',channel='mop')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('MOP','MOP Return','Ali Koubei MOP','Ali Koubei MOP RTN','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') and valid<>0 and pdate between date_add(current_date,-30) and date_add(current_date,-1)  group by member_id;
--mod p1m
insert overwrite table u_analysis_app.member_purchase_behavior_trans partition (pdate='${td}',type='P1M',channel='mod')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('App Delivery','App Return','Delivery MO','Wechat Delivery','Wechat Delivery RTN','Eleme','Eleme Return') and valid<>0 and pdate between date_add(current_date,-30) and date_add(current_date,-1)  group by member_id;
--mop p6m
insert overwrite table u_analysis_app.member_purchase_behavior_trans partition (pdate='${td}',type='P6M',channel='mop')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('MOP','MOP Return','Ali Koubei MOP','Ali Koubei MOP RTN','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') and valid<>0 and pdate between date_add(current_date,-182) and date_add(current_date,-1)  group by member_id;
--mod p6m
insert overwrite table u_analysis_app.member_purchase_behavior_trans partition (pdate='${td}',type='P6M',channel='mod')
select member_id,sum(cast(valid as int)) trans from u_analysis_dw.siebel_cx_order where commit_type_cd in('App Delivery','App Return','Delivery MO','Wechat Delivery','Wechat Delivery RTN','Eleme','Eleme Return') and valid<>0 and pdate between date_add(current_date,-182) and date_add(current_date,-1) group by member_id;"