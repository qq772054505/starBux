#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
--morning
insert overwrite table u_analysis_app.member_daypart_spend partition(pdate='${td}',daypart='1',type='P3M')  
select member_id,sum(cast(TOTAL_AMT as decimal(18,2)))  as spend from u_analysis_dw.siebel_cx_order 
where pdate between date_add(current_date(),-91) and date_add(current_date(),-1) and valid=1 and hour(order_dt) between 2 and 10
group by member_id;  
--noon
insert overwrite table u_analysis_app.member_daypart_spend partition(pdate='${td}',daypart='2',type='P3M')  
select member_id,sum(cast(TOTAL_AMT as decimal(18,2)))  as spend from u_analysis_dw.siebel_cx_order 
where pdate between date_add(current_date(),-91) and date_add(current_date(),-1) and valid=1 and hour(order_dt) between 11 and 13
group by member_id;  
--afternoon
insert overwrite table u_analysis_app.member_daypart_spend partition(pdate='${td}',daypart='3',type='P3M')  
select member_id,sum(cast(TOTAL_AMT as decimal(18,2)))  as spend from u_analysis_dw.siebel_cx_order 
where pdate between date_add(current_date(),-91) and date_add(current_date(),-1) and valid=1 and hour(order_dt) between 14 and 16
group by member_id;  
--dinner
insert overwrite table u_analysis_app.member_daypart_spend partition(pdate='${td}',daypart='4',type='P3M')  
select member_id,sum(cast(TOTAL_AMT as decimal(18,2)))  as spend from u_analysis_dw.siebel_cx_order 
where pdate between date_add(current_date(),-91) and date_add(current_date(),-1) and valid=1 and (hour(order_dt) >= 17 or hour(order_dt) <=1)
group by member_id;"  