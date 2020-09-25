#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
with temp_free_delivery as 
(select a.member_id,count(*) trans from u_analysis_dw.siebel_cx_order a 
join u_analysis_dw.siebel_order_items b on a.row_id=b.order_id 
where a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date() and b.x_category_cd='DELIVERY' and b.amount=0 and a.valid=1 group by a.member_id)
insert overwrite table u_analysis_app.member_campaign_perference_free_delivery partition(pdate='${dt}') 
select a.member_id,trans,9*trans from u_analysis_dw.siebel_member a join temp_free_delivery b on a.member_id=b.member_id;" 






