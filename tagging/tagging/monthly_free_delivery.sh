#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
-----------------------------------------------------------------------
--  功能: 更新tagging两个免配送费指标的数据
--  1.P3M of transactions with free delivery
--  2.P3M cost saved via free delivery
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_dw.siebel_cx_order
--       u_analysis_dw.siebel_order_items
--       u_analysis_dw.siebel_member
--  目标表: u_analysis_app.member_campaign_perference_free_delivery
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
-----------------------------------------------------------------------

--创建临时表，在order_item中DELIVERY类型且amount为0的数据，即为免配送费的数据
with temp_free_delivery as 
(
    select a.member_id,count(*) trans 
    from u_analysis_dw.siebel_cx_order a 
    join u_analysis_dw.siebel_order_items b on a.row_id=b.order_id 
    where a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date() 
    and b.x_category_cd='DELIVERY' and b.amount=0 and a.valid=1 group by a.member_id
)
--每单配送费默认是9元
insert overwrite table u_analysis_app.member_campaign_perference_free_delivery partition(pdate='${dt}') 
select a.member_id,trans,9*trans 
from u_analysis_dw.siebel_member a 
join temp_free_delivery b on a.member_id=b.member_id;
" 






