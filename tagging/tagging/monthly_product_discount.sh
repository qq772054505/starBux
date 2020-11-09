#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
-----------------------------------------------------------------------
--  功能: 更新tagging三个商品折扣指标的数据
--  P3M of transactions with product discount
--  P3M of products purchased
--  P3M cost saved via single product discount
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表: u_analysis_dw.siebel_cx_order
--        u_analysis_dw.siebel_cx_order_item
--  目标表: u_analysis_app.member_campaign_perference_product_discount
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：monthly
-----------------------------------------------------------------------

--创建临时表，计算折扣总单数
with temp_product_discount_trans as 
(
    select p.member_id,count(p.order_id) as  product_discount_trans from 
    (
        select distinct b.member_id,b.order_id
        from u_analysis_dw.siebel_cx_order a 
        join u_analysis_dw.siebel_cx_order_item b  on a.row_id=b.order_id and a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date() 
        where b.rollup_pri>b.discnt_amt and a.valid=1
    )p 
    group by p.member_id
)
--创建临时表，计算折扣物品清单，以逗号分隔。
,temp_prod_purchase as 
(
    select p.member_id,concat_ws(',',collect_set(prod_id)) as prod_purchase  from 
    (
        select distinct b.member_id,prod_id
        from u_analysis_dw.siebel_cx_order a 
        join u_analysis_dw.siebel_cx_order_item b  on a.row_id=b.order_id and a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date() 
        where b.rollup_pri>b.discnt_amt and a.valid=1
    )p 
    group by p.member_id
)
--创建临时表，计算折扣总金额
,temp_cost_saved as 
(
    select b.member_id,cast(sum(b.rollup_pri-b.discnt_amt) as decimal(10,2))  as cost_saved
    from u_analysis_dw.siebel_cx_order a 
    join u_analysis_dw.siebel_cx_order_item b  on a.row_id=b.order_id and a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date() 
    where b.rollup_pri>b.discnt_amt and a.valid=1  
    group by b.member_id
)
--连接临时表
insert overwrite  table u_analysis_app.member_campaign_perference_product_discount partition(pdate='${td}')
select a.member_id,a.product_discount_trans,b.prod_purchase,c.cost_saved 
from temp_product_discount_trans a 
join temp_prod_purchase b on a.member_id=b.member_id 
join temp_cost_saved c on a.member_id=c.member_id;
"






