#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
-----------------------------------------------------------------------
--  功能: 更新tagging中关于商品明细的指标
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_dw.siebel_order_items
--       u_analysis_dw.siebel_cx_order
--  目标表: u_analysis_app.member_purchase_behavior_category 
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：weekly
-----------------------------------------------------------------------


--连接order表和order_item表 根据不同的商品类别和时间范围计算指标
--bev p3m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P3M',category='0')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid=1   and x_b3g1_flg='Y'
where a.pdate between date_add(current_date,-91) and date_add(current_date,-1)  and b.pdate between date_add(current_date,-91) and date_add(current_date,-1) 
group by a.member_id; 
--bev p6m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P6M',category='0')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid<>0   and x_b3g1_flg='Y'
where a.pdate  between date_add(current_date,-182) and date_add(current_date,-1)  and b.pdate  between date_add(current_date,-182) and date_add(current_date,-1) 
group by a.member_id; 
--food p3m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P3M',category='1')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid<>0   and x_department_cd in ('FOOD','PRINCI FOOD')
where a.pdate  between date_add(current_date,-91) and date_add(current_date,-1)  and b.pdate  between date_add(current_date,-91) and date_add(current_date,-1) 
group by a.member_id; 
--food p6m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P6M',category='1')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid<>0   and x_department_cd in ('FOOD','PRINCI FOOD')
where a.pdate  between date_add(current_date,-182) and date_add(current_date,-1)  and b.pdate  between date_add(current_date,-182) and date_add(current_date,-1)  
group by a.member_id; 
--merch p3m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P3M',category='2')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid<>0   and x_department_cd='MERCHANDISE'
where a.pdate  between date_add(current_date,-91) and date_add(current_date,-1)  and b.pdate  between date_add(current_date,-91) and date_add(current_date,-1) 
group by a.member_id; 
--merch p6m
insert overwrite table u_analysis_app.member_purchase_behavior_category partition(pdate='${td}',type='P6M',category='2')
select a.member_id,
sum(cast(amount as decimal(18,2)))  Spend,
count(distinct order_id) transaction,
sum(cast(quantity as int)) item_size  
from u_analysis_dw.siebel_order_items a
join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id and valid<>0   and x_department_cd='MERCHANDISE'
where a.pdate  between date_add(current_date,-182) and date_add(current_date,-1)  and b.pdate  between date_add(current_date,-182) and date_add(current_date,-1) 
group by a.member_id;"