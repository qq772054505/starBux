#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
with temp_bev_party_size as 
(select order_id,sum(cast(quantity as int)) bev_party_size 
from u_analysis_dw.siebel_order_items 
where x_b3g1_flg='Y' and pdate between date_add(current_date,-91) and date_add(current_date,-1) group by order_id)
/*,temp_coupon_redeem as
(select distinct b.orig_order_Id  
from u_analysis_dw.siebel_s_loy_mem_vchr_used a
join u_analysis_temp.siebel_s_Loy_txn b on a.consumed_txn_id=b.row_id   
where b.pdate between date_add(current_date,-91) and date_add(current_date,-1) and a.pdate between date_add(current_date,-91) and date_add(current_date,-1))*/
,temp_coupon_redeem as
(select row_id,concat_ws(',',collect_set(part_num)) as coupon_redeem from
(select a.row_id,c.part_num  
from u_analysis_dw.siebel_cx_order a
join u_analysis_temp.siebel_s_Loy_txn b on  a.row_id=b.orig_order_Id 
join u_analysis_dw.siebel_s_loy_mem_vchr_used c on c.consumed_txn_id=b.row_id  
where b.pdate between date_add(current_date,-91) and date_add(current_date,-1) and a.pdate between date_add(current_date,-91) and date_add(current_date,-1)  and c.pdate between date_add(current_date,-91) and date_add(current_date,-1)) 
group by row_id)
,TEMP_distinct_prod_id as
(select a.row_id,b.prod_id,row_number() OVER(PARTITION by a.row_id,b.prod_id order by a.order_dt) rank
from u_analysis_dw.siebel_cx_order a join u_analysis_dw.siebel_order_items b on a.row_id=b.order_id and b.pdate between date_add(current_date,-91) and date_add(current_date,-1) and a.pdate between date_add(current_date,-91) and date_add(current_date,-1)
where(b.X_B3G1_FLG='Y' or b.X_DEPARTMENT_CD in('FESTIVAL FOOD','FOOD','MERCHANDISE','WHOLE BEAN','PACKAGED FOOD','Packaged Tea','PACKAGING','PRINCI FOOD','PRINCI MERCHANDISE','VIA','WHOLE CAKE'))
having rank=1)
,Temp_prod_collect as(
select row_id,concat_ws(',',collect_set(prod_id)) as prod_collect from TEMP_distinct_prod_id group by row_id) 
,temp_order_first_second
(select *,row_number() over(partition by member_id order by order_dt) rank from u_analysis_dw.siebel_cx_order 
where valid=1 and pdate between date_add(current_date,-91) and date_add(current_date,-1)  having rank<=2)
,temp_new_order(
select a.member_id,
a.row_id as order_id,
case when b.bev_party_size is null then 0 else b.bev_party_size end bev_party_size,
c.trade_area as store_type,
case 
when hour(order_dt) between 2 and 10 then 'Morning'
when hour(order_dt) between 11 and 13 then 'Noon'
when hour(order_dt) between 14 and 16 then 'Afternoon'
else 'Dinner' end day_part,
order_dt,
d.coupon_redeem,
concat(cast(discnt_amt/(total_amt+discnt_amt)*100 as int),'%') discount_rato,
discnt_amt as cost_save,
e.prod_collect,
case 
when a.commit_type_cd in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null') then 'store'
when a.commit_type_cd in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') then 'appmop'
when a.commit_type_cd in('Ali Koubei MOP','Ali Koubei MOP RTN') then 'alimop'
when a.commit_type_cd in('App Delivery','Delivery Return','APP Return') then 'appmod'
when a.commit_type_cd in('Wechat Delivery RTN','Wechat Delivery') then 'wechatmod'
when a.commit_type_cd in('Eleme','Eleme Return') then 'elememod'
else 'others' END as purchase_channel
from temp_order_first_second a 
left join temp_bev_party_size b on a.row_id=b.order_id 
join u_analysis_dw.siebel_store c on a.srv_prov_ou_id=c.store_id
left join temp_coupon_redeem d on a.row_id=d.row_id
left join temp_prod_collect e on a.row_id=e.row_id)
,temp_first_second_unique as(
select *,row_number() over(partition by order_id order by order_dt desc ) rank from  
(select * from temp_new_order
union all
select * from u_analysis_temp.temp_first_second_2)t1  having rank=1)
insert overwrite table u_analysis_temp.temp_first_second_2
select
member_id,
order_id,
bev_party_size,
store_type,
day_part,
order_dt,
coupon_redeem,
discount_rato,
cost_save,
prod_collect,
purchase_channel 
from 
(select *,row_number() over(partition by member_id order by order_dt) rk from temp_first_second_unique)p
where p.rk<=2;



insert OVERWRITE table u_analysis_app.member_purchase_behavior_transaction partition(pdate='${td}',time='1')
select 
member_id,
bev_party_size,
store_type,
day_part,
coupon_redeem,
discount_rato,
cost_save,
prod_collect,
purchase_channel 
from 
(select *,row_number() over(partition by member_id order by order_dt) rank from u_analysis_temp.temp_first_second_2)p 
where   p.rank=1; 


insert OVERWRITE table u_analysis_app.member_purchase_behavior_transaction  partition(pdate='${td}',time='2')
select 
member_id,
bev_party_size,
store_type,
day_part,
coupon_redeem,
discount_rato,
cost_save,
prod_collect,
purchase_channel
from 
(select *,row_number() over(partition by member_id order by order_dt) rank from u_analysis_temp.temp_first_second_2)p 
where   p.rank=2;"

















 





