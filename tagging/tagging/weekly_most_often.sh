#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
with temp_bev_party_size as
(select order_id,sum(cast(quantity as int)) bev_party_size from u_analysis_dw.siebel_order_items where x_b3g1_flg='Y' group by order_id)
insert overwrite table  u_analysis_temp.temp_most_often_bev_party_size  
select member_id,party_size from
(select member_id,party_size,row_number() over(partition by member_id order by frequency desc,party_size desc)  rank from 
(select a.member_id,case when bev_party_size is null then 0 else bev_party_size end as party_size,count(*) frequency from u_analysis_dw.siebel_cx_order a left join temp_bev_party_size b on a.row_id=b.order_id and a.valid=1 
 group by member_id,party_size)t1)t2  
where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_cost_save 
select member_id,cost_save from
(select member_id,cost_save,row_number() over(partition by member_id order by frequency desc,cost_save desc) rank  from
(select member_id,discnt_amt as cost_save,count(*) frequency from u_analysis_dw.siebel_cx_order where valid=1 and discnt_amt<>'null' and cast(discnt_amt as double)>0
 group by member_id,cost_save)t1)t2
where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_discount_rato 
select member_id,discount_rato from
(select member_id,discount_rato,row_number() over(partition by member_id order by frequency desc,discount_rato desc) rank from
(select member_id,concat(cast(discnt_amt/(total_amt+discnt_amt)*100 as int),'%') discount_rato,count(*) frequency from u_analysis_dw.siebel_cx_order
where valid=1 and discnt_amt<>'null' and cast(discnt_amt as double)>0    
group by member_id,discount_rato)t1)t2
where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_day_part 
select member_id,
case 
when day_part=1 then 'Morning'
when day_part=2 then 'Noon'
when day_part=3 then 'Afternoon'
else 'Dinner' end day_part from
(select member_id,day_part,row_number() over(partition by member_id order by frequency desc,day_part) rank from
(select member_id,
case 
when hour(order_dt) between 2 and 10 then 1
when hour(order_dt) between 11 and 13 then 2
when hour(order_dt) between 14 and 16 then 3
else 4 end day_part,
count(*) frequency
from u_analysis_dw.siebel_cx_order where valid=1  group by member_id,day_part)t1)t2
where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_store_type
 select member_id,trade_area from 
(select member_id,trade_area,row_number() over(partition by member_id order by frequency desc,trade_area) rank from 
(select member_id,trade_area,count(*) frequency from u_analysis_dw.siebel_cx_order a join u_analysis_dw.siebel_store c on a.srv_prov_ou_id=c.store_id and a.valid=1  group by member_id,trade_area)t1)t2
where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_coupon_redeem 
select member_id,part_num as coupon_redeem from
(select member_id,part_num,last_used_dt,row_number() over(partition by member_id order by frequency desc,last_used_dt desc) rank from
(select member_id,part_num,max(used_dt) as last_used_dt,count(*) frequency from u_analysis_dw.siebel_s_loy_mem_vchr_used 
 group by member_id,part_num)t1)t2 where rank=1;

insert overwrite table u_analysis_temp.temp_most_often_purchase_channel  
select member_id,purchase_channel from(
select member_id,purchase_channel,row_number() over(partition by member_id order by frequency desc,purchase_channel) rank from (
select member_id,purchase_channel,count(*) frequency from (
select member_id,
case 
when commit_type_cd in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null') then 'store'
when commit_type_cd in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') then 'appmop'
when commit_type_cd in('Ali Koubei MOP','Ali Koubei MOP RTN') then 'alimop'
when commit_type_cd in('App Delivery','Delivery Return','APP Return') then 'appmod'
when commit_type_cd in('Wechat Delivery RTN','Wechat Delivery') then 'wechatmod'
when commit_type_cd in('Eleme','Eleme Return') then 'elememod'
else 'others' END as purchase_channel
from  u_analysis_dw.siebel_cx_order where valid=1)t1
group by member_id,purchase_channel)t2    
)t3
where rank=1;
 
insert overwrite table  u_analysis_temp.temp_most_often_prod 
select member_id,prod_id from (
select member_id,prod_id,row_number() over(partition by member_id order by frequency desc,max_order_Dt desc)rank  from (
select a.member_id,b.prod_id,count(*) frequency,max(a.order_dt) max_order_Dt from u_analysis_dw.siebel_cx_order a 
join u_analysis_dw.siebel_order_items b on a.row_id=b.order_id and a.valid=1 and (b.X_B3G1_FLG='Y' or b.X_DEPARTMENT_CD in('FESTIVAL FOOD','FOOD','MERCHANDISE','WHOLE BEAN','PACKAGED FOOD','Packaged Tea','PACKAGING','PRINCI FOOD','PRINCI MERCHANDISE','VIA','WHOLE CAKE')) 
 group by a.member_id,b.prod_id)t1)t2  
where rank=1;

insert overwrite table u_analysis_app.member_purchase_behavior_transaction partition(pdate='${td}',time='MOST OFTEN')
select c.member_id,a.party_size,b.x_store_type,c.day_part,f.coupon_redeem,d.discount_rato,e.cost_save,g.prod_id,h.purchase_channel
from u_analysis_temp.temp_most_often_day_part c
left join u_analysis_temp.temp_most_often_bev_party_size a on c.member_id=a.member_id
left join u_analysis_temp.temp_most_often_store_type b on c.member_id=b.member_id
left join u_analysis_temp.temp_most_often_discount_rato d on c.member_id=d.member_id
left join u_analysis_temp.temp_most_often_cost_save e on c.member_id=e.member_id
left join u_analysis_temp.temp_most_often_coupon_redeem f on c.member_id=f.member_id
left join u_analysis_temp.temp_most_often_prod g on c.member_id=g.member_id
left join u_analysis_temp.temp_most_often_purchase_channel h on  c.member_id=h.member_id;"






