with TEMP_distinct_prod_id   as
(select a.order_id,b.prod_id,row_number() OVER(PARTITION by a.order_id,b.prod_id order by a.order_dt) rank
from u_analysis_temp.temp_first_second a join u_analysis_dw.siebel_order_items b on a.order_id=b.order_id 
where(b.X_B3G1_FLG='Y' or b.X_DEPARTMENT_CD in('FESTIVAL FOOD','FOOD','MERCHANDISE','WHOLE BEAN','PACKAGED FOOD','Packaged Tea','PACKAGING','PRINCI FOOD','PRINCI MERCHANDISE','VIA','WHOLE CAKE'))
having rank=1)
,Temp_prod_collect as
(select order_id,concat_ws(',',collect_set(prod_id)) as prod_collect from TEMP_distinct_prod_id group by order_id)
,temp_Purchase_channel as
(select a.order_id,
case 
when commit_type_cd in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null') then 'store'
when commit_type_cd in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN') then 'appmop'
when commit_type_cd in('Ali Koubei MOP','Ali Koubei MOP RTN') then 'alimop'
when commit_type_cd in('App Delivery','Delivery Return','APP Return') then 'appmod'
when commit_type_cd in('Wechat Delivery RTN','Wechat Delivery') then 'wechatmod'
when commit_type_cd in('Eleme','Eleme Return') then 'elememod'
else 'others' END as purchase_channel
from u_analysis_temp.temp_first_second a join u_analysis_dw.siebel_cx_order b on a.order_id=b.row_id)
,temp_vchr_part_num_all as
(select *,row_number() OVER(PARTITION by order_id,part_num order by order_id) rank from( 
 select * from u_analysis_temp.temp_vchr_part_num_after 
 union all
 select * from u_analysis_temp.temp_vchr_part_num_before)t1  having rank=1)
,temp_part_num_collect as
(select order_id,concat_ws(',',collect_set(part_num)) as part_num_collect from temp_vchr_part_num_all  group by order_id)
insert overwrite table  u_analysis_temp.temp_first_second_2
select a.member_id,a.order_id,a.bev_party_size,a.store_type,a.day_part,a.order_dt,d.part_num_collect,a.discount_rato,a.cost_save,b.prod_collect,c.purchase_channel 
from u_analysis_temp.temp_first_second a
left join temp_prod_collect b on a.order_id=b.order_id
join temp_Purchase_channel c on a.order_id=c.order_id
left join u_analysis_temp.temp_part_num_collect d on a.order_id=d.order_id;



CREATE table u_analysis_temp.temp_first_second_2(
member_id string,
order_id string,
bev_party_size string,
store_type string,
day_part string,
order_dt string,
coupon_redeem string,
discount_rato string,
cost_save string,
prod_collect string,
purchase_channel string);




--store
--appmop
--alimop
--APPMOD,WechatMOD,ELEMemod
--others

create table u_analysis_temp.temp_vchr_part_num_before as
select a.order_id,
d.part_num
from u_analysis_temp.temp_first_second a
join u_analysis_dw.siebel_cx_src_payment b on a.order_id=b.order_id and
b.pdate<='2018-09-01'  
join u_analysis_temp.SIEBEL_CX_PAYMENT_TYPE c on b.PAID_BY=c.SOURCE_CODE and b.TYPE_CD=c.TYPE and b.INVOICE_NUM=c.SERIAL_NUM
join u_analysis_app.s_benefit d on c.CODE=d.INTEGRATION_ID; 

create table u_analysis_temp.temp_vchr_part_num_after as
select a.order_id,
c.part_num
from u_analysis_temp.temp_first_second a
join u_analysis_temp.siebel_s_Loy_txn b on a.order_id=b.orig_order_Id 
join u_analysis_dw.siebel_s_loy_mem_vchr_used c on c.consumed_txn_id=b.row_id 
where b.pdate>'2018-09-01' and c.pdate>'2018-09-01';


with temp_vchr_part_num_all as
(select *,row_number() OVER(PARTITION by order_id,part_num order by order_id) rank from( 
 select * from u_analysis_temp.temp_vchr_part_num_after 
 union all
 select * from u_analysis_temp.temp_vchr_part_num_before)t1  having rank=1)
 select order_id,concat_ws(',',collect_set(part_num)) as part_num_collect from temp_vchr_part_num_all  group by order_id;
 
 
 
 
 
 
 
 
 
 
 

 

create table u_analysis_temp.temp_most_often_purchase_channel as  
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
 


create table u_analysis_temp.temp_most_often_prod as
select member_id,prod_id from (
select member_id,prod_id,row_number() over(partition by member_id order by frequency desc,max_order_Dt desc)rank  from (
select a.member_id,b.prod_id,count(*) frequency,max(a.order_dt) max_order_Dt from u_analysis_dw.siebel_cx_order a 
join u_analysis_dw.siebel_order_items b on a.row_id=b.order_id and a.valid=1 and (b.X_B3G1_FLG='Y' or b.X_DEPARTMENT_CD in('FESTIVAL FOOD','FOOD','MERCHANDISE','WHOLE BEAN','PACKAGED FOOD','Packaged Tea','PACKAGING','PRINCI FOOD','PRINCI MERCHANDISE','VIA','WHOLE CAKE')) 
 group by a.member_id,b.prod_id)t1)t2  
where rank=1;





u_analysis_dw.siebel_s_loy_mem_vchr_used a join u_analysis_temp.siebel_s_Loy_txn b on a.consumed_txn_id=b.row_id
join u_analysis_dw.siebel_cx_order c on c.row_Id = b.orig_order_Id 










