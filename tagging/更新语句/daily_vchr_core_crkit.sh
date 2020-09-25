#!/bin/bash
dt=`date +%Y%m%d`
p2d=`date -d '-2 day' +%Y%m%d`
p2y=`date -d '-730 day' +%Y-%m-%d`


/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
with temp_core_benefits as(
select * from bigdata.siebel_s_loy_mem_vchr where dt>'${p2d}' and dt<='${dt}' and prod_id in ('BFP_00000000001','BFP_00000000002','1-1ZBAAE8T','1-1ZBAAE93','1-1ZBAAE9D'))
insert overwrite table u_analysis_temp.temp_core_skrit_vchr  
select 
p.row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
b.member_uuid,    
p.member_id,      
prod_id,  
p.status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
p.last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id 
from temp_core_benefits p 
join u_analysis_app.s_benefit a on p.prod_id=a.row_id 
left join u_analysis_dw.member_uuid b on p.member_id=b.member_id;

with temp_dw_core_vchr as 
(select 
row_id,
consumed_txn_id,
vchr_eff_start_dt,
expiration_dt,
member_uuid,
member_id,
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id from u_analysis_dw.core_skrit_vchr where type='core')
insert overwrite table u_analysis_dw.core_skrit_vchr partition(type='core') 
select 
row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
member_uuid,    
member_id,      
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id
from(
select 
row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
member_uuid,    
member_id,      
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id,
row_number() over (partition by row_id order by last_upd desc) rank    
from(
select * from temp_dw_core_vchr 
union all
select * from u_analysis_temp.temp_core_skrit_vchr)t1)t2
where rank=1 and to_Date(vchr_eff_start_dt)>'${p2y}';


with temp_skrit as
(select a.* from bigdata.siebel_s_loy_mem_vchr a 
join u_analysis_temp.siebel_vw_s_loy_card b on a.x_card_num=b.card_num and b.x_card_type>='0150' where a.dt>'${p2d}' and a.dt<='${dt}' )
insert overwrite table u_analysis_temp.temp_core_skrit_vchr  
select 
p.row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
b.member_uuid,    
p.member_id,      
prod_id,  
p.status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
p.last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id 
from temp_skrit p 
join u_analysis_app.s_benefit a on p.prod_id=a.row_id 
left join u_analysis_dw.member_uuid b on p.member_id=b.member_id;
  
with temp_dw_srkit as 
(select 
row_id,
consumed_txn_id,
vchr_eff_start_dt,
expiration_dt,
member_uuid,
member_id,
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id from u_analysis_dw.core_skrit_vchr where type='srkit'  )
insert overwrite table u_analysis_dw.core_skrit_vchr partition(type='srkit') 
select 
row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
member_uuid,    
member_id,      
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id
from(
select 
row_id,  
consumed_txn_id, 
vchr_eff_start_dt,      
expiration_dt,   
member_uuid,    
member_id,      
prod_id,  
status_cd,       
used_dt,  
x_use_store_id,  
x_cmpgn_id,     
x_vchr_ear_dt,   
last_upd,        
part_num,        
x_ext_txn_num,   
x_card_num,      
x_promo_id,
row_number() over (partition by row_id order by last_upd desc) rank    
from(
select * from temp_dw_srkit
union all
select * from u_analysis_temp.temp_core_skrit_vchr)t1)t2
where rank=1 and to_Date(vchr_eff_start_dt)>'${p2y}' ;"

  
 

  
  


