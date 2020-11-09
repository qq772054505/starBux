#!/bin/bash
dt=`date +%Y%m%d`
p3d=`date -d '-3 day' +%Y%m%d`


pdate=`/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e "
--查询近三天的数据插入临时表中，并获得used_dt的集合，获得要更新的分区
with temp_vchr_used_refuned as(
select * from bigdata.siebel_s_loy_mem_vchr where dt>='${p3d}' and dt<='${dt}' and status_cd in('Used','Refunded') and used_dt<>'null')
insert overwrite table u_analysis_temp.temp_vchr_used
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
from temp_vchr_used_refuned p 
join u_analysis_app.s_benefit a on p.prod_id=a.row_id 
left join u_analysis_dw.member_uuid b on p.member_id=b.member_id;
SELECT concat_ws(',',collect_set(concat('\'',pdate,'\''))) FROM(select distinct to_date(used_dt) pdate from u_analysis_temp.temp_vchr_used)p;" | tail -1`

echo $pdate

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
-----------------------------------------------------------------------
--  功能: 每天更新用过的券表
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:bigdata.siebel_s_loy_mem_vchr
--  目标表: u_analysis_dw.siebel_s_loy_mem_vchr_used 
--  
--  参数: 需要更新的分区的集合pdate
--  数据策略：增量
--  频率：daily
--  中间表：u_analysis_app.s_benefit
--          u_analysis_dw.member_uuid 
--  临时表：u_analysis_temp.temp_vchr_used
-----------------------------------------------------------------------
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.optimize.sort.dynamic.partition=true;
set spark.sql.shuffle.partitions=400;
--将要更新的分区的数据和近两天的数据union在一起去重后，动态分区覆盖掉原来的分区
with temp_dw_vchr_used as 
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
x_promo_id from u_analysis_dw.siebel_s_loy_mem_vchr_used where pdate in (${pdate}))
insert overwrite table u_analysis_dw.siebel_s_loy_mem_vchr_used partition(pdate) 
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
to_date(used_dt)  pdate
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
select * from temp_dw_vchr_used  
union all
select * from u_analysis_temp.temp_vchr_used)t1)t2
where rank=1 and status_cd='Used' distribute by pdate,cast(rand() * 2 as int);"

















