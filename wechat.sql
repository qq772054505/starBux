/*五种券生效通知*/
use u_analysis_app;

CREATE EXTERNAL TABLE u_analysis_app.wechat_lifecycle_vchr_effect_notice(
uuid string,member_id string,prod_id string,part_num string,alias_name string,vchr_eff_start_dt string,user_name string
)
PARTITIONED BY (pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/wechat_lifecycle_vchr_effect_notice';

insert overwrite table u_analysis_app.vchr_effect_notice partition(pdate='2020-07-15')
 
select uuid,member_id,prod_id,d.part_num,d.alias_name,date_format(from_unixtime(vchr_eff_start_dt),'yyyy-MM-dd'),       
  case when e.name like '. %' and e.name like '% .' then substr(e.name,3,length(e.name)-4)
  when e.name like '. %' then substr(e.name,3)
  when e.name like '% .' then substr(e.name,1,length(e.name)-2)
  else e.name end as user_name
from u_analysis_dw.wechat_uuid a 
join u_analysis_ods.tb_member_3app b  on a.unionid=b.open_id and b.pdate='2020-07-15' and b.bind_flag=1  and app_enum='1201' 
join u_analysis_app.s_mem_vchr_one_year c on c.member_id=b.customer_id and status_cd='Available' and date_format(from_unixtime(vchr_eff_start_dt),'yyyy-MM-dd')='2020-07-15' and prod_id in('BFP_00000000001','BFP_00000000002','1-1ZBAAE8T','1-1ZBAAE93','1-1ZBAAE9D') 
join u_analysis_app.s_benefit d  on d.row_id=prod_id
join u_analysis_temp.siebel_vw_s_loy_member e on e.row_id=c.member_id and e.status_cd='Active';

/*30天后金星降级*/
use u_analysis_app;

CREATE EXTERNAL TABLE u_analysis_app.wechat_lifecycle_gold2green_30days_later(uuid string,member_id string,tier_end_dt string,cell_ph_num string) 
PARTITIONED BY (pdate string)

ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/wechat_lifecycle_gold2green_30days_later';

insert overwrite table u_analysis_app.gold2green_30days_later  partition(pdate='2020-07-15') 

select uuid,member_id,date_add(date(p.end_dt),1),concat(substr(cell_ph_num,1,3),'****',substr(cell_ph_num,8,4))
from
(select row_number() over(partition by member_id order by seq_num desc) as rank,member_id,tier_id,end_dt from u_analysis_temp.siebel_s_loy_mem_tier) p 
join u_analysis_temp.siebel_vw_s_loy_member a on p.member_id=a.row_id and rank=1 and a.status_cd='Active' and a.point_type_a_val<16 and datediff(p.end_dt,'2020-07-15')=29 and p.tier_id='1-2RU7'
join u_analysis_ods.tb_member_3app b on a.row_id=b.customer_id and b.pdate='2020-07-15' and b.bind_flag=1  and b.app_enum='1201' 
join u_analysis_dw.wechat_uuid c on c.unionid=b.open_id
join u_analysis_temp.siebel_s_contact d on d.par_row_id=a.pr_con_id ;


use u_analysis_app;


CREATE EXTERNAL TABLE u_analysis_app.wechat_lifecycle_review2green(uuid string,member_id string,tier_id string,last_tier_id string,tier_end_dt string,cell_ph_num string) 
PARTITIONED BY (pdate string)

ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/wechat_lifecycle_review2green';

insert overwrite table u_analysis_app.wechat_lifecycle_review2green partition(pdate='2020-07-12')

select uuid,member_id,tier_id,last_tier_id,
concat(year(current_date()),'-',if(date_format(a.start_dt,'MM-dd')='02-29' and !((year(current_date())%4=0 and year(current_date())%100<>0) or year(current_date())%400=0),'02-28',date_format(a.start_dt,'MM-dd'))),
concat(substr(cell_ph_num,1,3),'****',substr(cell_ph_num,8,4))
from
(select row_number() over(partition by member_id order by seq_num desc) as rank,member_id,start_dt,tier_id,lead(tier_id) over(partition by member_id order by seq_num desc) as last_tier_id from u_analysis_temp.siebel_s_loy_mem_tier) p 
join u_analysis_temp.siebel_vw_s_loy_member a 
on p.member_id=a.row_id and rank=1 and  tier_id='1-2RU6' and last_tier_id in('1-2RU6','1-2RU7') and date(p.start_dt)='2020-07-12' 
and concat(year(current_date()),'-',if(date_format(a.start_dt,'MM-dd')='02-29' and !((year(current_date())%4=0 and year(current_date())%100<>0) or year(current_date())%400=0),'02-28',date_format(a.start_dt,'MM-dd')))='2020-07-12'  
and a.status_cd='Active' and a.point_type_a_val<16   
join u_analysis_ods.tb_member_3app b on a.row_id=b.customer_id and b.pdate='2020-07-15' and b.bind_flag=1  and b.app_enum='1201' 
join u_analysis_dw.wechat_uuid c on c.unionid=b.open_id
join u_analysis_temp.siebel_s_contact d on d.par_row_id=a.pr_con_id;

