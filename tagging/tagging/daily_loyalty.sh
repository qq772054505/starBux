#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"

-----------------------------------------------------------------------
--  功能: 更新tagging   loyalty的指标 
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_dw.siebel_member
--       u_analysis_temp.siebel_vw_s_loy_member
--       u_analysis_temp.siebel_s_loy_mem_tier
--  目标表: u_analysis_app.member_profile_loyalty_daily
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：daily
-----------------------------------------------------------------------

--创建临时表，从u_analysis_dw.siebel_member获得tier_star,reward_star,计算和上一个等级差的天数
with temp_star as
(
    select member_id,
    cast(a.point_type_a_val as decimal(10,4)) as tier_star,
    cast(b.point_type_b_val as decimal(10,4)) as reward_star,
    case when level='Welcome' and a.point_type_a_val<4  then 4-cast(a.point_type_a_val  as decimal(10,4)) else 'null' end star_gap2Green,
    case when level='Green'   and a.point_type_a_val<8  then 8-cast(a.point_type_a_val  as decimal(10,4)) else 'null' end star_gap2halfGold,
    case when level='Green'   and a.point_type_a_val<16 then 16-cast(a.point_type_a_val as decimal(10,4)) else 'null' end star_gap2Gold
    from u_analysis_dw.siebel_member a
    join u_analysis_temp.siebel_vw_s_loy_member b on a.member_id = b.row_id
)
--创建临时表，分别计算天数指标
,temp_days_becoming_goldOgreen as
(
    select a.member_id,datediff(min(b.start_dt),a.submit_dt) as days_becoming_goldOgreen,b.tier_id 
    from u_analysis_dw.siebel_member a 
    join u_analysis_temp.siebel_s_loy_mem_tier b on a.member_id=b.member_id 
    where tier_id in('1-2RU7','1-2RU6') 
    group by a.member_id,submit_dt,tier_id
)
,temp_days_ex_gold as
(
    select 
    a.member_id,
    datediff(date_format(current_Date(),'yyyy-MM-dd hh:mm:ss'),max(b.end_dt)) as last_downgrade_from_gold 
    from u_analysis_dw.siebel_member a 
    join u_analysis_temp.siebel_s_loy_mem_tier b on a.member_id=b.member_id 
    where a.level='Green' and b.tier_id='1-2RU7' 
    group by a.member_id
)
,temp_days_ex_green as
(
    select 
    a.member_id,
    datediff(date_format(current_Date(),'yyyy-MM-dd hh:mm:ss'),max(b.end_dt)) as last_downgrade_from_green 
    from u_analysis_dw.siebel_member a
    join u_analysis_temp.siebel_s_loy_mem_tier b on a.member_id=b.member_id 
    where a.level='Welcome' and b.tier_id='1-2RU6' 
    group by a.member_id
)
--插入目标表
insert overwrite table u_analysis_app.member_profile_loyalty_daily partition(pdate='${td}')
select 
a.member_id,
a.tier_star,
a.reward_star,
a.star_gap2Green,
a.star_gap2halfGold,
a.star_gap2Gold,
b.days_becoming_goldOgreen,
c.days_becoming_goldOgreen,
d.last_downgrade_from_gold,
e.last_downgrade_from_green
from temp_star a 
left join temp_days_becoming_goldOgreen b on a.member_id=b.member_id and b.tier_id='1-2RU7'
left join temp_days_becoming_goldOgreen c on a.member_id=c.member_id and c.tier_id='1-2RU6'
left join temp_days_ex_gold d on a.member_id=d.member_id
left join temp_days_ex_green e on a.member_id=e.member_id;
"
 








