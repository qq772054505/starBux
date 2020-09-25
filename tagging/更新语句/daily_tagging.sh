#!/bin/bash
td=`date +%Y-%m-%d`

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 4 --executor-memory 16g -e"

select max(pdate) from u_analysis_app.communication_detail_tag_daily;
select max(pdate) from u_analysis_app.member_profile_loyalty_daily;
select max(pdate) from u_analysis_app.member_profile_vchr_balance_daily;
select max(pdate) from u_analysis_app.member_purchase_behavior_RFM_daily;
select max(pdate) from u_analysis_app.member_purchase_behavior_trans_daily;

insert overwrite table u_analysis_app.tagging_daily partition(pdate='${td}')
select 
t1.member_id,
t2.send                as P3M_sms_send, 
t2.click               as P3M_sms_click,  
t2.morning_click       as P3M_sms_morning_click,   
t2.noon_click          as P3M_sms_noon_click,
t2.afternoon_click     as P3M_sms_afternoon_click,
t2.dinner_click        as P3M_sms_dinner_click,
t2.click_with_purchase as P3M_sms_click_with_purchase,
t3.send                as P3M_push_send, 
t3.click               as P3M_push_click, 
t3.morning_click       as P3M_push_morning_click, 
t3.noon_click          as P3M_push_noon_click,
t3.afternoon_click     as P3M_push_afternoon_click,
t3.dinner_click        as P3M_push_dinner_click,
t3.click_with_purchase as P3M_push_click_with_purchase,
t4.send                as P3M_email_send,
t4.click               as P3M_email_click,           
t4.morning_click       as P3M_email_morning_click, 
t4.noon_click          as P3M_email_noon_click,
t4.afternoon_click     as P3M_email_afternoon_click,
t4.dinner_click        as P3M_email_dinner_click, 
t4.click_with_purchase as P3M_email_click_with_purchase,
t5.send                as P1M_sms_send, 
t5.click               as P1M_sms_click,  
t5.morning_click       as P1M_sms_morning_click, 
t5.noon_click          as P1M_sms_noon_click,
t5.afternoon_click     as P1M_sms_afternoon_click,
t5.dinner_click        as P1M_sms_dinner_click,
t5.click_with_purchase as P1M_sms_click_with_purchase,
t6.send                as P1M_push_send, 
t6.click               as P1M_push_click, 
t6.morning_click       as P1M_push_morning_click, 
t6.noon_click          as P1M_push_noon_click,
t6.afternoon_click     as P1M_push_afternoon_click,
t6.dinner_click        as P1M_push_dinner_click,
t6.click_with_purchase as P1M_push_click_with_purchase,
t7.send                as P1M_email_send,
t7.click               as P1M_email_click,           
t7.morning_click       as P1M_email_morning_click, 
t7.noon_click          as P1M_email_noon_click,
t7.afternoon_click     as P1M_email_afternoon_click,
t7.dinner_click        as P1M_email_dinner_click, 
t7.click_with_purchase as P1M_email_click_with_purchase,
t8.send                as P7D_sms_send, 
t8.click               as P7D_sms_click,  
t8.morning_click       as P7D_sms_morning_click, 
t8.noon_click          as P7D_sms_noon_click,
t8.afternoon_click     as P7D_sms_afternoon_click,
t8.dinner_click        as P7D_sms_dinner_click,
t8.click_with_purchase as P7D_sms_click_with_purchase,
t9.send                as P7D_push_send, 
t9.click               as P7D_push_click, 
t9.morning_click       as P7D_push_morning_click, 
t9.noon_click          as P7D_push_noon_click,
t9.afternoon_click     as P7D_push_afternoon_click,
t9.dinner_click        as P7D_push_dinner_click,
t9.click_with_purchase as P7D_push_click_with_purchase,
t10.send                as P7D_email_send,
t10.click               as P7D_email_click,           
t10.morning_click       as P7D_email_morning_click, 
t10.noon_click          as P7D_email_noon_click,
t10.afternoon_click     as P7D_email_afternoon_click,
t10.dinner_click        as P7D_email_dinner_click, 
t10.click_with_purchase as P7D_email_click_with_purchase,
t11.Tier_star_balance,
t11.Reward_star_balance,
t11.Star_gap_to_Green,
t11.Star_gap_to_half_Gold,
t11.Star_gap_to_Gold,
t11.Days_becoming_Gold,
t11.Days_becoming_Green,
t11.Days_ex_Gold,
t11.Days_ex_Green,
t12.vchr_balance as SRKit_coupon_balance,
t18.vchr_balance as Core_benefits_balance,
t13.recency,
t13.avg_transaction_interval,
t13.Personal_Active_Index,
t14.trans as P3M_MOP_trans,
t15.trans as P3M_historical_MOP_trans,
t16.trans as P3M_MOD_trans,
t17.trans as P3M_historical_MOD_trans
from u_analysis_dw.siebel_member t1
left join u_analysis_app.communication_detail_tag_daily t2  on t1.member_id=t2.member_id  and t2.pdate='${td}' and t2.channel='0' and t2.type='p3m'
left join u_analysis_app.communication_detail_tag_daily t3  on t1.member_id=t3.member_id  and t3.pdate='${td}' and t3.channel='1' and t3.type='p3m'
left join u_analysis_app.communication_detail_tag_daily t4  on t1.member_id=t4.member_id  and t4.pdate='${td}' and t4.channel='2' and t4.type='p3m'
left join u_analysis_app.communication_detail_tag_daily t5  on t1.member_id=t5.member_id  and t5.pdate='${td}' and t5.channel='0' and t5.type='p1m'
left join u_analysis_app.communication_detail_tag_daily t6  on t1.member_id=t6.member_id  and t6.pdate='${td}' and t6.channel='1' and t6.type='p1m'
left join u_analysis_app.communication_detail_tag_daily t7  on t1.member_id=t7.member_id  and t7.pdate='${td}' and t7.channel='2' and t7.type='p1m'
left join u_analysis_app.communication_detail_tag_daily t8  on t1.member_id=t8.member_id  and t8.pdate='${td}' and t8.channel='0' and t8.type='p7d'
left join u_analysis_app.communication_detail_tag_daily t9  on t1.member_id=t9.member_id  and t9.pdate='${td}' and t9.channel='1' and t9.type='p7d'
left join u_analysis_app.communication_detail_tag_daily t10 on t1.member_id=t10.member_id and t10.pdate='${td}' and t10.channel='2' and t10.type='p7d' 
left join u_analysis_app.member_profile_loyalty_daily t11   on t1.member_id=t11.member_id and t11.pdate='${td}'
left join u_analysis_app.member_profile_vchr_balance_daily t12 on t1.member_id=t12.member_id and t12.pdate='${td}' and t12.type='srkit'
left join u_analysis_app.member_purchase_behavior_RFM_daily t13   on t1.member_id=t13.member_id and t13.pdate='${td}'
left join u_analysis_app.member_purchase_behavior_trans_daily t14 on t1.member_id=t14.member_id and t14.pdate='${td}' and t14.channel='mop' and t14.type='P3M'
left join u_analysis_app.member_purchase_behavior_trans_daily t15 on t1.member_id=t15.member_id and t15.pdate='${td}' and t15.channel='mop' and t15.type='all'
left join u_analysis_app.member_purchase_behavior_trans_daily t16 on t1.member_id=t16.member_id and t16.pdate='${td}' and t16.channel='mod' and t16.type='P3M'
left join u_analysis_app.member_purchase_behavior_trans_daily t17 on t1.member_id=t17.member_id and t17.pdate='${td}' and t17.channel='mod' and t17.type='all'
left join u_analysis_app.member_profile_vchr_balance_daily t18 on t1.member_id=t18.member_id and t18.pdate='${td}' and t18.type='core'
;"

 
