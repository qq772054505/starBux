#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
select max(pdate) from u_analysis_app.member_profile_Demo_facts; 
select max(pdate) from u_analysis_app.member_profile_derived_demographic;
select max(pdate) from u_analysis_app.member_channel_active;
select max(pdate) from u_analysis_app.member_Register_channel;
select max(pdate) from u_analysis_app.member_bundle;
select max(pdate) from u_analysis_app.member_channel_active;
select max(pdate) from u_analysis_app.member_purchase_behavior_recency;
select max(pdate) from u_analysis_app.member_purchase_behavior_RFM_daypart;
select max(pdate) from u_analysis_app.member_daypart_spend;
select max(pdate) from u_analysis_app.member_purchase_behavior_category;
select max(pdate) from u_analysis_app.member_purchase_behavior_store; 
select max(pdate) from u_analysis_app.member_purchase_behavior_trans;
select max(pdate) from u_analysis_app.communication_total_tag;
select max(pdate) from u_analysis_app.communication_detail_tag_daily;
select max(pdate) from u_analysis_app.star_tag;
select max(pdate) from u_analysis_app.member_campaign_perference_SRKIT;
select max(pdate) from u_analysis_app.member_purchase_behavior_transaction;



insert overwrite table u_analysis_temp.temp_member_profile  
select
t1.member_id,
t2.Age,
t2.Birthday,
t2.Gender,
t3.Partner,
t2.city_tier,
t2.city,
t2.Register_channel,
t2.Register_time,
t3.Daigou,
t3.Fraud,
t3.tier,
t3.sub_tier,
case when t4.active is null then 0 else t4.active end as P3M_APP_active,
case when t5.active is null then 0 else t5.active end as P6M_APP_active,
case when t6.active is null then 0 else t6.active end as P12M_APP_active,
case when t7.active is null then 0 else t7.active end as P3M_PhysicalCard_active,
case when t8.Register  is null then 0 else t8.Register  end as Register_Ali ,
case when t9.Register  is null then 0 else t9.Register  end as Register_Wechat,
case when t10.Register is null then 0 else t10.Register end as Register_CMB,
case when t11.Register is null then 0 else t11.Register end as Register_Apple,
case when t12.Bundle is null then 0 else t12.Bundle end as Bundle_Ali,
case when t13.Bundle is null then 0 else t13.Bundle end as Bundle_Wechat,
case when t14.Bundle is null then 0 else t14.Bundle end as Bundle_CMB,
case when t15.Bundle is null then 0 else t15.Bundle end as Bundle_Apple,
case when t16.active is null then 0 else t16.active end as P3M_Alipay_active_users,
case when t17.active is null then 0 else t17.active end as P3M_Eleme_active_users,
case when t18.active is null then 0 else t18.active end as P3M_Wechat_active_users,
case when t19.active is null then 0 else t19.active end as P3M_CMB_active_users
from u_analysis_dw.siebel_member                            t1
left join  u_analysis_app.member_profile_Demo_facts          t2 on t1.member_id=t2.member_id and t2.pdate='${td}'
left join  u_analysis_app.member_profile_derived_demographic t3 on t1.member_id=t3.member_id and t3.pdate='${td}'
left join  u_analysis_app.member_channel_active   t4  on t1.member_id=t4.member_id  and t4.pdate='${td}'  and t4.channel='1' and t4.type='P3M'
left join  u_analysis_app.member_channel_active   t5  on t1.member_id=t5.member_id  and t5.pdate='${td}'  and t5.channel='1' and t5.type='P6M'
left join  u_analysis_app.member_channel_active   t6  on t1.member_id=t6.member_id  and t6.pdate='${td}'  and t6.channel='1' and t6.type='P12M'
left join  u_analysis_app.member_channel_active   t7  on t1.member_id=t7.member_id  and t7.pdate='${td}'  and t7.channel='0' and t7.type='P3M'
left join  u_analysis_app.member_Register_channel t8  on t1.member_id=t8.member_id  and t8.pdate='${td}'  and t8.channel='2' 
left join  u_analysis_app.member_Register_channel t9  on t1.member_id=t9.member_id  and t9.pdate='${td}'  and t9.channel='3' 
left join  u_analysis_app.member_Register_channel t10 on t1.member_id=t10.member_id and t10.pdate='${td}' and t10.channel='4' 
left join  u_analysis_app.member_Register_channel t11 on t1.member_id=t11.member_id and t11.pdate='${td}' and t11.channel='5' 
left join  u_analysis_app.member_bundle           t12 on t1.member_id=t12.member_id and t12.pdate='${td}' and t12.channel='2' 
left join  u_analysis_app.member_bundle           t13 on t1.member_id=t13.member_id and t13.pdate='${td}' and t13.channel='3' 
left join  u_analysis_app.member_bundle           t14 on t1.member_id=t14.member_id and t14.pdate='${td}' and t14.channel='4'  
left join  u_analysis_app.member_bundle           t15 on t1.member_id=t15.member_id and t15.pdate='${td}' and t15.channel='5'  
left join  u_analysis_app.member_channel_active   t16 on t1.member_id=t16.member_id and t16.pdate='${td}' and t16.channel='2' and t16.type='P3M'
left join  u_analysis_app.member_channel_active   t17 on t1.member_id=t17.member_id and t17.pdate='${td}' and t17.channel='6' and t17.type='P3M'
left join  u_analysis_app.member_channel_active   t18 on t1.member_id=t18.member_id and t18.pdate='${td}' and t18.channel='3' and t18.type='P3M'
left join  u_analysis_app.member_channel_active   t19 on t1.member_id=t19.member_id and t19.pdate='${td}' and t19.channel='4' and t19.type='P3M'
;



insert overwrite table u_analysis_temp.temp_member_purchase_behaviour 
select
t1.member_id,
datediff(current_date(),t2.recency) as historical_recency_days,
t3.transactions as P3M_transactions,
t3.spend        as P3M_spend,
t3.average_at   as P3M_average_at,
t3.highest_at   as P3M_highest_at,
t4.spend        as P3M_Morning_Spend,
t5.spend        as P3M_Moon_Spend,
t6.spend        as P3M_Afternoon_Spend,
t7.spend        as P3M_Evening_Spend,
t3.Day_part_diversity,
t8.transaction as P3M_Bev_transactions,
t8.spend        as P3M_Bev_Spend,
t11.spend       as P6M_Bev_Spend,
t8.item_size    as P3M_Bev_party_size,
t9.transaction as P3M_Food_transactions,
t9.spend        as P3M_Food_Spend,
t12.spend       as P6M_Food_Spend,
t9.item_size    as P3M_food_items_purchased,
t10.spend       as P3M_Merch_Spend,
t13.spend       as P6M_Merch_Spend,
t13.item_size   as P6M_merch_items_purchased,
t3.Brand_love_index,
t14.hub_store_num as Numb_of_HubStore,
t14.last_transact_hub_store as Last_Trans_HubStore,
t14.last_transact_hub_store_type as Last_Trans_Hub_Store_type,
datediff(current_date(),t16.recency) as MOP_recency,
t18.trans   as P1M_MOP_Tran,
t20.trans   as P3M_MOP_Tran,
t22.trans   as P6M_MOP_Tran,
datediff(current_date(),t15.recency) as MOD_recency,
t17.trans   as P1M_MOD_Tran,
t19.trans   as P3M_MOD_Tran,
t21.trans   as P6M_MOD_Tran,
t23.bev_party_size as first_Beverage_party_size,
t23.store_type     as first_Store_type,
t23.day_part       as first_Daypart,
t23.coupon_redeem  as first_Coupon_redeemed,
t23.discount_rato  as first_Discount_rato,
t23.cost_save      as first_Cost_saved,     
t24.bev_party_size as second_Beverage_party_size,
t24.store_type     as second_Store_type,
t24.day_part       as second_Daypart,
t24.coupon_redeem  as second_Coupon_redeemed,
t24.discount_rato  as second_Discount_rato,
t24.cost_save      as second_Cost_saved,
t25.bev_party_size as most_often_Beverage_party_size,
t25.Store_type     as most_often_Store_type,
t25.day_part       as most_often_Daypart,
t25.coupon_redeem  as most_often_Coupon_redeeme,
t25.discount_rato  as most_often_Discount_rato,
t25.cost_save      as most_often_Cost_saved,
t23.product_pruchase as first_product_purchase,  
t23.purchase_channel as first_purchase_channel,  
t24.product_pruchase as second_product_purchase, 
t24.purchase_channel as second_purchase_channel, 
t25.product_pruchase as most_often_product_purchase,    
t25.purchase_channel as most_often_purchase_channel     
from  u_analysis_dw.siebel_member                             t1
left join u_analysis_app.member_purchase_behavior_recency     t2 on t1.member_id=t2.member_id and  t2.pdate='${td}' and t2.channel='all'
left join u_analysis_app.member_purchase_behavior_RFM_daypart t3 on t1.member_id=t3.member_id and  t3.pdate='${td}' and t3.type='P3M'
left join u_analysis_app.member_daypart_spend                 t4 on t1.member_id=t4.member_id and  t4.pdate='${td}' and t4.daypart='1'
left join u_analysis_app.member_daypart_spend                 t5 on t1.member_id=t5.member_id and  t5.pdate='${td}' and t5.daypart='2'
left join u_analysis_app.member_daypart_spend                 t6 on t1.member_id=t6.member_id and  t6.pdate='${td}' and t6.daypart='3'
left join u_analysis_app.member_daypart_spend                 t7 on t1.member_id=t7.member_id and  t7.pdate='${td}' and t7.daypart='4'
left join u_analysis_app.member_purchase_behavior_category    t8 on t1.member_id=t8.member_id and  t8.pdate='${td}' and t8.type='P3M' and t8.category='0'
left join u_analysis_app.member_purchase_behavior_category    t9 on t1.member_id=t9.member_id and  t9.pdate='${td}' and t9.type='P3M' and t9.category='1'
left join u_analysis_app.member_purchase_behavior_category    t10 on t1.member_id=t10.member_id and  t10.pdate='${td}' and t10.type='P3M' and t10.category='2'
left join u_analysis_app.member_purchase_behavior_category    t11 on t1.member_id=t11.member_id and  t11.pdate='${td}' and t11.type='P6M' and t11.category='0'
left join u_analysis_app.member_purchase_behavior_category    t12 on t1.member_id=t12.member_id and  t12.pdate='${td}' and t12.type='P6M' and t12.category='1'
left join u_analysis_app.member_purchase_behavior_category    t13 on t1.member_id=t13.member_id and  t13.pdate='${td}' and t13.type='P6M' and t13.category='2'
left join u_analysis_app.member_purchase_behavior_store       t14 on t1.member_id=t14.member_id and  t14.pdate='${td}' 
left join u_analysis_app.member_purchase_behavior_recency     t15 on t1.member_id=t15.member_id and  t15.pdate='${td}' and t15.channel='mod'
left join u_analysis_app.member_purchase_behavior_recency     t16 on t1.member_id=t16.member_id and  t16.pdate='${td}' and t16.channel='mop'
left join u_analysis_app.member_purchase_behavior_trans       t17 on t1.member_id=t17.member_id and  t17.pdate='${td}' and t17.channel='mod' and t17.type='P1M'
left join u_analysis_app.member_purchase_behavior_trans       t18 on t1.member_id=t18.member_id and  t18.pdate='${td}' and t18.channel='mop' and t18.type='P1M'
left join u_analysis_app.member_purchase_behavior_trans_daily       t19 on t1.member_id=t19.member_id and  t19.pdate='${td}' and t19.channel='mod' and t19.type='P3M' 
left join u_analysis_app.member_purchase_behavior_trans_daily       t20 on t1.member_id=t20.member_id and  t20.pdate='${td}' and t20.channel='mop' and t20.type='P3M'
left join u_analysis_app.member_purchase_behavior_trans      t21 on t1.member_id=t21.member_id and  t21.pdate='${td}' and t21.channel='mod' and t21.type='P6M'
left join u_analysis_app.member_purchase_behavior_trans       t22 on t1.member_id=t22.member_id and  t22.pdate='${td}' and t22.channel='mop' and t22.type='P6M'
left join u_analysis_app.member_purchase_behavior_transaction t23 on t1.member_id=t23.member_id and t23.pdate='${td}' and t23.time='1'
left join u_analysis_app.member_purchase_behavior_transaction t24 on t1.member_id=t24.member_id and t24.pdate='${td}' and t24.time='2'
left join u_analysis_app.member_purchase_behavior_transaction t25 on t1.member_id=t25.member_id and t25.pdate='${td}' and t25.time='MOST OFTEN'
;

insert overwrite table u_analysis_temp.temp_member_communication 
select 
t1.member_id,
t2.prefer_channel,
t2.prefer_daypart,
t2.sms_prefer_daypart,
t2.push_prefer_daypart,
t2.email_prefer_daypart,
t3.send                as sms_send, 
t3.click               as sms_click,  
t3.morning_click       as sms_morning_click, 
t3.noon_click          as sms_noon_click,
t3.afternoon_click     as sms_afternoon_click,
t3.dinner_click        as sms_dinner_click,
t3.click_with_purchase as sms_click_with_purchase,
t4.send                as push_send, 
t4.click               as push_click, 
t4.morning_click       as push_morning_click, 
t4.noon_click          as push_noon_click,
t4.afternoon_click     as push_afternoon_click,
t4.dinner_click        as push_dinner_click,
t4.click_with_purchase as push_click_with_purchase,
t5.send                as email_send,
t5.click               as email_click,           
t5.morning_click       as email_morning_click, 
t5.noon_click          as email_noon_click,
t5.afternoon_click     as email_afternoon_click,
t5.dinner_click        as email_dinner_click, 
t5.click_with_purchase as email_click_with_purchase
from u_analysis_dw.siebel_member t1
left join u_analysis_app.communication_total_tag  t2 on t1.member_id=t2.member_id and t2.pdate='${td}' 
left join u_analysis_app.communication_detail_tag_daily t3 on t1.member_id=t3.member_id and t3.pdate='${td}' and t3.channel='0' and t3.type='p3m'
left join u_analysis_app.communication_detail_tag_daily t4 on t1.member_id=t4.member_id and t4.pdate='${td}' and t4.channel='1' and t4.type='p3m'
left join u_analysis_app.communication_detail_tag_daily t5 on t1.member_id=t5.member_id and t5.pdate='${td}' and t5.channel='2' and t5.type='p3m'
;

insert overwrite table u_analysis_temp.temp_member_campaign_performance 
select 
t1.member_id,
t2.participate_num as P3M_participating_star_campaign, 
t2.redeem_time     as P3M_times_redeeming_stars,
t2.redeem_num      as P3M_stars_redeemed, 
t2.actual_pay_amt  as P3M_Actual_paying_amount,
t2.save_amt        as P3M_Cost_saved_via_Star,
t3.redeem_coupon_time as P3M_times_redeeming_coupons,
t3.redeem_coupon_num  as P3M_coupons_redeemed,
t3.save_amt           as Cost_saved_via_Coupon,
t4.bundle_num         as P6M_SR_kit_bundled,
t4.redeem_num         as P6M_SR_kit_coupons_redeemed,
t4.core_bundle_num    as P6M_core_SR_kit_bundled,
t4.digital_bundle_num as P6M_Digital_SR_kit_bundled,
t4.save_amt           as P6M_Cost_saved_via_SR_kit,
t5.redeem_num         as P12M_times_redeeming_core_benefits,
t5.save_amt           as P12M_Cost_saved_via_core_benefits
from u_analysis_dw.siebel_member  t1
left join u_analysis_app.star_tag t2                                 on t1.member_id=t2.member_id and t2.pdate='${td}' 
left join u_analysis_app.member_campaign_perference_coupon        t3 on t1.member_id=t3.member_id and t3.pdate='${td}' and t3.type='P3M'
left join u_analysis_app.member_campaign_perference_SRKIT         t4 on t1.member_id=t4.member_id and t4.pdate='${td}' and t4.type='P6M'
left join u_analysis_app.member_campaign_perference_core_benefits t5 on t1.member_id=t5.member_id and t5.pdate='${td}' and t5.type='P12M';



 
insert overwrite table u_analysis_app.tagging partition(pdate='${td}')
select 
a.member_id,
prefer_channel,
prefer_daypart,
sms_prefer_daypart,
push_prefer_daypart,
email_prefer_daypart,
sms_send, 
sms_click,  
sms_morning_click, 
sms_noon_click,
sms_afternoon_click,
sms_dinner_click,
sms_click_with_purchase,
push_send, 
push_click, 
push_morning_click, 
push_noon_click,
push_afternoon_click,
push_dinner_click,
push_click_with_purchase,
email_send,
email_click,           
email_morning_click, 
email_noon_click,
email_afternoon_click,
email_dinner_click, 
email_click_with_purchase,
Age,
Birthday,
Gender,
Partner,
city_tier,
city,
Register_channel,
Register_time,
Daigou,
Fraud,
tier,
sub_tier,
P3M_APP_active,
P6M_APP_active,
P12M_APP_active,
P3M_PhysicalCard_active,
Register_Ali ,
Register_Wechat,
Register_CMB,
Register_Apple,
Bundle_Ali,
Bundle_Wechat,
Bundle_CMB,
Bundle_Apple,
P3M_Alipay_active_users,
P3M_Eleme_active_users,
P3M_Wechat_active_users,
P3M_CMB_active_users,
recency,
P3M_transactions,
P3M_spend,
P3M_average_at,
P3M_highest_at,
P3M_Morning_Spend,
P3M_Moon_Spend,
P3M_Afternoon_Spend,
P3M_Evening_Spend,
Day_part_diversity,
P3M_Bev_transactions,
P3M_Bev_Spend,
P6M_Bev_Spend,
P3M_Bev_party_size,
P3M_Food_transactions,
P3M_Food_Spend,
P6M_Food_Spend,
P3M_food_items_purchased,
P3M_Merch_Spend,
P6M_Merch_Spend,
P6M_merch_items_purchased,
Brand_love_index,
Numb_of_HubStore,
Last_Trans_HubStore,
Last_Trans_Hub_Store_type,
MOP_recency,
P1M_MOP_Tran,
P3M_MOP_Tran,
P6M_MOP_Tran,
MOD_recency,
P1M_MOD_Tran,
P3M_MOD_Tran,
P6M_MOD_Tran,
first_Beverage_party_size,
first_Store_type,
first_Daypart,
first_Coupon_redeemed,
first_Discount_rato,
first_Cost_saved,     
second_Beverage_party_size,
second_Store_type,
second_Daypart,
second_Coupon_redeemed,
second_Discount_rato,
second_Cost_saved,
most_often_Beverage_party_size,
most_often_Store_type,
most_often_Daypart,
most_often_Coupon_redeeme,
most_often_Discount_rato,
most_often_Cost_saved,
P3M_participating_star_campaign, 
P3M_times_redeeming_stars,
P3M_stars_redeemed, 
P3M_Actual_paying_amount,
P3M_Cost_saved_via_Star,
P3M_times_redeeming_coupons,
P3M_coupons_redeemed,
Cost_saved_via_Coupon,
P6M_SR_kit_bundled,
P6M_SR_kit_coupons_redeemed,
P6M_core_SR_kit_bundled,
P6M_Digital_SR_kit_bundled,
P6M_Cost_saved_via_SR_kit,
P12M_times_redeeming_core_benefits,
P12M_Cost_saved_via_core_benefits,
first_product_purchase,  
first_purchase_channel,  
second_product_purchase, 
second_purchase_channel, 
most_often_product_purchase,    
most_often_purchase_channel 
from u_analysis_temp.temp_member_communication  a
join u_analysis_temp.temp_member_profile b              on a.member_id=b.member_id
join u_analysis_temp.temp_member_purchase_behaviour c   on a.member_id=c.member_id
join u_analysis_temp.temp_member_campaign_performance d on a.member_id=d.member_id;"