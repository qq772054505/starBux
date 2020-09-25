--member_profile_Demo_facts
use u_analysis_app;
CREATE external TABLE if not exists u_analysis_app.member_profile_Demo_facts(
member_id string,
age int,
birthday string,
gender string,
city_tier string,
city string,
register_channel string,
register_time string)
PARTITIONED BY(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_profile_Demo_facts';
  
  
--member_profile_derived_demographic
use u_analysis_app;
CREATE external TABLE if not exists u_analysis_app.member_profile_derived_demographic(
member_id string,
partner string,
daigou string,
fraud string,
tier string,
sub_tier string) 
PARTITIONED BY (pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_profile_derived_demographic';


  
--member_purchase_behavior_daypart 
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_RFM_daypart(
member_id string,
transactions int,
spend int,
average_at int,
highest_at int,
day_part_diversity int,
brand_love_index int)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_RFM_daypart';
  
--member_purchase_behavior_category
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_category(
member_id string,
spend int,
transaction int,
item_size int)
partitioned by(pdate string,type string,category string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_category';
  
  
--member_purchase_behavior_store
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_store(
member_id string,
--attached_store_type string,
hub_store_num int,
last_transact_hub_store string,
last_transact_hub_store_type string)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_store';
drop table  u_analysis_app.member_purchase_behavior_store;


--member_purchase_behavior_recency  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_recency(
member_id string,
row_id string,
recency string)
partitioned by(pdate string,channel string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_recency';


  
--member_purchase_behavior_trans  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_trans(
member_id string,
trans int)
partitioned by(pdate string,type string,channel string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_trans';
  
  
--member_purchase_behavior_first_second_mostOften  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_first_second_mostOften(
member_id string,
bev_party_size string,
store_type string,
day_part string,
coupon_redeem string,
discount_rato string,
cost_save string)
partitioned by(pdate string,time string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_first_second_mostOften';
  
  
  
  
--member_campaign_perference_coupon 
use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_coupon(
member_id string,
redeem_coupon_time int,
redeem_coupon_num int,
save_amt int)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_coupon';

--member_campaign_perference_SRKIT 
use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_SRKIT(
member_id string,
bundle_num int,
redeem_num int,
core_bundle_num int,
digital_bundle_num int,
save_amt int)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_SRKIT';
  
--member_campaign_perference_core_benefits
use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_core_benefits(
member_id string,
redeem_num int,
save_amt int)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_core_benefits';
  

  

use u_analysis_dw;
create external table if not exists  u_analysis_dw.siebel_s_loy_mem_vchr_used(
row_id string,  
consumed_txn_id string, 
vchr_eff_start_dt string,      
expiration_dt string,   
member_uuid string,    
member_id string,      
prod_id string,  
status_cd string,       
used_dt string,  
x_use_store_id string,  
x_cmpgn_id string,     
x_vchr_ear_dt string,   
last_upd string,        
part_num string,        
x_ext_txn_num string,   
x_card_num string,      
x_promo_id string
)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/dw/siebel_s_loy_mem_vchr_used';
  
  

use u_analysis_app;
create external table if not exists u_analysis_app.member_bundle(
member_id string,
bundle string)
partitioned by(pdate string,channel string) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_bundle';
  
  
use u_analysis_app;
create external table if not exists u_analysis_app.member_channel_active(
member_id string,
active string)
partitioned by(pdate string,channel string,type string) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_channel_active';
  
  
use u_analysis_app;
create external table if not exists u_analysis_app.member_daypart_spend(
member_id string,
spend string)
partitioned by(pdate string,daypart string,type string) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_daypart_spend';
  
  
 
use u_analysis_app;
create external table if not exists u_analysis_app.member_Register_channel(
member_id string,
register string)
partitioned by(pdate string,channel string) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_Register_channel';
  
  
use u_analysis_app;
create external table if not exists u_analysis_app.tagging(
member_id  string,
prefer_channel string, 
prefer_daypart string,
sms_prefer_daypart  string,
push_prefer_daypart string,
email_prefer_daypart string,
sms_send string, 
sms_click string,  
sms_morning_click string, 
sms_noon_click string,
sms_afternoon_click string,
sms_dinner_click string,
sms_click_with_purchase string,
push_send string, 
push_click string, 
push_morning_click string, 
push_noon_click string,
push_afternoon_click string,
push_dinner_click string,
push_click_with_purchase string,
email_send string,
email_click string,           
email_morning_click string, 
email_noon_click string,
email_afternoon_click string,
email_dinner_click string, 
email_click_with_purchase string,
Age string,
Birthday string,
Gender string,
Partner string,
city_tier string,
city string,
Register_channel string,
Register_time string,
Daigou string,
Fraud string,
tier string,
sub_tier string,
P3M_APP_active string,
P6M_APP_active string,
P12M_APP_active string, 
P3M_PhysicalCard_active string,
Register_Ali string,
Register_Wechat string,
Register_CMB string,
Register_Apple string,
Bundle_Ali string,
Bundle_Wechat string,
Bundle_CMB string,
Bundle_Apple string,
P3M_Alipay_active_users string,
P3M_Eleme_active_users string,
P3M_Wechat_active_users string,
P3M_CMB_active_users string,
recency string,
P3M_transactions string,
P3M_spend string,
P3M_average_at string,
P3M_highest_at string,
P3M_Morning_Spend string,
P3M_Moon_Spend string,
P3M_Afternoon_Spend string,
P3M_Evening_Spend string,
Day_part_diversity string,
P3M_Bev_transactions string,
P3M_Bev_Spend string,
P6M_Bev_Spend string,
P3M_Bev_party_size string,
P3M_Food_transactions string,
P3M_Food_Spend string,
P6M_Food_Spend string,
P3M_food_items_purchased string,
P3M_Merch_Spend string,
P6M_Merch_Spend string,
P6M_merch_items_purchased string,
Brand_love_index string,
Numb_of_HubStore string,
Last_Trans_HubStore string,
Last_Trans_Hub_Store_type string,
MOP_recency string,
P1M_MOP_Tran string,
P3M_MOP_Tran string,
P6M_MOP_Tran string,
MOD_recency string,
P1M_MOD_Tran string,
P3M_MOD_Tran string,
P6M_MOD_Tran string,
first_Beverage_party_size string,
first_Store_type string,
first_Daypart string,
first_Coupon_redeemed string,
first_Discount_rato string,
first_Cost_saved string,     
second_Beverage_party_size string,
second_Store_type string,
second_Daypart string,
second_Coupon_redeemed string,
second_Discount_rato string,
second_Cost_saved string,
most_often_Beverage_party_size string,
most_often_Store_type string,
most_often_Daypart string,
most_often_Coupon_redeeme string,
most_often_Discount_rato string,
most_often_Cost_saved string,
P3M_participating_star_campaign string, 
P3M_times_redeeming_stars string,
P3M_stars_redeemed string, 
P3M_Actual_paying_amount string,
P3M_Cost_saved_via_Star string,
P3M_times_redeeming_coupons string,
P3M_coupons_redeemed string,
Cost_saved_via_Coupon string,
P6M_SR_kit_bundled string,
P6M_SR_kit_coupons_redeemed string,
P6M_core_SR_kit_bundled string,
P6M_Digital_SR_kit_bundled string,
P6M_Cost_saved_via_SR_kit string,
P12M_times_redeeming_core_benefits string,
P12M_Cost_saved_via_core_benefits string
)
partitioned by(pdate string) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/tagging';
  
  
  
use u_analysis_app;
create external table  if not exists u_analysis_app.member_purchase_behavior_transaction 	(
member_id string,
purchase_channel string,
bev_party_size string,
store_type string,
day_part string,
coupon_redeem string,
product_pruchase string,
discount_rato string,
cost_save string)
partitioned by(pdate string,time string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_transaction';


  
create external table if not exists  u_analysis_app.communication_detail_tag_daily(
member_id string,
send int,
click int,
morning_click int,
noon_click int,   
afternoon_click int,   
dinner_click int,    
click_with_purchase string
)
partitioned by(pdate string,channel string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/communication_detail_tag_daily';
  
  
  
create external table if not exists  u_analysis_dw.communication(
member_id string,
match_id string,
channel string,
status int,  
str_1 string,
str_2 string,  
str_3 string,
event_time string,
run_date string,
dayparts string 
)
partitioned by(type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/dw/communication';
  
  

  
  
create external table if not exists u_analysis_app.member_purchase_behavior_RFM_daily(
member_id string,
recency   string,
avg_transaction_interval string,
Personal_Active_Index string
)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_RFM_daily';
  
  
  
--member_purchase_behavior_trans  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_trans_daily(
member_id string, 
trans int)
partitioned by(pdate string,type string,channel string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_trans';
  
  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_profile_loyalty_daily(
member_id string,
Tier_star_balance string,
Reward_star_balance string,
Star_gap_to_Green string,
Star_gap_to_half_Gold string,
Star_gap_to_Gold string,
Days_becoming_Gold string,
Days_becoming_Green string,
Days_ex_Gold string,
Days_ex_Green string)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_profile_loyalty_daily';
  
  
  
use u_analysis_app;
create external table if not exists  u_analysis_app.tagging_daily(
member_id string,
P3M_sms_send  string, 
P3M_sms_click string,  
P3M_sms_morning_click string, 
P3M_sms_noon_click string,
P3M_sms_afternoon_click string,
P3M_sms_dinner_click string,
P3M_sms_click_with_purchase string,
P3M_push_send string, 
P3M_push_click string, 
P3M_push_morning_click string, 
P3M_push_noon_click string,
P3M_push_afternoon_click string,
P3M_push_dinner_click string,
P3M_push_click_with_purchase string,
P3M_email_send string,
P3M_email_click string,           
P3M_email_morning_click string, 
P3M_email_noon_click string,
P3M_email_afternoon_click string,
P3M_email_dinner_click string, 
P3M_email_click_with_purchase string,
P1M_sms_send string, 
P1M_sms_click string,  
P1M_sms_morning_click string, 
P1M_sms_noon_click string,
P1M_sms_afternoon_click string,
P1M_sms_dinner_click string,
P1M_sms_click_with_purchase string,
P1M_push_send string, 
P1M_push_click string, 
P1M_push_morning_click string, 
P1M_push_noon_click string,
P1M_push_afternoon_click string,
P1M_push_dinner_click string,
P1M_P1M_push_click_with_purchase string,
P1M_email_send string,
P1M_email_click string,           
P1M_email_morning_click string, 
P1M_email_noon_click string,
P1M_email_afternoon_click string,
P1M_email_dinner_click string, 
P1M_email_click_with_purchase string,
P7D_sms_send string, 
P7D_sms_click string,  
P7D_sms_morning_click string, 
P7D_sms_noon_click string,
P7D_sms_afternoon_click string,
P7D_sms_dinner_click string,
P7D_sms_click_with_purchase string,
P7D_push_send string, 
P7D_push_click string, 
P7D_push_morning_click string, 
P7D_push_noon_click string,
P7D_push_afternoon_click string,
P7D_push_dinner_click string,
P7D_push_click_with_purchase string,
P7D_email_send string,
P7D_email_click string,           
P7D_email_morning_click string, 
P7D_email_noon_click string,
P7D_email_afternoon_click string,
P7D_email_dinner_click string, 
P7D_email_click_with_purchase string,
Tier_star_balance string,
Reward_star_balance string,
Star_gap_to_Green string,
Star_gap_to_half_Gold string,
Star_gap_to_Gold string,
Days_becoming_Gold string,
Days_becoming_Green string,
Days_ex_Gold string,
Days_ex_Green string,
SRKit_coupon_balance string,
Core_benefits_balance string,
recency   string,
avg_transaction_interval string,
Personal_Active_Index string,
P3M_MOP_trans string,
P3M_historical_MOP_trans string,
P3M_MOD_trans string,
P3M_historical_MOD_trans string
)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/tagging_daily';
  
  


use u_analysis_app;
create external table if not exists  u_analysis_app.member_profile_vchr_balance_daily(
member_id string,
vchr_balance string
)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_profile_vchr_balance_daily';
  



use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_free_delivery(
member_id string,
trans string,
cost_saved string
)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_free_delivery';
  
  
  
  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_product_discount(
member_id string,
trans string,
product_pruchase string,
cost_saved string
)
partitioned by(pdate string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_product_discount';
  
  
  
  

  

  
use u_analysis_app;
create external table if not exists  u_analysis_app.member_purchase_behavior_trans_daily(
member_id string, 
trans int)
partitioned by(pdate string,type string,channel string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_purchase_behavior_trans_daily';




use u_analysis_app;
create external table if not exists  u_analysis_app.member_campaign_perference_pay_promo(
member_id string,
trans string,
cost_saved string
)
partitioned by(pdate string,type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/app/member_campaign_perference_pay_promo';


   



 
  
  
