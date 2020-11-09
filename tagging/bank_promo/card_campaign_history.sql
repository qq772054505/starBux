create external table if not exists  u_analysis_dw.card_pay_promo(
member_id string,
order_id   string,
order_date string,
payment_amount decimal(10,2),
business_discount string,
platform_discount string
)
partitioned by(pdate string,promo_channel string,promotion_tag string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/u_analysis/private/dw/card_pay_promo';
--周三 In-App/小程序(7.8) 微信 PAB平安卡 70-18 6.17-8.31  Tag pabpromo2020
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select
d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)),
a.pdate as pdate,
'wechat'   as   promo_channel,
'PAB70-18' as   promotion_tag
from u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.wechat_order_id=b.svc_or_b2b_number  and a.wechat_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number 
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where
a.pdate>='2020-06-17' and  a.pdate<='2020-08-31' 
and b.pdate>='2020-06-17' and  b.pdate<='2020-08-31'
and c.pdate>='2020-06-17' and  c.pdate<='2020-08-31'
and d.pdate>='2020-06-17' and  d.pdate<='2020-08-31' 
and date_format(d.order_dt,'u')=3  
and  cast(platform_discount as double)=18 
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN') 
distribute by pdate,cast(rand() * 2 as int);
--周一 In-App 支付宝 ICBC工行卡 70-15 7.20-12.31  Tag icbcpromotion
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)),
 a.pdate as pdate,
'alipay'   as   promo_channel,
'ICBC70-15' as   promotion_tag
from u_analysis_ods.urp_alipay a 
join u_analysis_dw.esb_payment_item b on a.alipay_order_id=b.svc_or_b2b_number  and a.alipay_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number and c.svc_or_b2b_number<>a.alipay_order_id
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where 
a.pdate>='2020-07-20' and  a.pdate<='2020-12-31' 
and b.pdate>='2020-07-20' and  b.pdate<='2020-12-31'
and c.pdate>='2020-07-20' and  c.pdate<='2020-12-31'
and d.pdate>='2020-07-20' and  d.pdate<='2020-12-31' 
and date_format(d.order_dt,'u')=1 
and  cast(platform_discount as double )=15 
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN')
distribute by pdate,cast(rand() * 2 as int);
--周一 In-Store 微信 ICBC工行卡 60-15 8.3-12.31 Tag icbcpromotion
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select c.member_id,c.row_id,c.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)),
 a.pdate as pdate,
'wechat'   as   promo_channel,
'ICBC60-15' as   promotion_tag 
from  u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id  
where 
a.pdate>='2020-08-03' and  a.pdate<='2020-12-31' 
and b.pdate>='2020-08-03' and  b.pdate<='2020-12-31'
and c.pdate>='2020-08-03' and  c.pdate<='2020-12-31' 
and date_format(c.order_dt,'u')=1 
and cast(platform_discount as double )=15
and a.service_type in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null')
distribute by pdate,cast(rand() * 2 as int);
--周二 In-Store 微信 BOC中行借记卡 60-20 Tag bocpromo60 8.11-10.31
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select c.member_id,c.row_id,c.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)),
 a.pdate as pdate,
'wechat'   as   promo_channel,
'BOC60-20' as   promotion_tag    
from  u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number 
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id
where   
a.pdate>='2020-08-11' and  a.pdate<='2020-10-31' 
and b.pdate>='2020-08-11' and  b.pdate<='2020-10-31'
and c.pdate>='2020-08-11' and  c.pdate<='2020-10-31' 
and date_format(c.order_dt,'u') in (2,4) 
and cast(platform_discount as double )=20
and a.service_type in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null')
distribute by pdate,cast(rand() * 2 as int);
--周二 In-App/小程序 微信 BOC中行借记卡 70-20 Tag bocpromo70
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)),
 a.pdate as pdate,
'wechat'   as   promo_channel,
'BOC70-20' as   promotion_tag   
from u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.wechat_order_id=b.svc_or_b2b_number  and a.wechat_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number 
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where 
a.pdate>='2020-08-11' and  a.pdate<='2020-10-31' 
and b.pdate>='2020-08-11' and  b.pdate<='2020-10-31'
and c.pdate>='2020-08-11' and  c.pdate<='2020-10-31' 
and d.pdate>='2020-08-11' and  d.pdate<='2020-10-31' 
and date_format(d.order_dt,'u') in (2,4) 
and  cast(platform_discount as double )=20  
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN')
distribute by pdate,cast(rand() * 2 as int);
--周六 In-Store 银联二维码 云闪付APP CCB 60-20 7.4-12.31
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate,promo_channel,promotion_tag)
select c.member_id,c.row_id,c.order_dt,
cast(cast(a.amount as double)-cast(a.discount_amount as double) as decimal(10,2)),
0.00,
cast(a.discount_amount as decimal(10,2)),
a.trade_date as pdate,
'bank'   as   promo_channel,
'CCB60-20' as   promotion_tag   
from  u_analysis_ods.urp_bank a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number 
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id  
where 
a.pdate>='2020-07-03' and a.pdate<='2020-12-31' 
and b.pdate>='2020-07-03' and b.pdate<='2020-12-31' 
and c.pdate>='2020-07-03' and c.pdate<='2020-12-31' 
and a.discount_campaign_id='2112020081432872'
and date_format(a.trade_date,'u')=6
and cast(a.discount_amount as double )=20 and  cast(a.amount as double ) >=60;
distribute by pdate,cast(rand() * 2 as int);



































select count(*),trade_date
from  u_analysis_ods.urp_bank a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number 
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id  
where 
a.pdate>='2020-07-03' and a.pdate<='2020-12-31' 
and b.pdate>='2020-07-03' and b.pdate<='2020-12-31' 
and c.pdate>='2020-07-03' and c.pdate<='2020-12-31' 
--and a.discount_campaign_id='2112020081432872'
and date_format(a.trade_date,'u')=6
and cast(a.discount_amount as double )=20 and  cast(a.amount as double ) >=60
group by trade_date;






 