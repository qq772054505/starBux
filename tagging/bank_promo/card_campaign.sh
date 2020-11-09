#!/bin/bash
td=`date -d "-1 day" +%Y-%m-%d`
day=`date -d "-1 day"  +%w`
if [ "${day}" == "1" ] 
then
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
set spark.sql.shuffle.partitions=1;
--周一 In-App 支付宝 ICBC工行卡 70-15 7.20-12.31  Tag icbcpromotion
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate='${td}',promo_channel='alipay',promotion_tag='ICBC70-15')
select d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)) 
from u_analysis_ods.urp_alipay a 
join u_analysis_dw.esb_payment_item b on a.alipay_order_id=b.svc_or_b2b_number  and a.alipay_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number and c.svc_or_b2b_number<>a.alipay_order_id
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where a.pdate='${td}' and b.pdate='${td}' and c.pdate='${td}' and d.pdate='${td}'  
and  cast(platform_discount as double )=15 
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN');
--周一 In-Store 微信 ICBC工行卡 60-15 8.3-12.31 Tag icbcpromotion
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate='${td}',promo_channel='wechat',promotion_tag='ICBC60-15')
select c.member_id,c.row_id,c.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)) 
from  u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id  
where a.pdate='${td}' and b.pdate='${td}' and c.pdate='${td}' 
and cast(platform_discount as double )=15
and a.service_type in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null');"
elif [ "${day}" == "2" ] || [ "${day}" == "4" ] 
then 
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
set spark.sql.shuffle.partitions=1;
--周二 In-Store 微信 BOC中行借记卡 60-20 Tag bocpromo60 8.11-10.31
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate='${td}',promo_channel='wechat',promotion_tag='BOC60-20')
select c.member_id,c.row_id,c.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2))   
from  u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.business_order_id=b.svc_or_b2b_number 
join u_analysis_dw.siebel_cx_order c  on b.receipt_number=c.integration_id  
where a.pdate='${td}' and b.pdate='${td}' and c.pdate='${td}' 
and cast(platform_discount as double )=20
and a.service_type in('For Here','DineIn','ToGo','EX Service','Sales Return','Other','null');
--周二 In-App/小程序 微信 BOC中行借记卡 70-20 Tag bocpromo70
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate='${td}',promo_channel='wechat',promotion_tag='BOC70-20')
select d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2)) 
from u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.wechat_order_id=b.svc_or_b2b_number  and a.wechat_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number 
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where a.pdate='${td}' and b.pdate='${td}' and c.pdate='${td}' and d.pdate='${td}'  
and  cast(platform_discount as double )=20  
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN');"
elif [ "${day}" == "3" ]
then 
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
set spark.sql.shuffle.partitions=1;
--周三 In-App/小程序(7.8) 微信 PAB平安卡 70-18 6.17-8.31  Tag pabpromo2020
insert overwrite table u_analysis_dw.card_pay_promo partition(pdate='${td}',promo_channel='wechat',promotion_tag='PAB70-18')
select d.member_id,d.row_id,d.order_dt,
cast(cast(amount as double)-cast(platform_discount as double) as decimal(10,2)),
cast(business_discount as decimal(10,2)),
cast(platform_discount as decimal(10,2))  
from u_analysis_ods.urp_wechat a 
join u_analysis_dw.esb_payment_item b on a.wechat_order_id=b.svc_or_b2b_number  and a.wechat_order_id <>''
join u_analysis_dw.esb_payment_item c on c.receipt_number=b.receipt_number 
join u_analysis_dw.siebel_cx_order d  on c.svc_or_b2b_number=d.integration_id
where a.pdate='${td}' and b.pdate='${td}' and c.pdate='${td}' and d.pdate='${td}'  
and  cast(platform_discount as double)=18 
and a.service_type in('MOP','MOP Return','APP MOP PreOrder Today','APP MOP PreOrder Today RTN','App Delivery','Delivery Return','APP Return','APP MOD PreOrder Today','APP MOD PreOrder Today RTN');"
fi