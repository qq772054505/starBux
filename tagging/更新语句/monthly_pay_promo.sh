#!/bin/bash
td=`date +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 15g -e"
--微信 10004 20001  0-微信
insert overwrite table u_analysis_app.member_campaign_perference_pay_promo partition(pdate='${td}',type='0')
select member_id,sum(trans),cast(sum(cost_saved) as decimal(10,2)) from 
((select member_id,count(*) as trans,sum(pcoupon)*1.0/100 as cost_saved FROM u_analysis_dw.ups_pay t1 
join u_analysis_dw.esb_payment_item t2 on t1.platform_trade_number = t2.svc_or_b2b_number 
and t1.pdate>=date_add(current_date(),-91) and t1.pdate<current_date() and t2.pdate>=date_add(current_date(),-91) and t2.pdate<current_date() and t1.pcoupon*1.0/100>0  and platform='10004'
join u_analysis_dw.siebel_cx_order t3 on t2.receipt_number = t3.integration_id 
and t3.pdate>=date_add(current_date(),-91) and t3.pdate<current_date() and t3.valid=1
group by t3.member_id) 
union all 
(select a.member_id,count(*) as trans,sum(pcoupon)*1.0/100 as cost_saved from  u_analysis_dw.siebel_cx_order a 
join u_analysis_dw.ups_mobile_pay b
on b.partner_order_id=a.integration_id 
and a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date()  and a.valid=1 and b.pcoupon*1.0/100>0 and platform='20001' group by member_id)) group by member_id ;
--微信 10001 20002  1-支付宝
insert overwrite table u_analysis_app.member_campaign_perference_pay_promo partition(pdate='${td}',type='1')
select member_id,sum(trans),cast(sum(cost_saved) as decimal(10,2)) from
((select t3.member_id,count(*) as trans,sum(pcoupon)*1.0/100 as cost_saved from u_analysis_dw.ups_pay t1 
join u_analysis_dw.esb_payment_item t2 on t1.platform_trade_number = t2.svc_or_b2b_number 
and t1.pdate>=date_add(current_date(),-91) and t1.pdate<current_date() and t2.pdate>=date_add(current_date(),-91) and t2.pdate<current_date() and t1.pcoupon*1.0/100>0  and platform='10001'
join u_analysis_dw.siebel_cx_order t3 on t2.receipt_number = t3.integration_id 
and t3.pdate>=date_add(current_date(),-91) and t3.pdate<current_date() and t3.valid=1
group by t3.member_id)
union all
(select a.member_id,count(*) as trans,sum(pcoupon)*1.0/100 as cost_saved from  u_analysis_dw.siebel_cx_order a 
join u_analysis_dw.ups_mobile_pay b
on b.partner_order_id=a.integration_id 
and a.pdate>=date_add(current_date(),-91) and a.pdate<current_date() and b.pdate>=date_add(current_date(),-91) and b.pdate<current_date()  and a.valid=1 and b.pcoupon*1.0/100>0 and platform='20002' group by member_id)) group by member_id;"















