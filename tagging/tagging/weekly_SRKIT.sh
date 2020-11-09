#!/bin/bash
td=`date +%Y-%m-%d`

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
--sr kit
with temp_bundled as (select member_id,count(distinct card_num) cards,count(distinct case 
when x_card_type in ('0170','0185','0186','0187','0188','0202','0203','0204','0205','0213','0214','0215','0216') 
then card_num end) digital_cards,count(distinct case when x_card_type 
not in ('0170','0185','0186','0187','0188','0202','0203','0204','0205','0213','0214','0215','0216') 
then card_num end) core_cards FROM u_analysis_temp.siebel_vw_s_loy_card 
where start_dt>=date_add(current_date,-182) and start_dt<current_date and x_card_type>('0150')
and x_returncard_flg<>'Y' group by member_id)
,temp_vchr as (SELECT ROW_ID,PART_NUM,MEMBER_ID,USED_DT,CONSUMED_TXN_ID 
FROM u_analysis_dw.siebel_s_loy_mem_vchr_used where pdate between date_add(current_date,-182) and date_add(current_date,-1) and  used_dt  
between date_add(current_date,-182) and date_add(current_date,-1) and status_cd='Used' and x_card_num<>'null')
,temp_redeem as (
SELECT t1.*,
TXN_CHANNEL_CD,t5.INTEGRATION_ID,ORIG_ORDER_ID,AMT_VAL
FROM
(SELECT ROW_ID,PART_NUM as coupon_id,MEMBER_ID,
USED_DT,CONSUMED_TXN_ID
FROM temp_vchr) t1
JOIN
(select * from u_analysis_temp.siebel_s_loy_txn where pdate>=date_add(current_date,-182)) t3
on t1.CONSUMED_TXN_ID=t3.ROW_ID
LEFT JOIN
u_analysis_app.s_benefit t5
on t1.coupon_id=t5.part_num
),temp_MOD as (
select row_id,ORIG_ORDER_ID,coupon_id,member_id,used_dt,AMT_VAL as discount,
case when TXN_CHANNEL_CD in ('MOD','MOD_WECHAT') then 'MOD' ELSE 'MOP' END as channel FROM temp_redeem where TXN_CHANNEL_CD in ('MOD','MOP','MOD_WECHAT')
),temp_instore as (
select t1.row_id,ORIG_ORDER_ID,coupon_id,t1.member_id,used_dt,PAY_AMT as discount,'Retail' as channel FROM (select * from temp_redeem where TXN_CHANNEL_CD not in ('MOD','MOP','MOD_WECHAT')) t1
JOIN
(SELECT ORDER_ID,SUM(PAY_AMT) PAY_AMT,TYPE_CD,PAID_BY,INVOICE_NUM FROM u_analysis_dw.siebel_cx_src_payment where pdate>=date_add(current_date,-182) and TYPE_CD = 'Discount' GROUP BY ORDER_ID,TYPE_CD,PAID_BY,INVOICE_NUM) t6
on t1.ORIG_ORDER_ID=t6.order_id
JOIN
(SELECT CODE,SOURCE_CODE,TYPE,SERIAL_NUM FROM u_analysis_temp.SIEBEL_CX_PAYMENT_TYPE) t7
on t6.PAID_BY=t7.SOURCE_CODE and t6.TYPE_CD=t7.TYPE and t6.INVOICE_NUM=t7.SERIAL_NUM and t7.CODE=t1.INTEGRATION_ID
),temp_all as (
SELECT row_id,ORIG_ORDER_ID,coupon_id,member_id,used_dt,discount,channel FROM (
select * from temp_MOD
union all
select * from temp_instore) t1
)
INSERT OVERWRITE TABLE u_analysis_app.member_campaign_perference_SRKIT partition(pdate='${td}',type='P6M')
select t1.member_id,t2.cards,t3.coupons,t3.discount,t2.core_cards,t2.digital_cards from u_analysis_dw.siebel_member t1
left join temp_bundled t2 on t1.member_id = t2.member_id
left join
(select member_id,count(distinct row_id)coupons,sum(discount) discount FROM temp_all group by member_id) t3
on t1.member_id = t3.member_id
where t2.member_id is not null or t3.member_id is not null;"