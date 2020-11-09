#!/bin/bash
td=`date +%Y-%m-%d`
p30d=`date -d '-30 day' +%Y%m%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"

set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.optimize.sort.dynamic.partition=true;
set spark.sql.shuffle.partitions=400;

INSERT OVERWRITE TABLE u_analysis_dw.star_redemption_order PARTITION (pdate)
SELECT 
T2.ORDER_ID,
T1.ORDER_DT,
T1.COMMIT_TYPE_CD,
'STAR',
T1.SRV_PROV_OU_ID,
SUM(CASE WHEN PAY_AMT<0 THEN -1 ELSE 1 END *
CASE 
WHEN T3.CODE IN('SR-1SRedeem','SR-1SRCondi') THEN 1
WHEN T3.CODE IN('SR-9SRedeem','MSR-9SRedeem') THEN 9
WHEN T3.CODE IN('SR-15SRedeem') AND T7.PART_NUM IS NULL THEN 15 
WHEN T3.CODE IN('SR-15SRedeem') AND T7.PART_NUM IS NOT NULL THEN 3.75
--WHEN T3.CODE IN('SR-25SRedeem') THEN 25
WHEN T3.CODE IN('SR-12SRedeem') THEN 12
WHEN T3.CODE IN('SR-9+1SRedeem') THEN 10
--WHEN T3.CODE IN('SR-78OFF') THEN 25
END) AS STAR,
SUM(PAY_AMT) AS AMOUNT,
t1.pdate
FROM u_analysis_dw.siebel_cx_order t1 join
u_analysis_dw.siebel_cx_src_payment t2 
on t1.row_id=t2.order_id and t1.pdate>='${p30d}'
and t2.pdate>='${p30d}'
AND t1.valid=1
JOIN u_analysis_temp.siebel_cx_payment_type T3 ON T2.TYPE_CD = T3.TYPE AND T2.PAID_BY = T3.SOURCE_CODE AND T2.INVOICE_NUM = T3.SERIAL_NUM
AND T2.TYPE_CD = 'Discount' AND T3.CODE in('SR-1SRedeem','SR-1SRCondi','SR-9SRedeem','MSR-9SRedeem','SR-15SRedeem','SR-12SRedeem','SR-9+1SRedeem')
JOIN u_analysis_dw.SIEBEL_CX_ORDER_ITEM T4 ON T2.INTEGRATION_ID = T4.ROW_ID
JOIN u_analysis_temp.SIEBEL_S_PROD_INT T5 ON T4.PROD_ID = T5.ROW_ID 
LEFT JOIN u_analysis_ods.NOVA_MINIITEM T7 ON T5.PART_NUM = T7.PART_NUM
GROUP BY T2.ORDER_ID,T1.ORDER_DT,T1.COMMIT_TYPE_CD,T1.SRV_PROV_OU_ID,t1.pdate
HAVING SUM(CASE WHEN PAY_AMT<0 THEN -1 ELSE 1 END * ABS(cast(REQUEST_AMT as decimal(14,2)))) >0 AND AMOUNT>0
UNION ALL
select t3.row_id order_id,t3.order_dt,t3.commit_type_cd,'SR-78OFF',t3.SRV_PROV_OU_ID,25,78,t3.pdate
FROM u_analysis_app.s_mem_vchr_one_year t1 join u_analysis_temp.siebel_s_loy_txn t2 join u_analysis_dw.siebel_cx_order t3
on t1.PROD_ID IN('1-41HXQZQP', '1-41HXQZRI') and t1.status_cd='Used' and t1.consumed_txn_id = t2.row_id and t3.valid=1
and t2.orig_order_id = t3.row_id
and t2.pdate>='${p30d}'
and t3.pdate>='${p30d}'
UNION ALL
SELECT t3.row_id order_id,t3.order_dt,t3.commit_type_cd,'STAR',t3.SRV_PROV_OU_ID,stars_used,star_discount*0.01,t3.pdate
FROM u_analysis_dw.oms_order t1 join u_analysis_dw.siebel_cx_order t3 on t1.pdate>='${p30d}' and t1.channel='MOP' and t1.status='5' and t1.stars_used>0 and t1.star_discount>0 and t3.pdate>='${p30d}' and t3.valid=1 and t3.integration_id=t1.id and t3.commit_type_cd in ('MOP','Ali Koubei MOP','APP MOP PreOrder Today')
distribute by pdate,cast(rand()*2 as int);








--参加活动次数
with temp_star as
(
    select member_id,count(distinct promo_id) participate_num FROM u_analysis_temp.siebel_s_loy_acrl_itm where process_dt>=date_add(current_date,-91) group by member_id
),
temp_use as 
(
    select t2.member_id,count(distinct order_id) redeem_time,sum(redeem_num) redeem_num,sum(save_amt) save_amt,sum(total_amt) actual_pay_amt FROM 
    (
        select order_id,sum(star) redeem_num,sum(star_discount) save_amt FROM u_analysis_dw.star_redemption_order where pdate>=date_add(current_date,-91) group by order_id
    )t1 
    join
    (
        select row_id,total_amt,member_id FROM u_analysis_dw.siebel_cx_order where pdate>=date_add(current_date,-91)
    )t2 
    on t1.order_id = t2.row_id group by t2.member_id
)
INSERT OVERWRITE TABLE u_analysis_app.star_tag partition(pdate='${td}')
SELECT t1.member_id,t2.participate_num,t3.redeem_time,t3.redeem_num,t3.actual_pay_amt,t3.save_amt 
FROM u_analysis_dw.siebel_member t1
left join temp_star t2 on t1.member_id = t2.member_id
left join temp_use t3 on t1.member_id = t3.member_id
where t2.member_id is not null or t3.member_id is not null;"


desc u_analysis_temp.siebel_s_loy_acrl_itm;
desc u_analysis_temp.SIEBEL_S_PROD_INT;
desc u_analysis_ods.NOVA_MINIITEM;
desc u_analysis_temp.siebel_s_loy_txn;