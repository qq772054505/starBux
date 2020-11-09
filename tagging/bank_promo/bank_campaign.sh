td=`date -d '-1 days'  +%Y-%m-%d`
nd=`date -d '-2 days'  +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
-----------------------------------------------------------------------
--  功能: 每天清洗urp数据到银行活动表
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_ods.urp_wechat
--       u_analysis_ods.urp_alipay
--       u_analysis_ods.urp_bank
--       u_analysis_dw.siebel_cx_order
--  目标表: u_analysis_dw.msr_bank_promo
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：daily
-----------------------------------------------------------------------
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.optimize.sort.dynamic.partition=true;
set spark.sql.shuffle.partitions=400;
--微信
--创建临时表获得还在活动期内的微信活动
with temp_bank_campaign as 
(select * from u_analysis_temp.bank_campaign where '${td}'>=start_date and  '${td}'<=end_date and date_format('${td}','u')=day and discount_chaneel='wechat' )
--创建临时表，用temp_bank_campaign的活动折扣金额去匹配urp微信数据
,temp_wechat as 
(select * from u_analysis_ods.urp_wechat a  join temp_bank_campaign b on cast(a.platform_discount as double)=b.discount_amount 
and  trade_type='xf' and amount>0  and pdate='${td}')
--根据线上线下数据，分别用wechat_order_id和business_order_id去连接ESB数据
--取只能连上一条的数据去除退单
,temp_esb1 as 
(select *,wechat_order_id as external_order_id,'wechat_order_id' as map_col,count(1) over(partition by b.svc_or_b2b_number)  as row  
from temp_wechat a
join u_analysis_dw.esb_payment_item b on  a.wechat_order_id=b.svc_or_b2b_number  
and  a.wechat_order_id <>''   and  b.pdate='${td}'  and a.service_type in('MOP','APP MOP PreOrder Today','App Delivery','APP MOD PreOrder Today')  
having row=1)
,temp_esb2 as 
(select *,business_order_id as external_order_id,'business_order_id' as map_col,count(1) over(partition by b.svc_or_b2b_number)  as row  
from temp_wechat a
join u_analysis_dw.esb_payment_item b on  a.business_order_id=b.svc_or_b2b_number 
and  a.business_order_id<>'' and b.pdate='${td}' and a.service_type in('For Here','DineIn','ToGo','EX Service','Other','null')
having row=1)
--分成线上线下数据各自连接到order表后做union操作，插入目标表 
insert overwrite table u_analysis_dw.msr_bank_promo partition(promotion_tag,pdate)
select 
member_id,
external_order_id,
map_col,
row_id,
order_dt,
cast(cast(amount as double)-cast(discount_amount as double) as decimal(10,2)),
business_discount,
cast(discount_amount as decimal(10,2)),
'wechat'   as   promo_channel,
bank   as  bank,
promotion_tag as   promotion_tag,
trade_date as pdate    
from (
select a.*,c.* 
from temp_esb1 a
--线上数据需要连接两次esb数据，拿到receipt_number相同的另一条数据，再用这一条数据的svc_or_b2b_number去连接order表
join u_analysis_dw.esb_payment_item b on  b.receipt_number=a.receipt_number and b.pdate='${td}' 
join u_analysis_dw.siebel_cx_order  c on  b.svc_or_b2b_number=c.integration_id and c.pdate='${td}'
union all 
select a.*,b.* 
from temp_esb2 a  
--线下数据连接esb后直接用receipt_number去连接order表
join u_analysis_dw.siebel_cx_order b  on  a.receipt_number=b.integration_id and b.pdate='${td}') as t;


--支付宝
--支付宝的数据清洗逻辑与微信相同
--创建临时表获得还在活动期内的支付宝活动
with temp_bank_campaign as 
(select * from u_analysis_temp.bank_campaign where '${td}'>=start_date and  '${td}'<=end_date and date_format('${td}','u')=day and discount_chaneel='alipay' )
,temp_alipay as 
(select * from u_analysis_ods.urp_alipay a  join temp_bank_campaign b on cast(a.platform_discount as double)=b.discount_amount and  pdate='${td}'
and  trade_type='xf' and amount>0 
)
,temp_esb1 as 
(select *,alipay_order_id as external_order_id,'alipay_order_id' as map_col,count(1) over(partition by b.svc_or_b2b_number)  as row  
from temp_alipay a
join u_analysis_dw.esb_payment_item b on  a.alipay_order_id=b.svc_or_b2b_number  and  a.alipay_order_id<>''  and b.pdate='${td}'  
and a.service_type in('MOP','APP MOP PreOrder Today','App Delivery','APP MOD PreOrder Today')
having row=1)
,temp_esb2 as 
(select *,business_order_id as external_order_id,'business_order_id' as map_col,count(1) over(partition by b.svc_or_b2b_number)  as row  
from temp_alipay a
join u_analysis_dw.esb_payment_item b on  a.business_order_id=b.svc_or_b2b_number and  a.business_order_id<>'' and b.pdate='${td}' 
and a.service_type in('For Here','DineIn','ToGo','EX Service','Other','null')
having row=1)
insert overwrite table u_analysis_dw.msr_bank_promo partition(promotion_tag,pdate)
select 
member_id,
external_order_id,
map_col,
row_id,
order_dt,
cast(cast(amount as double)-cast(discount_amount as double) as decimal(10,2)),
business_discount,
cast(discount_amount as decimal(10,2)),
'alipay'   as  promo_channel,
bank       as  bank,
promotion_tag as   promotion_tag,
trade_date as pdate   
from (
select a.*,c.* 
from temp_esb1 a  
join u_analysis_dw.esb_payment_item b on  b.receipt_number=a.receipt_number  and a.svc_or_b2b_number<>b.svc_or_b2b_number and b.pdate='${td}' 
join u_analysis_dw.siebel_cx_order  c on  b.svc_or_b2b_number=c.integration_id and c.pdate='${td}'
union all 
select a.*,b.* 
from temp_esb2 a  
join u_analysis_dw.siebel_cx_order b  on  a.receipt_number=b.integration_id and b.pdate='${td}') as t;




--bank
--创建临时表获得还在活动期内的银行和unionpay活动
with temp_bank_campaign as 
(select * from u_analysis_temp.bank_campaign where '${td}'>=start_date and  '${td}'<=end_date  and discount_chaneel in ('bank','unionPay'))
--先根据svc_or_b2b_number,receipt_number两个字段给ESB数据去重，因为bank数据连接到ESB数据后会有两条数据，一条原价，一条折扣。
,temp_esb as 
(
  select *,row_number() over(partition by svc_or_b2b_number,receipt_number order by transaction_datetime) rank from  u_analysis_dw.esb_payment_item 
  where pdate in ('${td}','${nd}')  having rank=1  
)
--bank数据连接活动表只看活动码
,temp_bank as 
(
  select *,a.discount_amount as discount from u_analysis_ods.urp_bank a  join temp_bank_campaign b on a.discount_campaign_id=b.discount_campaign_id  
  and trade_date in ('${td}','${nd}') and pdate in('${td}','${nd}')  and trade_type='xf' and amount>0
)
--和wechat和alipay一样去退单，不同的是都是用business_order_id去连接订单
,temp_order1 as
(
select *,count(1) over(partition by a.business_order_id)  as row1  
from temp_bank a
join temp_esb b on a.business_order_id=b.svc_or_b2b_number  and  a.business_order_id<>''   
having row1=1
)
,temp_order2 as
(
select *,b.svc_or_b2b_number as svc,count(1) over(partition by a.receipt_number)  as row2  
from temp_order1 a
join temp_esb b on a.receipt_number=b.receipt_number  and  a.receipt_number<>'' and b.svc_or_b2b_number<>''  and  a.svc_or_b2b_number<>b.svc_or_b2b_number   
having row2=1
)
--下面和wechat和alipay一样,分成线上线下数据各自连接到order表后做union操作，插入目标表 
insert overwrite table u_analysis_dw.msr_bank_promo partition(promotion_tag,pdate)
select 
member_id,
external_order_id,
'business_order_id',
order_id, 
order_date,
payment_amount,
0,
platform_discount,
promo_channel,
bank,
promotion_tag,
pdate  
from
(
  select 
  member_id,
  business_order_id  as external_order_id,
  row_id as order_id,
  order_dt as order_date,
  cast(cast(amount as double)-cast(discount as double) as decimal(10,2))  as payment_amount,
  cast(discount as decimal(10,2))  as platform_discount,
  discount_chaneel   as   promo_channel,
  bank as bank, 
  promotion_tag as  promotion_tag,
  trade_date as pdate
  from
  temp_order1 a  
  join u_analysis_dw.siebel_cx_order b  on a.receipt_number=b.integration_id and   b.pdate in ('${td}','${nd}') 
  union all 
  select 
  member_id,
  business_order_id,
  row_id,
  order_dt,
  cast(cast(amount as double)-cast(discount as double) as decimal(10,2)),
  cast(discount as decimal(10,2)),
  discount_chaneel   as   promo_channel,
  bank as bank, 
  promotion_tag as  promotion_tag,
  trade_date as pdate
  from
  temp_order2 a 
  join u_analysis_dw.siebel_cx_order b  on a.svc=b.integration_id and  b.pdate in ('${td}','${nd}') 
)as t;
"
