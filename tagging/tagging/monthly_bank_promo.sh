td=`date  +%Y-%m-%d`
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
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

-----------------------------------------------------------------------
--  功能: 更新tagging三个银行活动指标的数据
--  1.P12M # of transactions enjoyed xx pay promo   xx=promo_chaneel+bank
--  2.P12M faviourate xx Pay Promo
--  3.P12M cost saved via pay promo 
--  创建日期: 2020-11-02
-----------------------------------------------------------------------
--  源表: u_analysis_dw.msr_bank_promo
--  目标表: u_analysis_app.member_campaign_preference_pay_promo_monthly
--  参数: 计算日期 td=yyyy-MM-dd
--  数据策略：全量
--  频率：monthly
-----------------------------------------------------------------------
--生成临时表temp_pay_promo1 计算订单总数和折扣总金额
with temp_pay_promo1 as 
(
    select 
    member_id,
    count(distinct order_id) as transactions, 
    sum(platform_discount)   as cost_save,  
    promo_channel            as promo_chaneel, 
    bank 
    from u_analysis_dw.msr_bank_promo 
    where pdate>=date_add('${td}',-91)  and pdate<='${td}'
    group by member_id,promo_channel,bank
)
--生成临时表temp_pay_promo2 根据temp_pay_promo1 计算订单最多的活动（promo_chaneel+bank）
,temp_pay_promo2 as 
(
    select
    member_id,
    promo_chaneel,
    bank,
    row_number() over(partition by member_id order by transactions desc,cost_save desc) rank 
    from temp_pay_promo1 having rank=1
)
--连接两个临时表，插入目标表
insert overwrite table u_analysis_app.member_campaign_preference_pay_promo_monthly  partition(pdate,promo_chaneel,bank)
select 
t1.member_id,
t1.transactions,
t1.cost_save,
concat(t1.promo_chaneel,'_',t1.bank) as promo_type,
concat(t2.promo_chaneel,'_',t2.bank) as favourite_pay_promo,
'${td}' as pdate,
t1.promo_chaneel,
t1.bank
from
temp_pay_promo1 t1 join temp_pay_promo2 t2 on t1.member_id=t2.member_id
distribute by pdate,promo_chaneel,bank,cast(rand() * 2 as int);
"
