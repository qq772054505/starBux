#!/bin/bash
td=`date +%Y-%m-%d`
p3m=`date -d '-91 day' +%Y-%m-%d`

echo $td
echo $p3m


#all
#获得表上一个分区
lastpdate=`/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 4g --conf spark.sql.broadcastTimeout=7200 --conf spark.port.maxRetries=1000 -e "select max(distinct pdate) from u_analysis_app.member_purchase_behavior_recency where channel='all';"| tail -1`

echo $lastpdate


/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"

-----------------------------------------------------------------------
--  功能: 更新tagging RFM daily的指标 
--  修改日期: 2020-11-02
-----------------------------------------------------------------------
--  源表:u_analysis_Dw.siebel_cx_order
--
--  目标表: u_analysis_app.member_purchase_behavior_RFM_daily
--
--  参数: 计算日期 td=yyyy-MM-dd  
--        上一个分区 lastpdate=yyyy-MM-dd
--
--  中间表: u_analysis_temp.temp_recency
--          u_analysis_app.member_purchase_behavior_recency
--
--  数据策略：全量
--  频率：daily
-----------------------------------------------------------------------


use u_analysis_temp;
--获得上一个分区的数据。
with temp_order_recency as 
(
    select * from u_analysis_Dw.siebel_cx_order where pdate>='${p3m}' and pdate <'${td}' and  valid=1 
)
--将上一个分区的数据和最近三十天的数据插入u_analysis_temp.temp_recency表中
insert overwrite table u_analysis_temp.temp_recency
select * from
( 
    select member_id,row_id,recency 
    from u_analysis_app.member_purchase_behavior_recency where pdate='${lastpdate}' and channel='all'
    union all 
    select member_id,row_id,order_dt as recency 
    from temp_order_recency
)p;
--只取最近的一条数据插入u_analysis_app.member_purchase_behavior_recency中
insert overwrite table u_analysis_app.member_purchase_behavior_recency	 partition(pdate='${td}',channel='all')
select member_id,order_id,date_format(recency,'yyyy-MM-dd hh:mm:ss') from
(
    select *,row_number() over (partition by member_id  order by recency desc) rank from u_analysis_temp.temp_recency
)p 
where rank=1;
--通过lead函数获得每个order_id的上一条记录，从而计算出每个用户每单之间相隔的平均天数
with temp_Avg_transaction_interval as 
(
    select member_id,cast(avg(datediff(order_dt,last_order_dt))as decimal(10,2)) as avg_transaction_interval from
    (
        select member_id,order_dt,lead(order_dt) over(partition by member_id order by order_dt desc) last_order_dt from u_analysis_dw.siebel_cx_order where pdate between date_add(current_date(),-91) and current_date()  and  valid=1
    ) t1
    group by member_id
)
insert overwrite table u_analysis_app.member_purchase_behavior_RFM_daily partition(pdate='${td}')
select a.member_id,a.recency_days,b.avg_transaction_interval,cast((b.avg_transaction_interval/a.recency_days) as decimal(10,5))  as Personal_Active_index 
from  
(
    --根据u_analysis_app.member_purchase_behavior_recency的数据计算最近一单距今的天数
    select member_id,datediff(current_date(),recency) as recency_days from u_analysis_app.member_purchase_behavior_recency where channel='all' and pdate='${td}'
) a 
left join temp_Avg_transaction_interval b on  a.member_id=b.member_id;
"


