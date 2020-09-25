SELECT MEMBER_ID,SUM(VALID) Trans,SUM(TOTAL_AMT) Amount
INTO TEMP_MEMBER_P3M_VISITS
FROM CRM_Staging.SIEBEL.CX_ORDER O
JOIN MSRSiebel.DBO.SIEBEL_MEMBER M ON O.LOY_MEMBER_ID = M.MEMBER_ID
WHERE O.STATUS_CD = 'CLOSED' AND VALID <> 0
AND ORDER_DT >= DATEADD(MM,-3,'20200602') AND ORDER_DT < '20200602'
AND LEVEL = 'GOLD'
GROUP BY MEMBER_ID

-- 3672398


DROP TABLE TEMP_MEMBER_P3M_VISITS_v1
SELECT MEMBER_ID,Trans,Amount,
CASE 
WHEN Trans_DESC <= 365653 THEN 'Trans_1'
WHEN Trans_DESC <= 365653 * 2 THEN 'Trans_2' 
WHEN Trans_DESC <= 365653 * 3 THEN 'Trans_3' 
WHEN Trans_DESC <= 365653 * 4 THEN 'Trans_4' 
WHEN Trans_DESC <= 365653 * 5 THEN 'Trans_5' 
WHEN Trans_DESC <= 365653 * 6 THEN 'Trans_6' 
WHEN Trans_DESC <= 365653 * 7 THEN 'Trans_7' 
WHEN Trans_DESC <= 365653 * 8 THEN 'Trans_8' 
WHEN Trans_DESC <= 365653 * 9 THEN 'Trans_9' 
ELSE 'Trans_10' END Trans_Seg
into TEMP_MEMBER_P3M_VISITS_v1
FROM (
SELECT MEMBER_ID,Trans,Amount,ROW_NUMBER()OVER(ORDER BY Trans DESC ) Trans_DESC
FROM TEMP_MEMBER_P3M_VISITS
) T


DROP TABLE TEMP_MEMBER_P3M_VISITS_v2
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,
CASE 
WHEN Spend_DESC <= 365653 THEN 'Spend_1'
WHEN Spend_DESC <= 365653 * 2 THEN 'Spend_2' 
WHEN Spend_DESC <= 365653 * 3 THEN 'Spend_3' 
WHEN Spend_DESC <= 365653 * 4 THEN 'Spend_4' 
WHEN Spend_DESC <= 365653 * 5 THEN 'Spend_5' 
WHEN Spend_DESC <= 365653 * 6 THEN 'Spend_6' 
WHEN Spend_DESC <= 365653 * 7 THEN 'Spend_7' 
WHEN Spend_DESC <= 365653 * 8 THEN 'Spend_8' 
WHEN Spend_DESC <= 365653 * 9 THEN 'Spend_9' 
ELSE 'Spend_10' END Spend_Seg
into TEMP_MEMBER_P3M_VISITS_v2
FROM (
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,ROW_NUMBER()OVER(ORDER BY Amount DESC ) Spend_DESC
FROM TEMP_MEMBER_P3M_VISITS_v1
) T



SELECT Trans_Seg,Spend_Seg,COUNT(1) 
FROM TEMP_MEMBER_P3M_VISITS_v2
GROUP BY Trans_Seg, Spend_Seg
ORDER BY  convert(int,SUBSTRING(Spend_Seg,7,2)),convert(int,SUBSTRING(Trans_Seg,7,2))


DROP TABLE u_analysis_test.temp_member_p3m_visits;
CREATE TABLE u_analysis_test.temp_member_p3m_visits AS 
SELECT t1.MEMBER_ID,SUM(VALID) Trans,SUM(TOTAL_AMT) Amount
FROM u_analysis_dw.siebel_cx_order t1
JOIN u_analysis_dw.siebel_member t2 ON t1.MEMBER_ID = t2.MEMBER_ID
LEFT JOIN (select distinct loy_member_id FROM u_analysis_ods.SIEBEL_DELIVERYACCOUNT) t3
on t1.member_id = t3.loy_member_id
WHERE VALID <> 0
AND loy_member_id is null
AND pdate >= ADD_MONTHS(current_date,-3) AND pdate < current_date
AND LEVEL = 'Gold'
GROUP BY t1.MEMBER_ID;

select count(*) FROM u_analysis_test.temp_member_p3m_visits;
--3656531

DROP TABLE IF EXISTS u_analysis_test.temp_member_p3m_visits_trans;
CREATE TABLE u_analysis_test.temp_member_p3m_visits_trans AS
SELECT MEMBER_ID,Trans,Amount,
CASE 
WHEN Trans_DESC <= 365653 THEN 'Trans_1'
WHEN Trans_DESC <= 365653 * 2 THEN 'Trans_2' 
WHEN Trans_DESC <= 365653 * 3 THEN 'Trans_3' 
WHEN Trans_DESC <= 365653 * 4 THEN 'Trans_4' 
WHEN Trans_DESC <= 365653 * 5 THEN 'Trans_5' 
WHEN Trans_DESC <= 365653 * 6 THEN 'Trans_6' 
WHEN Trans_DESC <= 365653 * 7 THEN 'Trans_7' 
WHEN Trans_DESC <= 365653 * 8 THEN 'Trans_8' 
WHEN Trans_DESC <= 365653 * 9 THEN 'Trans_9' 
ELSE 'Trans_10' END Trans_Seg
FROM (
SELECT MEMBER_ID,Trans,Amount,ROW_NUMBER()OVER(ORDER BY Trans DESC) Trans_DESC
FROM u_analysis_test.temp_member_p3m_visits
) T
;

DROP TABLE IF EXISTS u_analysis_test.temp_member_p3m_visits_v2;
CREATE TABLE u_analysis_test.temp_member_p3m_visits_v2 AS
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,
CASE 
WHEN Spend_DESC <= 365653 THEN 'Spend_1'
WHEN Spend_DESC <= 365653 * 2 THEN 'Spend_2' 
WHEN Spend_DESC <= 365653 * 3 THEN 'Spend_3' 
WHEN Spend_DESC <= 365653 * 4 THEN 'Spend_4' 
WHEN Spend_DESC <= 365653 * 5 THEN 'Spend_5' 
WHEN Spend_DESC <= 365653 * 6 THEN 'Spend_6' 
WHEN Spend_DESC <= 365653 * 7 THEN 'Spend_7' 
WHEN Spend_DESC <= 365653 * 8 THEN 'Spend_8' 
WHEN Spend_DESC <= 365653 * 9 THEN 'Spend_9' 
ELSE 'Spend_10' END Spend_Seg
FROM (
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,ROW_NUMBER()OVER(ORDER BY Amount DESC) Spend_DESC
FROM u_analysis_test.temp_member_p3m_visits_trans
) T
;

SELECT Trans_Seg,Spend_Seg,COUNT(1) 
FROM u_analysis_test.temp_member_p3m_visits_v2
GROUP BY Trans_Seg, Spend_Seg
ORDER BY cast(substr(Spend_Seg,7) as int),cast(substr(Trans_Seg,7) as int);


SELECT Trans_Seg,max(trans),min(trans)
FROM u_analysis_test.temp_member_p3m_visits_v2
GROUP BY Trans_Seg
ORDER BY cast(substr(Trans_Seg,7) as int);

SELECT Spend_Seg,cast(max(Amount) as decimal(18,4)),cast(min(Amount) as decimal(18,4))
FROM u_analysis_test.temp_member_p3m_visits_v2
GROUP BY Spend_Seg
ORDER BY cast(substr(Spend_Seg,7) as int);

DROP TABLE IF EXISTS u_analysis_test.temp_member_p3m_visits_p5;
CREATE TABLE u_analysis_test.temp_member_p3m_visits_p5 AS
SELECT MEMBER_ID,Trans,Amount,
CASE 
WHEN Trans_DESC <= 367239 * 2 THEN 'Trans_20' 
WHEN Trans_DESC <= 367239 * 4 THEN 'Trans_40' 
WHEN Trans_DESC <= 367239 * 6 THEN 'Trans_60' 
WHEN Trans_DESC <= 367239 * 8 THEN 'Trans_80' 
ELSE 'Trans_100' END Trans_Seg
FROM (
SELECT MEMBER_ID,Trans,Amount,ROW_NUMBER()OVER(ORDER BY Trans DESC) Trans_DESC
FROM u_analysis_test.temp_member_p3m_visits
) T
;

DROP TABLE IF EXISTS u_analysis_test.temp_member_p3m_visits_p5_v2;
CREATE TABLE u_analysis_test.temp_member_p3m_visits_p5_v2 AS
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,
CASE 
WHEN Spend_DESC <= 367239 * 2 THEN 'Spend_20' 
WHEN Spend_DESC <= 367239 * 4 THEN 'Spend_40' 
WHEN Spend_DESC <= 367239 * 6 THEN 'Spend_60' 
WHEN Spend_DESC <= 367239 * 8 THEN 'Spend_80' 
ELSE 'Spend_100' END Spend_Seg
FROM (
SELECT MEMBER_ID,Trans,Amount,Trans_Seg,ROW_NUMBER()OVER(ORDER BY Amount DESC) Spend_DESC
FROM u_analysis_test.temp_member_p3m_visits_p5
) T
;

select Trans_Seg,count(distinct member_id),cast(sum(Trans) as decimal(18,4)) tran,cast(sum(amount) as decimal(18,4)) amt FROM u_analysis_test.temp_member_p3m_visits_p5_v2 group by Trans_Seg ORDER BY cast(substr(Trans_Seg,7) as int);

select Spend_Seg,count(distinct member_id),cast(sum(Trans) as decimal(18,4)) tran,cast(sum(amount) as decimal(18,4)) amt FROM u_analysis_test.temp_member_p3m_visits_p5_v2 group by Spend_Seg ORDER BY cast(substr(Spend_Seg,7) as int);


SELECT Trans_Seg,max(trans),min(trans)
FROM u_analysis_test.temp_member_p3m_visits_p5_v2
GROUP BY Trans_Seg
ORDER BY cast(substr(Trans_Seg,7) as int);

SELECT Spend_Seg,cast(max(Amount) as decimal(18,4)),cast(min(Amount) as decimal(18,4))
FROM u_analysis_test.temp_member_p3m_visits_p5_v2
GROUP BY Spend_Seg
ORDER BY cast(substr(Spend_Seg,7) as int);


Morning	7-11am
Noon	11-2pm
Afternoon	2-5pm
Dinner	5pm-8pm
Late night	8pm- 7am



with temp_sms_click as(
SELECT t1.*,t2.member_id,case 
when hour(t1.click_date) between 7 and 10 then 'Morning'
when hour(t1.click_date) between 11 and 13 then 'Noon'
when hour(t1.click_date) between 14 and 16 then 'Afternoon'
when hour(t1.click_date) between 17 and 19 then 'Dinner'
else 'Late night' END dayparts
 FROM u_analysis_ods.sms_click t1 join u_analysis_dw.siebel_member t2 on t1.mobile = t2.cell_ph_num
 where pdate>=add_months(current_date,-3) and click_date>=add_months(current_date,-3)
)
select * FROM temp_sms_click limit 3;


with temp_sms_click as(
SELECT t1.*,t2.member_id,case 
when hour(t1.click_date) between 7 and 10 then 'Morning'
when hour(t1.click_date) between 11 and 13 then 'Noon'
when hour(t1.click_date) between 14 and 16 then 'Afternoon'
when hour(t1.click_date) between 17 and 19 then 'Dinner'
else 'Late night' END dayparts
 FROM u_analysis_ods.callback_data t1 join u_analysis_dw.siebel_member t2 on t1.mobile = t2.cell_ph_num
 where pdate>=add_months(current_date,-3) and reportTime>=add_months(current_date,-3)
)
select * FROM temp_sms_click limit 3;
INSERT OVERWRITE TABLE 

select count(distinct member_id),sum(cast(trans as decimal(18,2))),sum(cast(AMOUNT as decimal(18,2))),max(trans),min(trans) FROM
(SELECT t1.MEMBER_ID,SUM(VALID) Trans,SUM(TOTAL_AMT) Amount
FROM u_analysis_dw.siebel_cx_order t1
JOIN u_analysis_dw.siebel_member t2 ON t1.MEMBER_ID = t2.MEMBER_ID
JOIN (select distinct loy_member_id FROM u_analysis_ods.SIEBEL_DELIVERYACCOUNT) t3
on t1.member_id = t3.loy_member_id
WHERE VALID <> 0
AND pdate >= ADD_MONTHS(current_date,-3) AND pdate < current_date
AND LEVEL = 'Gold'
GROUP BY t1.MEMBER_ID);

SELECT count(*) FROM 
u_analysis_dw.siebel_member t1
JOIN (select distinct loy_member_id FROM u_analysis_ods.SIEBEL_DELIVERYACCOUNT) t3
on t1.member_id = t3.loy_member_id
and t1.level='Gold';





CREATE EXTERNAL TABLE `sms_send`(`campaign_name` string, `mobile` string, `deliver_date` string)
PARTITIONED BY (`pdate` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = ',',
  'serialization.format' = ','
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs://ns1/user/u_analysis/public/ods/sms_send'

Black Gold	10%		39%	[24+
Gold 24K	15%		25%	[12-24)
Gold 18K	25%		21%	[6-12)
Gold 14K	25%		11%	[3-6)
Gold 9K	25%		4%	[0-3）



CREATE EXTERNAL TABLE u_analysis_app.`level_tag`(`member_id` string,trans int,amount decimal(14,2),`tag` string)
PARTITIONED BY (`pdate` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://ns1/user/u_analysis/public/app/level_tag';


set td=2020-08-31;
WITH temp_daigou AS (
select distinct loy_member_id FROM u_analysis_ods.SIEBEL_DELIVERYACCOUNT
),temp_mem AS (SELECT t1.MEMBER_ID,level,Trans,Amount
FROM u_analysis_dw.siebel_member t1 
LEFT JOIN (select member_id,cast(SUM(VALID) as int) Trans,sum(cast(TOTAL_AMT as decimal(18,2))) Amount FROM u_analysis_dw.siebel_cx_order where valid<>0 AND pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}' group by member_id) t2 ON t1.MEMBER_ID = t2.MEMBER_ID
)
,temp_fraud as (
SELECT distinct member_id as loy_member_id FROM u_analysis_ods.temp_bound_svc_fraud
)
INSERT OVERWRITE TABLE u_analysis_app.level_tag partition(pdate='${td}')
SELECT member_id,trans,amount,
case 
when t2.loy_member_id is not null and trans>0 then 'Daigou Active'
when t2.loy_member_id is not null then 'Daigou Inactive'
when trans between 0 and 2 and level='Gold' then 'Gold 9K'
when trans between 3 and 5 and level='Gold' then 'Gold 14K'
when trans between 6 and 11 and level='Gold' then 'Gold 18K'
when trans between 12 and 23 and level='Gold' then 'Gold 24K'
when trans >=24 and level='Gold' then 'Black Gold'
when t3.loy_member_id is not null and trans>0 then 'Fraud Active'
when t3.loy_member_id is not null then 'Fraud Inactive'
when level='Gold' then 'Gold inactive' 
when level='Green' and trans>0 then 'Green Active'
when level='Green' then 'Green Inactive'
when level='Welcome' and trans>0 then 'Welcome Active'
when level='Welcome' then 'Welcome Inactive'
END tag FROM temp_mem t1
left join temp_daigou t2 on t1.member_id=t2.loy_member_id
left join temp_fraud t3    on t1.member_id=t3.loy_member_id;


ALTER TABLE u_analysis_app.level_tag FILEFORMAT ORC;


with temp_sms as(
SELECT member_id,a.*,row_number() over(partition by member_id,match_id,campaign_name order by click desc,t)rk FROM 
(SELECT mobile as match_id,campaign_name,deliver_date as t,0 as click FROM u_analysis_ods.sms_send where campaign_name<>'campaignName' and deliver_date>=add_months(current_date,-4)
UNION ALL
select mobile as match_id,campaign_name,click_date as t,1 as click FROM u_analysis_ods.sms_click where campaign_name<>'campaignName' and click_date>=add_months(current_date,-4)
) a 
JOIN u_analysis_dw.siebel_member b 
on a.match_id=b.cell_ph_num
having rk=1
),temp_push as(
SELECT member_id,recipient as match_id,messageid,reportTime as t,case when status='2001' then 0 else 1 END click FROM (select *,row_number() over(partition by recipient,messageid,pdate order by reportTime desc)rk FROM u_analysis_ods.callback_data where pdate>= DATE_ADD('${td}',-90) and requestTime>=DATE_ADD('${td}',-91) and requestTime<'${td}'--当天分区存的T+1
AND status in ('2001','2002') having rk=1) t1 join u_analysis_dw.siebel_member t2 on t1.recipient=t2.member_id
),temp_email as (SELECT member_id,recipient as match_id,messageid,reportTime as t,case when status='4001' then 0 when status='4003' then 1 else 2 END click FROM (select *,row_number() over(partition by recipient,messageid,pdate order by reportTime desc)rk FROM u_analysis_ods.callback_data where pdate>= DATE_ADD('${td}',-90) and requestTime>=DATE_ADD('${td}',-91) and requestTime<'${td}'--当天分区存的T+1
AND status in ('4001','4002','4003')) t1 join u_analysis_dw.siebel_member t2 on t1.recipient=t2.email_addr
)
INSERT OVERWRITE TABLE u_analysis_dw.p3m_communication
SELECT tb.*,case 
when hour(event_time) between 7 and 10 then 'Morning'
when hour(event_time) between 11 and 13 then 'Noon'
when hour(event_time) between 14 and 16 then 'Afternoon'
when hour(event_time) between 17 and 19 then 'Dinner'
else 'Late night' END dayparts FROM
(SELECT member_id,match_id,'SMS' channel,click,campaign_name str_1,'' str_2,'' str_3,t event_time,'${td}' run_date FROM temp_sms
UNION ALL
SELECT member_id,match_id,'PUSH',click,messageid,'','',t,'${td}' FROM temp_push
UNION ALL
SELECT member_id,match_id,'EMAIL',click,messageid,'','',t,'${td}' FROM temp_email) tb;

CREATE EXTERNAL TABLE `u_analysis_dw`.`p3m_communication`(`member_id` string, `match_id` string, `channel` string, `status` int, `str_1` string, `str_2` string, `str_3` string, `event_time` string, `run_date` string,`dayparts` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://ns1/user/u_analysis/public/dw/p3m_communication';

CREATE EXTERNAL TABLE u_analysis_app.`communication_tag`(member_id string,perfer_channel string,prefer_time string,sms_send int,sms_click int,sms_morning int,sms_noon int,sms_afternoon int,sms_dinner int,sms_latenight int,sms_prefer_time string,push_send int,push_click int,push_morning int,push_noon int,push_afternoon int,push_dinner int,push_latenight int,push_prefer_time string,email_send int,email_open int,email_morning int,email_noon int,email_afternoon int,email_dinner int,email_latenight int,email_prefer_time string)
PARTITIONED BY (`pdate` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://ns1/user/u_analysis/public/app/communication_tag';


with temp_mem as (select distinct member_id FROM u_analysis_dw.p3m_communication),
temp_all_channel as (select member_id,channel,count(*) as send_times,sum(case when status>0 then 1 end) clicks FROM u_analysis_dw.p3m_communication group by member_id,channel)
,temp_sms as (select member_id,dayparts,case when dayparts='Morning' then 1 when dayparts='Noon' then 2 when dayparts='Afternoon' then 3 when dayparts='Dinner' then 4 when dayparts='Late night' then 5 END day_weight,count(*) clicks FROM u_analysis_dw.p3m_communication where channel='SMS' and status=1 group by member_id,dayparts)
,temp_push as (select member_id,dayparts,case when dayparts='Morning' then 1 when dayparts='Noon' then 2 when dayparts='Afternoon' then 3 when dayparts='Dinner' then 4 when dayparts='Late night' then 5 END day_weight,count(*) clicks FROM u_analysis_dw.p3m_communication where channel='PUSH' and status=1 group by member_id,dayparts)
,temp_email as (select member_id,dayparts,case when dayparts='Morning' then 1 when dayparts='Noon' then 2 when dayparts='Afternoon' then 3 when dayparts='Dinner' then 4 when dayparts='Late night' then 5 END day_weight,count(*) opens,sum(case when status=1 then 1 end) clicks FROM u_analysis_dw.p3m_communication where channel='EMAIL' and status>=1 group by member_id,dayparts)
,temp_prefer_channel as (select member_id,channel,row_number() over(partition by member_id order by times desc,channel_weight)rk FROM (select member_id,channel,case when channel='SMS' then 1 when channel='PUSH' then 2 else 3 END channel_weight,count(*) as times FROM u_analysis_dw.p3m_communication where status>0 group by member_id,channel) having rk=1)
,temp_prefer_time as (select member_id,dayparts,row_number() over(partition by member_id order by times desc,day_weight)rk FROM (select member_id,dayparts,case when dayparts='Morning' then 1 when dayparts='Noon' then 2 when dayparts='Afternoon' then 3 when dayparts='Dinner' then 4 when dayparts='Late night' then 5 END day_weight,count(*) as times FROM u_analysis_dw.p3m_communication where status=1 group by member_id,dayparts) having rk=1)
INSERT OVERWRITE TABLE u_analysis_app.communication_tag partition(pdate='${td}')
SELECT t1.member_id,t5.channel,t6.dayparts,t2.send_times,t2.clicks,t7.clicks,t8.clicks,t9.clicks,t10.clicks,t11.clicks,t22.dayparts,t3.send_times,t3.clicks,t12.clicks,t13.clicks,t14.clicks,t15.clicks,t16.clicks,t23.dayparts,t4.send_times,t4.clicks,t17.clicks,t18.clicks,t19.clicks,t20.clicks,t21.clicks,t24.dayparts
FROM temp_mem t1
LEFT JOIN temp_all_channel t2 on t1.member_id = t2.member_id AND t2.channel='SMS'
LEFT JOIN temp_all_channel t3 on t1.member_id = t3.member_id AND t3.channel='PUSH'
LEFT JOIN temp_all_channel t4 on t1.member_id = t4.member_id AND t4.channel='EMAIL'
LEFT JOIN temp_prefer_channel t5 on t1.member_id = t5.member_id
LEFT JOIN temp_prefer_time t6 on t1.member_id = t6.member_id
LEFT JOIN temp_sms t7 on t1.member_id = t7.member_id and t7.dayparts='Morning'
LEFT JOIN temp_sms t8 on t1.member_id = t8.member_id and t8.dayparts='Noon'
LEFT JOIN temp_sms t9 on t1.member_id = t9.member_id and t9.dayparts='Afternoon'
LEFT JOIN temp_sms t10 on t1.member_id = t10.member_id and t10.dayparts='Dinner'
LEFT JOIN temp_sms t11 on t1.member_id = t11.member_id and t11.dayparts='Late night'
LEFT JOIN temp_push t12 on t1.member_id = t12.member_id and t12.dayparts='Morning'
LEFT JOIN temp_push t13 on t1.member_id = t13.member_id and t13.dayparts='Noon'
LEFT JOIN temp_push t14 on t1.member_id = t14.member_id and t14.dayparts='Afternoon'
LEFT JOIN temp_push t15 on t1.member_id = t15.member_id and t15.dayparts='Dinner'
LEFT JOIN temp_push t16 on t1.member_id = t16.member_id and t16.dayparts='Late night'
LEFT JOIN temp_email t17 on t1.member_id = t17.member_id and t17.dayparts='Morning'
LEFT JOIN temp_email t18 on t1.member_id = t18.member_id and t18.dayparts='Noon'
LEFT JOIN temp_email t19 on t1.member_id = t19.member_id and t19.dayparts='Afternoon'
LEFT JOIN temp_email t20 on t1.member_id = t20.member_id and t20.dayparts='Dinner'
LEFT JOIN temp_email t21 on t1.member_id = t21.member_id and t21.dayparts='Late night'
LEFT JOIN (select member_id,dayparts,row_number() over(partition by member_id order by clicks desc,day_weight)rk FROM temp_sms having rk=1) t22
on t1.member_id=t22.member_id
LEFT JOIN (select member_id,dayparts,row_number() over(partition by member_id order by clicks desc,day_weight)rk FROM temp_push having rk=1) t23
on t1.member_id=t23.member_id
LEFT JOIN (select member_id,dayparts,row_number() over(partition by member_id order by clicks desc,day_weight)rk FROM temp_email where clicks>0 having rk=1) t24
on t1.member_id=t24.member_id;

when hour(event_time) between 7 and 10 then 'Morning'
when hour(event_time) between 11 and 13 then 'Noon'
when hour(event_time) between 14 and 16 then 'Afternoon'
when hour(event_time) between 17 and 19 then 'Dinner'
else 'Late night' END dayparts FROM




--sr kit boundle
with temp_bundled as (select member_id,count(distinct card_num) cards,count(distinct case when x_card_type in ('0170','0185','0186','0187','0188','0202','0203','0204','0205','0213','0214','0215','0216') then card_num end) digital_cards,count(distinct case when x_card_type not in ('0170','0185','0186','0187','0188','0202','0203','0204','0205','0213','0214','0215','0216') then card_num end) core_cards FROM u_analysis_temp.siebel_vw_s_loy_card where start_dt>=date_add(current_date,-365) and start_dt<current_date and x_card_type in ('0151','0152','0153','0154','0155','0156','0157','0158','0159','0160','0161','0162','0163','0164','0165','0166','0167','0168','0169','0170','0171','0172','0173','0174','0175','0176','0177','0185','0186','0187','0188','0178','0179','0180','0181','0182','0183','0184','0189','0190','0191','0192','0193','0194','0199','0200','0201','0198','0195','0202','0203','0204','0205','0196','0197','0206','0207','0208','0213','0214','0215','0216','0209','0210','0211','0212','0217','0218','0219','0220') and x_returncard_flg<>'Y' group by member_id)
,temp_vchr as (SELECT ROW_ID,PART_NUM,MEMBER_ID,USED_DT,CONSUMED_TXN_ID FROM u_analysis_app.s_mem_vchr_one_year where from_unixtime(used_dt,'yyyy-MM-dd') between date_add(current_date,-365) and date_add(current_date,-1) and status_cd='Used' and x_card_num<>'null')
,temp_redeem as (
SELECT t1.*,
TXN_CHANNEL_CD,t5.INTEGRATION_ID,ORIG_ORDER_ID,AMT_VAL
FROM
(SELECT ROW_ID,PART_NUM as coupon_id,MEMBER_ID,
from_unixtime(USED_DT,'yyyy-MM-dd') AS USED_DT,CONSUMED_TXN_ID
FROM temp_vchr) t1
JOIN
(select * from u_analysis_temp.siebel_s_loy_txn where pdate>=date_add(current_date,-365)) t3
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
(SELECT ORDER_ID,SUM(PAY_AMT) PAY_AMT,TYPE_CD,PAID_BY,INVOICE_NUM FROM u_analysis_dw.siebel_cx_src_payment where pdate>=date_add(current_date,-365) and TYPE_CD = 'Discount' GROUP BY ORDER_ID,TYPE_CD,PAID_BY,INVOICE_NUM) t6
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
INSERT OVERWRITE TABLE u_analysis_test.temp_sr_kit_tag 
select t1.member_id,t2.cards,t3.coupons,t3.discount,t2.core_cards,t2.digital_cards from u_analysis_dw.siebel_member t1
left join temp_bundled t2 on t1.member_id = t2.member_id
left join
(select member_id,count(distinct row_id)coupons,sum(discount) discount FROM temp_all group by member_id) t3
on t1.member_id = t3.member_id
where t2.member_id is not null or t3.member_id is not null;

create table u_analysis_temp.temp_sr_kit_redeemed AS
SELECT member_id,count(distinct row_id) FROM u_analysis_app.s_mem_vchr_one_year where from_unixtime(used_dt,'yyyy-MM-dd') between date_add(current_date,-365) and date_add(current_date,-1) and status_cd='Used' and x_card_num<>'null' group by member_id;


select count(distinct member_id),count(distinct card_num) FROM u_analysis_temp.siebel_vw_s_loy_card where start_dt>=date_add(current_date,-365) and start_dt<current_date and  x_prod_id in ('1-2CEK4UYZ','1-3DITA7JH','1-2OSF6KXE','1-2SO9OPJN','1-3OOWBW1W','1-2OSF6FYJ','1-3DITA7MP','1-2KVAKDD2','1-3U9EFG8S','1-3OOWCNK9','1-2QRWYP6L','1-3OOWCNIK','1-39QPQIIL','1-2DY3SKP2','1-27HEMPKN','1-2SO9OXNX','1-27HEMX5R','1-3DITA7LW','1-3DITA7L3','1-3U9ED778','1-3AIAHYT9','1-3DITA7BL','1-41SG5HZX','1-3AIAK8HX','1-3DITA7KA','1-3I9BHOR1','1-3I9BKTXT','1-41SG4YGW','1-41SG58O4','1-3I9BHORT','1-3AIAK8IP','1-3OOWBW6Z','1-3SUPBWT9','1-3AIAHYSH','1-27HEME84','1-41SG5YI3','1-2KVAKFO4','1-41SG4YI5','1-3DITA7CE','1-2CEK4HQQ','1-2ZVMAURL','1-2CEK4HQ0','1-2ZVMKW0U','1-3AIAHYRP','1-2OSF6BX1','1-3I9BHOQ9','1-41SG4Y9O','1-27HEME7D','1-3I9BHONL','1-39QPSE1P','1-3DITA7FE','1-2HYHIGTQ','1-3SUPJRLZ','1-41SG4YC9','1-39QPSE4P','1-36S0UDCJ','1-3SUPF8KQ','1-2KVAGNEQ','1-3XC2LB7J','1-2CEK4UZR','1-3DITA7H1','1-3I9BHOPH','1-3RCU4NY9','1-3XC2NYYG','1-3SUPF8JY','1-2ZVML6P7','1-3U9EDYX0','1-3AIAK8LX','1-2DY3SJNQ','1-2OSF6KY6','1-39QPSE5H','1-2SO9OO3P','1-3AIAK8JH','1-3DITA7D7','1-3DITA7IO','1-2KVAKDCB') and x_returncard_flg<>'Y' group by member_id;


--Stars_rewards_sensitivity
with temp_star as(select row_id,member_id,promo_id,accrualed_value,attrib_defn_id FROM u_analysis_temp.siebel_s_loy_acrl_itm where promo_id in ('1-41ZKH2U0','1-436FV10L','1-41ZKH2ZD','1-436FV15M','1-41ZKH355','1-436FV1AZ','1-41ZKH3BC','1-436FV1GQ') and process_dt>='2020-05-23')
,temp_vchr_78 as (select row_id,txn_id,status_cd,x_batch_id,row_number() over(partition by row_id order by last_upd desc)rk FROM bigdata.siebel_s_loy_mem_vchr where dt>='20200525' and PROD_ID IN('1-41HXQZQP', '1-41HXQZRI') and vchr_eff_start_dt>= date_add(current_date,-92) having rk=1)
,temp_vchr_set_txn_id as (
SELECT distinct * FROM (
select t1.row_id,t2.txn_id,t1.status_cd,t1.x_batch_id FROM temp_vchr_78 t1 join temp_vchr_78 t2 on t1.x_batch_id=t2.x_batch_id and t1.txn_id='null' and t2.txn_id<>'null'
UNION ALL
select row_id,txn_id,status_cd,x_batch_id FROM temp_vchr_78 where txn_id<>'null')a
)
,temp_redeem as(select t1.member_id,t1.txn_id,SUM(t1.value) stars
from u_analysis_temp.siebel_s_loy_rdm_itm  t1
 join temp_star t2 on t1.accrual_item_id = t2.ROW_ID
    join u_analysis_temp.siebel_s_loy_txn t3 on t1.txn_id = t3.ROW_ID 
where t1.type_cd = 'Product' and t3.sub_type_cd = 'Product' AND t3.status_cd !='Cancelled' AND T2.attrib_defn_id='1-2RV8' and t3.pdate>='2020-05-20' and to_date(t1.process_dt) >= '2020-05-23' group by t1.member_id,t1.txn_id)
,temp_redeem_vchr78 as (
select member_id,sum(stars) redeem FROM (
SELECT distinct t1.*,t2.status_cd FROM temp_redeem t1 LEFT join temp_vchr_set_txn_id t2 on t1.txn_id=t2.txn_id where t2.status_cd is NULL or status_cd='Used') group by member_id
)
INSERT OVERWRITE TABLE u_analysis_test.temp_Stars_rewards_sensitivity 
SELECT t1.member_id,times,redeem,gets FROM (select member_id,count(distinct promo_id) times,sum(accrualed_value) gets FROM temp_star group by member_id) t1 left join temp_redeem_vchr78 t2 on t1.member_id=t2.member_id;


--core redeem
with temp_vchr as (SELECT ROW_ID,PART_NUM,MEMBER_ID,USED_DT,CONSUMED_TXN_ID FROM u_analysis_app.s_mem_vchr_one_year where from_unixtime(used_dt,'yyyy-MM-dd') between date_add(current_date,-365) and date_add(current_date,-1) and status_cd='Used' and prod_id in ('BFP_00000000001','BFP_00000000002','1-1ZBAAE8T','1-1ZBAAE93','1-1ZBAAE9D'))
,temp_redeem as (
SELECT t1.*,
TXN_CHANNEL_CD,t5.INTEGRATION_ID,ORIG_ORDER_ID,AMT_VAL
FROM
(SELECT ROW_ID,PART_NUM as coupon_id,MEMBER_ID,
from_unixtime(USED_DT,'yyyy-MM-dd') AS USED_DT,CONSUMED_TXN_ID
FROM temp_vchr) t1
JOIN
(select * from u_analysis_temp.siebel_s_loy_txn where pdate>=date_add(current_date,-365)) t3
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
(SELECT ORDER_ID,SUM(PAY_AMT) PAY_AMT,TYPE_CD,PAID_BY,INVOICE_NUM FROM u_analysis_dw.siebel_cx_src_payment where pdate>=date_add(current_date,-365) and TYPE_CD = 'Discount' GROUP BY ORDER_ID,TYPE_CD,PAID_BY,INVOICE_NUM) t6
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
INSERT OVERWRITE TABLE u_analysis_test.temp_core_benefits_tag 
select t1.member_id,t3.coupons,t3.discount from u_analysis_dw.siebel_member t1
join
(select member_id,count(distinct row_id) coupons,sum(discount) discount FROM temp_all group by member_id) t3
on t1.member_id = t3.member_id
;


--p3m coupon redeem
with temp_vchr as (SELECT ROW_ID,PART_NUM,MEMBER_ID,USED_DT,CONSUMED_TXN_ID FROM u_analysis_app.s_mem_vchr_one_year where from_unixtime(used_dt,'yyyy-MM-dd') between date_add(current_date,-91) and date_add(current_date,-1) and status_cd='Used')
,temp_redeem as (
SELECT t1.*,
TXN_CHANNEL_CD,t5.INTEGRATION_ID,ORIG_ORDER_ID,AMT_VAL
FROM
(SELECT ROW_ID,PART_NUM as coupon_id,MEMBER_ID,
from_unixtime(USED_DT,'yyyy-MM-dd') AS USED_DT,CONSUMED_TXN_ID
FROM temp_vchr) t1
JOIN
(select * from u_analysis_temp.siebel_s_loy_txn where pdate>=date_add(current_date,-91)) t3
on t1.CONSUMED_TXN_ID=t3.ROW_ID
LEFT JOIN
u_analysis_app.s_benefit t5
on t1.coupon_id=t5.part_num
)
,temp_MOD as (
select row_id,ORIG_ORDER_ID,coupon_id,member_id,used_dt,AMT_VAL as discount,
case when TXN_CHANNEL_CD in ('MOD','MOD_WECHAT') then 'MOD' ELSE 'MOP' END as channel FROM temp_redeem where TXN_CHANNEL_CD in ('MOD','MOP','MOD_WECHAT')
),temp_instore as (
select t1.row_id,ORIG_ORDER_ID,coupon_id,t1.member_id,used_dt,PAY_AMT as discount,'Retail' as channel FROM (select * from temp_redeem where TXN_CHANNEL_CD not in ('MOD','MOP','MOD_WECHAT')) t1
JOIN
(SELECT ORDER_ID,SUM(PAY_AMT) PAY_AMT,TYPE_CD,PAID_BY,INVOICE_NUM FROM u_analysis_dw.siebel_cx_src_payment where pdate>=date_add(current_date,-91) and TYPE_CD = 'Discount' GROUP BY ORDER_ID,TYPE_CD,PAID_BY,INVOICE_NUM) t6
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
INSERT OVERWRITE TABLE u_analysis_test.temp_p3m_coupons_tag 
select t1.member_id,t3.trans,t3.coupons,t3.discount from u_analysis_dw.siebel_member t1
join
(select member_id,count(distinct ORIG_ORDER_ID) trans,count(distinct row_id) coupons,sum(discount) discount FROM temp_all group by member_id) t3
on t1.member_id = t3.member_id;




--LTO
with temp_prod_id as (select distinct row_id FROM u_analysis_app.s_prod t1 join bigdata.view_cube_dim_saleitem t2 on t1.part_num=t2.simphonyitemcode where x_b3g1_flg='Y' and t2.dt='20200630' and upper(t2.isltoflag)='LTO')
,temp_order_item as (
select t1.row_id,member_id,order_id,cast(rollup_pri - discnt_amt as decimal(10,2)) discnt FROM u_analysis_dw.siebel_cx_order_item t1 join temp_prod_id t2 on t1.prod_id=t2.row_id where pdate between date_add(current_date,-91) and date_add(current_date,-1)
),
temp_order as (
select t1.*,t2.valid from temp_order_item t1 join u_analysis_dw.siebel_cx_order t2 on t1.order_id=t2.row_id where pdate between date_add(current_date,-91) and date_add(current_date,-1) and valid <>0 
)
insert overwrite table u_analysis_test.temp_lto_tag
SELECT member_id,count(distinct case when discnt>0 then order_id end)-count(distinct case when discnt<0 then order_id end) dis_trans,(count(distinct case when valid=1 then order_id end)-count(distinct case when valid=-1 then order_id end))-(count(distinct case when discnt>0 then order_id end)-count(distinct case when discnt<0 then order_id end)) without_dis_trans,sum(discnt) discount FROM temp_Order group by member_id having dis_trans+without_dis_trans>0;


--starRedeem
with temp_vchr_78 as (select row_id,txn_id,status_cd,x_batch_id,row_number() over(partition by row_id order by last_upd desc)rk FROM bigdata.siebel_s_loy_mem_vchr where dt>='20200520' and PROD_ID IN('1-41HXQZQP', '1-41HXQZRI') and vchr_eff_start_dt>= date_add(current_date,-92) having rk=1)
,temp_vchr_set_txn_id as (
SELECT distinct * FROM (
select t1.row_id,t2.txn_id,t1.status_cd,t1.x_batch_id FROM temp_vchr_78 t1 join temp_vchr_78 t2 on t1.x_batch_id=t2.x_batch_id and t1.txn_id='null' and t2.txn_id<>'null'
UNION ALL
select row_id,txn_id,status_cd,x_batch_id FROM temp_vchr_78 where txn_id<>'null')a
),temp_star_redeem as (
select t1.member_id,t1.txn_id,SUM(t1.value) redeem
from u_analysis_temp.siebel_s_loy_rdm_itm t1
 join u_analysis_temp.siebel_s_loy_acrl_itm t2 on t1.accrual_item_id = t2.ROW_ID
    join u_analysis_temp.siebel_s_loy_txn t3 on t1.txn_id = t3.ROW_ID 
where t1.type_cd = 'Product' and t3.sub_type_cd = 'Product' AND t3.status_cd !='Cancelled' AND T2.attrib_defn_id='1-2RV8' and t3.pdate>=date_add(current_date,-92) and to_date(t1.process_dt) between date_add(current_date,-91) and date_add(current_date,-1) group by t1.member_id,t1.txn_id
),temp_redeem_vchr78 as (
SELECT distinct t1.*,t2.status_cd FROM temp_star_redeem t1 LEFT join temp_vchr_set_txn_id t2 on t1.txn_id=t2.txn_id
)
INSERT OVERWRITE TABLE u_analysis_test.temp_star_redemption_tag
select member_id,count(distinct txn_id),sum(redeem),NULL FROM temp_redeem_vchr78 where status_cd is NULL or status_cd='Used' group by member_id
;

drop table u_analysis_app.campaign_tag;
CREATE EXTERNAL TABLE u_analysis_app.campaign_tag(member_id string, stars_rewards_times int,stars_rewards_stars decimal(14,4), stars_rewards_redeem decimal(14,4), star_redeemed_times int,star_redeemed_num decimal(14,4),star_redeemed_discount decimal(14,2),coupon_used_times int,coupon_used int,coupon_discount decimal(14,2),sr_kit int,sr_kit_redeemed int,sr_kit_discount decimal(14,2),sr_kit_core int,sr_kit_digital int,core_benefit_used int,core_benefit_discount decimal(14,2),lto_bev_trans_with_discount int,lto_bev_trans_without_discount int,lto_bev_discount decimal(14,2))
PARTITIONED BY (pdate string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://ns1/user/u_analysis/public/app/campaign_tag';
;
INSERT OVERWRITE TABLE u_analysis_app.campaign_tag partition(pdate='2020-06-30')
SELECT t1.member_id,t2.times,t2.gets,t2.stars,t3.trans,t3.stars,t3.discount,t4.trans,t4.coupons,t4.discount,t5.cards,t5.coupons,t5.discount,t5.core_cards,t5.digital_cards,t6.coupons,t6.discount,t7.dis_trans,t7.without_dis_trans,t7.discount
FROM u_analysis_dw.siebel_member t1
left join u_analysis_test.temp_Stars_rewards_sensitivity t2
on t1.member_id = t2.member_id
left join u_analysis_test.temp_star_redemption_tag t3
on t1.member_id = t3.member_id
left join u_analysis_test.temp_p3m_coupons_tag t4
on t1.member_id = t4.member_id
left join u_analysis_test.temp_sr_kit_tag t5
on t1.member_id = t5.member_id
left join u_analysis_test.temp_core_benefits_tag t6
on t1.member_id = t6.member_id
left join u_analysis_test.temp_lto_tag t7
on t1.member_id = t7.member_id
where t2.member_id is not null or 
t3.member_id is not null or
t4.member_id is not null or
t5.member_id is not null or
t6.member_id is not null or
t7.member_id is not null;


MEM_00004305387
MEM_00004480653
1-29J0H5GK
1-18H28KYI
1-3NQYAU1Y
MEM_00001461255
MEM_00005559718
1-231IHCBI
1-2E5QW4ZS
MEM_00016097258

