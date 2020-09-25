



insert overwrite table u_analysis_ods.sms_click partition(pdate='2020-08-30') select campaign_name,mobile,date_format(click_date,'yyyy-MM-dd hh:mm:ss') from u_analysis_ods.sms_click_2 where pdate='2020-08-30';

insert overwrite table u_analysis_ods.sms_click partition(pdate='2020-08-30')
select campaign_name,mobile,
case 
when length(click_date)=15 then concat(substr(click_date,1,4),'-0',substr(click_date,6,1),'-',substr(click_date,8,2),' ',substr(click_date,11,5),':00')
when length(click_date)=14 then concat(substr(click_date,1,4),'-0',substr(click_date,6,1),'-',substr(click_date,8,2),' 0',substr(click_date,11,4),':00')
else 'null'  end  click_date  from u_analysis_ods.sms_click_2 where pdate='2020-08-30';



load data local inpath "/data/soft/campaign_sh/0831-0906 点击数据.csv" into table  u_analysis_ods.sms_click_2 partition(pdate='2020-09-06');


insert overwrite table u_analysis_ods.sms_click partition(pdate='2020-09-06')
select campaign_name,mobile,click_date from u_analysis_ods.sms_click_2 where pdate='2020-09-06'