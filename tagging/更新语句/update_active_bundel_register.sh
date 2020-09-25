#!/bin/bash
td=`date +%Y-%m-%d`

/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 2g --num-executors 4 --executor-cores 4 --executor-memory 16g  -e"
--bundle
--ali
insert overwrite table u_analysis_app.member_bundle partition(pdate='${td}',channel='2')
select p.member_id,case when sum(cast(p.bind_flag as int))> 0 then 1 else 0 end bundle  from (
select a.member_id as member_id,
bind_flag
from u_analysis_dw.siebel_member a 
join u_analysis_ods.tb_member_3app b on a.member_id=b.customer_id and b.pdate='${td}' and a.REG_CHANNEL_CD not  in('3PP-AP','3PP-EM','3PP-FZ','3PP-TB','3PP-TM','taobao') and b.app_enum in('1100','1101','1102','1103'))p group by p.member_id;
--wechat
insert overwrite table u_analysis_app.member_bundle partition(pdate='${td}',channel='3')
select p.member_id,case when sum(cast(p.bind_flag as int))> 0 then 1 else 0 end bundle  from (
select a.member_id as member_id,
bind_flag
from u_analysis_dw.siebel_member a 
join u_analysis_ods.tb_member_3app b on a.member_id=b.customer_id and b.pdate='${td}' and a.REG_CHANNEL_CD not in('WeChat','MINI-WECHAT','MINI-WECHAT-OFF') and b.app_enum in('1201','1202','1203'))p group by p.member_id;
--cmb
insert overwrite table u_analysis_app.member_bundle partition(pdate='${td}',channel='4')
select p.member_id,case when sum(cast(p.bind_flag as int))> 0 then 1 else 0 end bundle  from (
select a.member_id as member_id,
bind_flag
from u_analysis_dw.siebel_member a 
join u_analysis_ods.tb_member_3app b on a.member_id=b.customer_id and b.pdate='${td}' and a.REG_CHANNEL_CD<>'3PP-CMB' and b.app_enum='1501')p group by p.member_id;
--apple
insert overwrite table u_analysis_app.member_bundle partition(pdate='${td}',channel='5')
select p.member_id,case when sum(cast(p.bind_flag as int))> 0 then 1 else 0 end bundle  from (
select a.member_id as member_id,
bind_flag
from u_analysis_dw.siebel_member a 
join u_analysis_ods.tb_member_3app b on a.member_id=b.customer_id and b.pdate='${td}' and a.REG_CHANNEL_CD<>'APP-APPLE' and b.app_enum = '1401')p group by p.member_id;

--active
--ali
use u_analysis_app;
with temp_payment as
(select * from u_analysis_dw.siebel_cx_src_payment where pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='2',type='P3M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_payment c on c.member_id=a.member_id  and desc_text='Alipay';
--cmb
use u_analysis_app;
with temp_payment as
(select * from u_analysis_dw.siebel_cx_src_payment where pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='4',type='P3M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_payment c on c.member_id=a.member_id  and desc_text in ('CMB O2O','CMB POINT','CMB2014');
--wechat
use u_analysis_app;
with temp_order as
(select * from u_analysis_dw.siebel_cx_order where pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='3',type='P3M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_order c on c.member_id=a.member_id  and COMMIT_TYPE_CD='Wechat Delivery'  and valid=1;
--elema
use u_analysis_app;
with temp_order as
(select * from u_analysis_dw.siebel_cx_order where pdate >= DATE_ADD('${td}',-91) AND pdate < '${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='6',type='P3M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_order c on c.member_id=a.member_id  and COMMIT_TYPE_CD='Eleme'  and valid=1;
--app
use u_analysis_app;
with temp_app_active as
(select * from u_analysis_ods.REF_ACTIVEAPPUSERSFULLLIST where pdate >= DATE_ADD('${td}',-91) AND pdate <'${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='1',type='P3M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_app_active c on c.member_id=a.member_id;
use u_analysis_app;
with temp_app_active as
(select * from u_analysis_ods.REF_ACTIVEAPPUSERSFULLLIST where pdate >= DATE_ADD('${td}',-182) AND pdate <'${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='1',type='P6M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_app_active c on c.member_id=a.member_id;
use u_analysis_app;
with temp_app_active as
(select * from u_analysis_ods.REF_ACTIVEAPPUSERSFULLLIST where pdate >= DATE_ADD('${td}',-365) AND pdate <'${td}')
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='1',type='P12M')
select distinct a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_app_active c on c.member_id=a.member_id;
--physicalcard
with temp_physicalcard as(
select distinct MEMBER_ID 
from u_analysis_dw.SIEBEL_CX_ORDER O
where O.STATUS_CD='Closed' and O.VALID<>0 
and O.COMMIT_TYPE_CD in ('ToGo','For Here','EX Service')
and date(O.ORDER_DT) between date_add(current_date(),-91) and current_date() and O.pdate between date_add(current_date(),-91) and date_add(current_date(),-1)
and not exists(select ORDER_ID from(
select distinct ORDER_ID
from u_analysis_dw.SIEBEL_CX_ORDER O
join u_analysis_dw.SIEBEL_CX_ORDER_ITEM OI
on O.ROW_ID=OI.ORDER_ID
where OI.PROD_ID in ('1-36Q4UB7T','PDC_00000005282','1-46EWDQFJ','1-3DIT0Q6Y')
and O.STATUS_CD='Closed' and O.VALID<>0 
and date(O.ORDER_DT) between date_add(current_date(),-91) and current_date() and o.pdate between date_add(current_date(),-91) and date_add(current_date(),-1) and Oi.pdate between date_add(current_date(),-91) and date_add(current_date(),-1) 
) t2 where O.ROW_ID=t2.ORDER_ID))
insert overwrite table u_analysis_app.member_channel_active partition(pdate='${td}',channel='0',type='P3M')
select a.member_id as member_id,
case when c.member_id is not null then 1 else 0 end  active
from u_analysis_dw.siebel_member a 
left join temp_physicalcard c on c.member_id=a.member_id;

--Register
--ali
insert overwrite table u_analysis_app.member_Register_channel partition(pdate='${td}',channel='2')
select a.member_id as member_id,
case when b.member_id is null then 0 else 1 end Register
from u_analysis_dw.siebel_member a 
left join u_analysis_dw.siebel_member b on a.member_id=b.member_id and b.REG_CHANNEL_CD in('3PP-AP','3PP-EM','3PP-FZ','3PP-TB','3PP-TM','taobao');
--wechat
insert overwrite table u_analysis_app.member_Register_channel partition(pdate='${td}',channel='3')
select a.member_id as member_id,
case when b.member_id is null then 0 else 1 end Register
from u_analysis_dw.siebel_member a 
left join u_analysis_dw.siebel_member b on a.member_id=b.member_id and b.REG_CHANNEL_CD in('WeChat','MINI-WECHAT','MINI-WECHAT-OFF');
--cmb
insert overwrite table u_analysis_app.member_Register_channel partition(pdate='${td}',channel='4')
select a.member_id as member_id,
case when b.member_id is null then 0 else 1 end Register
from u_analysis_dw.siebel_member a 
left join u_analysis_dw.siebel_member b on a.member_id=b.member_id and b.REG_CHANNEL_CD='3PP-CMB';
--apple
insert overwrite table u_analysis_app.member_Register_channel partition(pdate='${td}',channel='5')
select a.member_id as member_id,
case when b.member_id is null then 0 else 1 end Register
from u_analysis_dw.siebel_member a 
left join u_analysis_dw.siebel_member b on a.member_id=b.member_id and b.REG_CHANNEL_CD='APP-APPLE';
"