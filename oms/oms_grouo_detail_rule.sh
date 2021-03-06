#!/bin/bash
dt=`date +%Y%m%d`
echo ${dt}
/usr/local/spark-2.2.0-bin-2.6.0-cdh5.8.0/bin/spark-sql --master yarn --deploy-mode client --driver-memory 1g --num-executors 4 --executor-cores 3 --executor-memory 12g  -e"
insert overwrite table u_analysis_test.test_oms_group_detail_rule
as select 
case    when    id=''               then    'null'  else    id                            end     id,
case    when    product_sku=''      then    'null'  else    product_sku                   end     product_sku,
case    when    product_name=''     then    'null'  else    product_name                  end     product_name,
case    when    group_code=''       then    'null'  else    group_code                    end     group_code,
case    when    group_name=''       then    'null'  else    group_name                    end     group_name,
case    when    parent_group_code=''then    'null'  else    parent_group_code             end     parent_group_code,
case    when    type=''             then    'null'  else    type                          end     type,
case    when    must=''             then    'null'  else    must                          end     must,
case    when    min_number=''       then    'null'  else    min_number                    end     min_number,
case    when    max_number=''       then    'null'  else    max_number                    end     max_number,
case    when    rule_number=''      then    'null'  else    rule_number                   end     rule_number,
case    when    default_number=''   then    'null'  else    default_number                end     default_number,
case    when    default_item=''     then    'null'  else    default_item                  end     default_item,s
case    when    marking_tag=''      then    'null'  else    marking_tag                   end     marking_tag,
case    when    repurchase=''       then    'null'  else    repurchase                    end     repurchase,
case    when    sequence=''         then    'null'  else    sequence                      end     sequence,
case    when    remark=''           then    'null'  else    remark                        end     remark,
case    when    create_date=''      then    'null'  else    create_date                   end     create_date,
case    when    update_date=''      then    'null'  else    update_date                   end     update_date,
case    when    hide=''             then    'null'  else    hide                          end     hide,
case    when    channel=''          then    'null'  else    channel                       end     channel,
case    when    style=''            then    'null'  else    style                         end     style
from    u_analysis_    .group_detail_rule  where dptime='${td}';"