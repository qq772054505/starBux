package com.ruisdata.starbucks;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author guihao.qing
 * @description
 * @since 2019-12-12
 **/
public class Constant {

    /**
     * 从头开始
     */
    public static final String Head = "H";
    /**
     * 从当前日期开始
     */
    public static final String Now = "N";
    /**
     * 已经完成
     */
    public static final String Finished = "F";

    public static Map<String,String> level = new HashMap<String,String>(){{
        put("inputwarn","campaign输入信息警告");
        put("outputwarn","excel输出警告");
        put("updatewarn","更新问题警告");
        put("error","错误警告");
        put("etl","etl_daily");
    }};

}
