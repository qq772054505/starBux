package com.ruisdata.starbucks.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * @author ShenHuiJun
 * @since 2020-01-08
 */
public class T {
    public static void main(String[] args) {
//        String timePartCheck = "([0-1][0-9]|2[0-4]):([0-5][0-9])-([0-1][0-9]|2[0-4]):([0-5][0-9])";
        String s=" 1_3da,efs".replaceAll("[^(0-9_)]","");
//        String s=" fse01364,56".replaceAll("[^(0-9,)]","");
        System.out.println(s);
//        System.out.println(Pattern.matches(timePartCheck,"03:59-20:00"));
//        System.out.println(Pattern.matches(timePartCheck,"13:61-14:61"));
//        System.out.println(isValidDate("2020-02-30"));
//        System.out.println(isValidDate("2020-02-29"));
    }

    public static boolean isValidDate(String str) {
        String datePartCheck = "([0-9][0-9][0-9][0-9])-(0[0-9]|1[0-2])-([0-2][0-9]|3[0-1])";
        boolean dateRegex = Pattern.matches(datePartCheck,str);
        boolean convertSuccess = true;
        // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            // 设置lenient为false.
            // 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            format.setLenient(false);
            format.parse(str);
        } catch (ParseException e) {
            // e.printStackTrace();
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            convertSuccess = false;
        }
        return (convertSuccess && dateRegex);
    }
}
