package com.ruisdata.starbucks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import com.mysql.jdbc.Statement;
import com.ruisdata.starbucks.utils.EmailUtil;
import com.ruisdata.starbucks.utils.MysqlUtil;
import com.typesafe.config.Config;


public class main {
	    

	public static void main(String[] args) throws Exception {
	    String startDate=args[0];
	    String endDate=args[1];
	    String emailAddress=args[2];
	    String title=args[3];
		try{
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException cne){
            cne.printStackTrace();
        }
        MysqlUtil mysqlUtil = new  MysqlUtil();
        Connection conn = mysqlUtil.getConnection();                
        messageMaking(title,startDate,emailAddress,conn,endDate);
       
	}
	public static List<String> createDateList(String startDate,String endDate) throws Exception {
	    Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    Date date = sdf.parse(startDate);
	    cal.setTime(date);
	    List<String> dateList = new ArrayList<String>();
	    dateList.add(startDate);
	    while (startDate.compareTo(endDate) < 0) {
	        cal.add(Calendar.DATE, 1);
	        startDate = sdf.format(cal.getTime());
	        dateList.add(startDate);
	    }
	    return dateList;
	}
	
	
	public static void messageMaking(String title,String startDate,String emallAddress,Connection conn,String rundate) throws Exception
	{
		StringBuffer sb = new StringBuffer();
		String[] kpiName={"1.MLC company sales",
				          "2.MOP sales",
				          "3.MOD sales",
				          "4.Member sales",
				          "5.Daily active member",
				          "6.MLC ADT",
				          "7.Member ADT",
				          "8.MOP ADT",
				          "9.MOD ADT",
				          "10.MLC AT",
				          "11.Member AT",
				          "12.MOP AT",
				          "13.MOD AT"};
	    List<String> dateList=createDateList(startDate,rundate);
	    String sql="select 1;";
	    java.sql.Statement stmt = conn.createStatement();
        //ResultSet rs = pstmt.executeQuery();
	    ResultSet rst = stmt.executeQuery(sql);
	    int width=dateList.size()*105+150;
		sb.append("<table width=\""+width+"px\"  border=\"1\">\n");

		sb.append(" <tr>\n");
		sb.append("    <th width=\"150px\" ></th>");	
		for(int i=0;i<dateList.size();i++)
		{   
			sb.append("\n    <th width=\"105\"  >");
			sb.append(dateList.get(i)+"</th>");
		}
		
		sb.append("\n  </tr>");
		for(int i=0;i<13;i++)
		{
			sb.append("\n  <tr>\n    <td>"+kpiName[i]+"</td>");
			for(int j=0;j<dateList.size();j++)
			{
				sb.append("   <td>");
				sql="select kpi_rs from mail_kpi where data_time='"+dateList.get(j).toString()+"' and kpi_name='"
				+String.valueOf(i+1)+"';"; 
			    rst = stmt.executeQuery(sql);
				if(rst.next())
				{
					//System.out.println(rst.getDouble(1));
					sb.append(rst.getString(1));
				};				
				sb.append("   </td>\n");
				
			}
			sb.append("\n  </tr>");
		}		
		sb.append("\n");
		sb.append("</table>");
		//System.out.println(sb.toString());
	    EmailUtil email = new EmailUtil();
	    email.setAddress(emallAddress);
	    email.send(title,sb.toString() );
	}
}
	



