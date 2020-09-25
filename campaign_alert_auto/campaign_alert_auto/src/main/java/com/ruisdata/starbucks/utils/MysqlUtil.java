package com.ruisdata.starbucks.utils;

import com.ruisdata.starbucks.Constant;
import com.ruisdata.starbucks.entity.Msg;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * @Author guihao.qing
 * @description
 * @since 2019-12-11
 **/
@Data
public class MysqlUtil {
    /** DEV */
//    private static final String URL = "jdbc:mysql://127.0.0.1:3306/starbucks?useSSL=false&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true";
//    private static final String DRIVER = "com.mysql.jdbc.Driver";
//    private static final String USER = "root";
//    private static final String PASSWORD = "123456";

    /**
     * PROD
     */
//    private static String URL = "jdbc:mysql://172.18.1.230:3306/starbucks?useSSL=false&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true";
//    private static String DRIVER = "com.mysql.jdbc.Driver";
//    private static String USER = "m_starbucks";
//    private static String PASSWORD = "oJnNYzLo7fTeUaTMPVyt";

    private String URL = "";
    private String DRIVER = "";
    private String USER = "";
    private String PASSWORD = "";
    private final String tab = "&nbsp;";
    private final String tb = "<table border=1>";
    private final String tbclose= "</table>";
    private final String tr = "<tr>";
    private final String trclose = "</tr>";
    private final String td = "<td>";
    private final String tdclose = "</td>";
    private final String br = "<br />";

    public MysqlUtil() {
        load();
    }

    private void load() {
        Config config = ConfigFactory.parseFile(new File("mysql-config.conf"));
        this.URL = config.getString("URL");
        this.DRIVER = config.getString("DRIVER");
        this.USER = config.getString("USER");
        this.PASSWORD = config.getString("PASSWORD");
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public String campaignMsg(String type,String status,Connection connection) throws SQLException {
        String sql = "select * FROM campaign_message where create_time>=current_date and status="+status+" and type='"+type+"' order by id";
        if(status==null){
            sql = "select * FROM campaign_message where create_time>=current_date and type='"+type+"' order by id";
        }
        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        StringBuffer sb = new StringBuffer();
        List<String> rsStrList = new ArrayList<String>();
        while (rs.next()) {
            String str = tr + td+rs.getString("campaign_name")+tdclose+
                    td+rs.getString("message")+tdclose+
                    td+ rs.getString("create_time")+tdclose+
                    trclose;
            rsStrList.add(str);
//            sb.append(tab+rs.getString("campaign_name")+":{"+rs.getString("message")+"} time: "+rs.getString("create_time")+"<br />");
            updateStatus(connection,rs.getInt("id"));
        }
        if(rsStrList.size()>0){
            sb.append(Constant.level.get(type)+br);
            sb.append(tb);
            sb.append(tr)
                    .append(td).append("campaign").append(tdclose)
                    .append(td).append("message").append(tdclose)
                    .append(td).append("time").append(tdclose)
                    .append(trclose);
            for(String rsStr:rsStrList){
                sb.append(rsStr);
            }
            sb.append(tbclose);
        }
        pstmt.close();
        rs.close();
        return sb.toString();
    }

    public String normalMsg(String type,String status,Connection connection) throws SQLException {
        String sql = "select * FROM campaign_message where create_time>=current_date and status="+status+" and type='"+type+"' order by id";
        if(status==null){
            sql = "select * FROM campaign_message where create_time>=current_date and type='"+type+"' order by id";
        }
        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        StringBuffer sb = new StringBuffer();
        List<String> rsStrList = new ArrayList<String>();
        while (rs.next()) {
            String str = tr + td+rs.getString("campaign_name")+tdclose+
                    td+rs.getString("message")+tdclose+
                    td+ rs.getString("create_time")+tdclose+
                    trclose;
            rsStrList.add(str);
//            sb.append(tab+rs.getString("campaign_name")+":{"+rs.getString("message")+"} time: "+rs.getString("create_time")+"<br />");
            updateStatus(connection,rs.getInt("id"));
        }
        if(rsStrList.size()>0){
            sb.append(Constant.level.getOrDefault(type,type)+br);
            sb.append(tb);
            sb.append(tr)
                    .append(td).append("title").append(tdclose)
                    .append(td).append("message").append(tdclose)
                    .append(td).append("time").append(tdclose)
                    .append(trclose);
            for(String rsStr:rsStrList){
                sb.append(rsStr);
            }
            sb.append(tbclose);
        }
        pstmt.close();
        rs.close();
        return sb.toString();
    }

    public String successJobs(Connection connection,Boolean changeStatus) throws SQLException {

        String sql = "select * FROM campaign_message where create_time>=current_date and status=0 and type='success' order by id";
        if(!changeStatus){
            sql = "select * FROM campaign_message where create_time>=current_date and type='success' order by id";
        }
        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        StringBuffer sb = new StringBuffer();
        List<String> rsStrList = new ArrayList<String>();
        while (rs.next()) {
            String str =tr + td+rs.getString("campaign_name")+tdclose+
//                    td+":"+rs.getString("message")+tdclose+
                    td + rs.getString("create_time")+tdclose+
                    trclose;
//            sb.append(tab+rs.getString("campaign_name")+":{"+rs.getString("message")+"} time: "+rs.getString("create_time")+"<br />");
            if(changeStatus){
                updateStatus(connection,rs.getInt("id"));
            }
            rsStrList.add(str);
        }
        if(rsStrList.size()>0){
            sb.append(tb);
            sb.append(tr)
                    .append(td).append("campaign").append(tdclose)
                    .append(td).append("finish_time").append(tdclose)
                    .append(trclose);
            for(String rsStr:rsStrList){
                sb.append(rsStr);
            }
            sb.append(tbclose);
        }
        pstmt.close();
        rs.close();
        return sb.toString();
    }

    public String cmpgnFinishCheck(Connection connection,String status) throws SQLException {
        String sql = "select * FROM campaign_message where date(create_time)=current_date and status="+status+" and type='finished'";
        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        String msg = "";
        if (rs.next()){
            msg = tab + tab +"今日所有campaign_evaluation任务已结束,结束时间："+rs.getString("create_time")+",任务如下：<br />";
            Integer id = rs.getInt("id");
            updateStatus(connection,id);
        }
        while (rs.next()) {
            Integer id = rs.getInt("id");
            updateStatus(connection,id);
        }
        pstmt.close();
        rs.close();
        return msg;
    }

    public void updateStatus(Connection conn,Integer id) throws SQLException {
        String sql = "update campaign_message set status=1 where id ="+id;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        int rs = pstmt.executeUpdate();
        if(rs<1){
            pstmt.close();
            throw new SQLException("id"+id+"更新status失败");
        }
        pstmt.close();
    }

    private void close(Connection connection, PreparedStatement pstmt, ResultSet rs)
            throws SQLException {
        try {
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException("Error: Close ResultSet Fail.");
        }
        close(connection, pstmt);
    }

    private void close(Connection connection)
            throws SQLException {
        try {
            if(connection!=null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException("Error: Close Connection Fail.");
        }
    }

    private void close(Connection connection, PreparedStatement pstmt) throws SQLException {
        try {
            if(pstmt !=null){
                pstmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException("Error: Close PreparedStatement Fail.");
        }
        close(connection);
    }

    public static void main(String[] args) throws Exception {
        StringBuffer sb = new StringBuffer();
        System.out.println(sb.toString());

    }



}
