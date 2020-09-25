package com.ruisdata.starbucks.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;
import org.apache.commons.lang3.StringUtils;

/**
 * @Author guihao.qing
 * @description 邮件发送工具
 * @since 2019-12-11
 **/
public class EmailUtil {

    private Config config;
    private String PROTOCOL_SMTP = "";
    private String MAIL_SERVER_HOST = "";
    private String MAIL_SERVER_POST = "";

    private String FROM_EMAIL = "";
    private String USER = "";
    private String PASSWORD = "";

    private String TO_MAILS = "";
    private final String tab = "&nbsp;";

    /**
     * 实例化时自动加载配置文件
     */
    public EmailUtil() {
        load();
    }

    /**
     * 导入配置 - 配置文件和程序包同一位置 email-config.conf
     */
    private void load() {
        this.config = ConfigFactory.parseFile(new File("email-config.conf"));
        this.PROTOCOL_SMTP = config.getString("PROTOCOL_SMTP");
        this.MAIL_SERVER_HOST = config.getString("MAIL_SERVER_HOST");
        this.MAIL_SERVER_POST = config.getString("MAIL_SERVER_POST");

        this.FROM_EMAIL = config.getString("FROM_EMAIL");
        this.USER = config.getString("USER");
        this.PASSWORD = config.getString("PASSWORD");
        this.TO_MAILS = config.getString("TO_MAILS_ALL");
    }

    public void setToMySelf() {
        System.out.println("set mails to my self");
        this.TO_MAILS = config.getString("TO_MAILS_SELF");
    }

    public void setAddress(String address){
        this.TO_MAILS = address;
    }

    /**
     * todo support custom email name
     */
    public void send(
            String mailTitle,
            String mailContent
    ) throws Exception {
        // 可以加载一个配置文件
        Properties props = new Properties();

        // 使用smtp：简单邮件传输协议
        props.put("mail.smtp.host", MAIL_SERVER_HOST);

        // 存储发送邮件服务器的信息
        props.put("mail.smtp.port", MAIL_SERVER_POST);
        props.put("mail.smtp.auth", "true");

        // 根据属性新建一个邮件会话
        Session session = Session.getInstance(props);
//        session.setDebug(true);

        // 由邮件会话新建一个消息对象
        MimeMessage message = new MimeMessage(session);

        // 设置发件人的地址
        message.setFrom(new InternetAddress(FROM_EMAIL));
        String[] toMails = StringUtils.split(TO_MAILS, ",");
        int toMailsSize = toMails.length;
        InternetAddress[] mailAddrs = new InternetAddress[toMailsSize];
        for (int i = 0; i < toMails.length; i++) {
            mailAddrs[i] = new InternetAddress(toMails[i]);
        }

        // 设置收件人,并设置其接收类型为TO
        message.setRecipients(Message.RecipientType.TO, mailAddrs);
        // 设置标题
        message.setSubject(
                MimeUtility.encodeText(mailTitle, MimeUtility.mimeCharset("UTF-8"), null));
        // 设置信件内容
        // 发送HTML邮件，内容样式比较丰富
        message.setContent(mailContent, "text/html;charset=UTF-8");
        // 设置发信时间
        message.setSentDate(new Date());
        // 存储邮件信息
        message.saveChanges();

        // 发送邮件
        Transport transport = session.getTransport(PROTOCOL_SMTP);
        transport.connect(USER, PASSWORD);

        // 发送邮件,其中第二个参数是所有已设好的收件人地址
        transport.sendMessage(message, message.getAllRecipients());
        transport.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("{finished|success|etl|alert}");

            System.exit(-1);
        }
        EmailUtil email = new EmailUtil();
        MysqlUtil mysql = new MysqlUtil();
        if (args.length<2){
            email.messageMaking(args[0], mysql,email,null);
        } else {
            email.messageMaking(args[0], mysql,email,args[1]);
        }
//        System.out.println(content);

        File file = new File("");
        System.out.println(file.getPath());
//        System.out.println(file.getCanonicalPath());
//        System.out.println(file.getAbsolutePath());

    }

public void messageMaking(String checkType, MysqlUtil mysql, EmailUtil email,String emallAddress)
            throws Exception {
        StringBuffer sb = new StringBuffer();
        String title = "campaign-evaluation-alert";
        if (checkType.equals("etl")) {
            title = "etl_daily";
        }
        Connection conn = mysql.getConnection();
        if (checkType.equals("finished")) {
            //9:30检查
            sb.append("info:<br />");
            String finishedCheck = mysql.cmpgnFinishCheck(conn, "0");
            if (!finishedCheck.equals("")) {
                sb.append(finishedCheck);
                sb.append(mysql.successJobs(conn,true));
                sb = msgAdd(mysql, conn, sb, "0", Arrays.asList("error"));
                email.send(title, sb.toString());
            } else {
                String checkEmail = mysql.cmpgnFinishCheck(conn, "1");
                if (!checkEmail.equals("")) {
                    System.out.println("邮件已发送");
                } else {
                    System.out.println("任务未完成");
                    String finishedCampaigns = mysql.successJobs(conn,true);
                    if (finishedCampaigns.equals("")) {
                        sb.append("warning:<br />");
                        sb.append(tab + "campaign-evaluation任务均未完成");
                    } else {
                        sb.append("info:<br />");
                        sb.append(tab + "campaign-evaluation任务未全部结束，已完成任务如下：<br />");
                        sb.append(finishedCampaigns);
                    }
                    email.send(title, sb.toString());
                }
                if (conn != null) {
                    conn.close();
                }
                System.exit(0);
            }
        } else if (checkType.equals("success")) {
            setToMySelf();
            //检查今天任务是否结束，如果未结束，检查已完成的campaign数量
            String finishedCampaigns = mysql.successJobs(conn,false);
            if (finishedCampaigns.equals("")) {
                sb.append("warn:<br />");
                sb.append(tab + "campaign-evaluation任务均未完成");
            } else {
                sb.append("info:<br />");
                sb.append(tab + "campaign-evaluation已完成任务如下：<br />");
                sb.append(finishedCampaigns);
            }
            email.send(title, sb.toString());
        } else if (checkType.equals("etl")) {
            //检查etl_daily完成
            String checkETL = mysql.campaignMsg("etl", null, conn);
            setToMySelf();
            if (checkETL.equals("")) {
                sb.append("warning:<br />");
                sb.append(tab + "etl_daily清洗任务尚未完成");
            } else {
                System.out.println("etl_daily已完成");
                sb.append("info:<br />");
                sb.append(checkETL);
            }
            email.send(title, sb.toString());
        } else if (checkType.equals("alert")) {
            sb = msgAdd(mysql, conn, sb, "0", Arrays.asList("inputwarn","error"));
            if(!sb.toString().equals("")){
                email.send(title, sb.toString());
            }
        } else if (checkType.equals("warning")) {
            setToMySelf();
            sb = msgAdd(mysql, conn, sb, "0", Arrays.asList("outputwarn","updatewarn"));
            if (!sb.toString().equals("")) {
                email.send(title, sb.toString());
            }
        } else {
            title=checkType;
            if (emallAddress !=null){
                setAddress(emallAddress);
            } else {
                setToMySelf();
            }
            sb = normalMsgAdd(mysql, conn, sb, "0", Arrays.asList(checkType));
            if (!sb.toString().equals("")) {
                email.send(title, sb.toString());
            }

        }
    }

    public StringBuffer msgAdd(MysqlUtil mysql, Connection conn, StringBuffer sb, String status,
            List<String> types)
            throws SQLException {
        for (String type : types) {
            String msg = mysql.campaignMsg(type, status, conn);
            if (!msg.equals("")) {
                sb.append(msg);
            }
        }
        return sb;
    }

    public StringBuffer normalMsgAdd(MysqlUtil mysql, Connection conn, StringBuffer sb, String status,
            List<String> types)
            throws SQLException {
        for (String type : types) {
            String msg = mysql.normalMsg(type, status, conn);
            if (!msg.equals("")) {
                sb.append(msg);
            }
        }
        return sb;
    }
}
