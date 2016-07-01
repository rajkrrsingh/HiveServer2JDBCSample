import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by rasingh on 6/14/16.
 */
public class HiveJdbcClientSecure {

    final static Logger logger = Logger.getLogger(HiveJdbcClientSecure.class);
    private static Connection con;
    private static Statement stmt;
    private static ResultSet res;

    static{
        System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        // sample jaas.conf can be found in resources replicate this file on the cluster node on same location.
        System.setProperty("java.security.auth.login.config","/tmp/jaas/jaas.conf");
        // enable jgss debugging
        System.setProperty("sun.security.jgss.debug","true");
    }

    public static void main(String[] args) {
        logger.info("starting main");
        if(args.length<3){
            logger.error("Usage java -cp HiveServer2JDBCTest-jar-with-dependencies.jar HiveJdbcClientSecure <connetion_url> <principal> <keytab> ");
            logger.error("--help: java -cp HiveServer2JDBCTest-jar-with-dependencies.jar HiveJdbcClientSecure jdbc:hive2://hb-n2.hwxblr.com:10000/;principal=hive/hb-n2.hwxblr.com@HWXBLR.COM  ambari-qa-hbase234@HWXBLR.COM /etc/security/keytabs/smokeuser.headless.keytab");
            System.exit(0);
        }
        String url = args[0];
        String principal = args[1];
        String keytab = args[2];
        try {
            // getting connection
            con = getConnection(url,principal,keytab);
            stmt = con.createStatement();
            logger.info("Running: Query; ");
            res = stmt.executeQuery("SELECT * FROM hbase_table_1");
            while (res.next()) {
                logger.info("res.next() "+res.getString(1));
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        logger.info("end main");
    }

    public static Connection getConnection(String url,String princ,String keytab){
        try {
            org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            logger.info("starting logging from keytab");
            //UserGroupInformation.loginUserFromKeytab("ambari-qa-hbase234@HWXBLR.COM", "/etc/security/keytabs/smokeuser.headless.keytab");
            UserGroupInformation.loginUserFromKeytab(princ, keytab);
            logger.info("done logging from keytabl");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            logger.info("getting connection");
            con = DriverManager.getConnection(url);
            logger.info("Connected");

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

}