import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by rasingh on 6/14/16.
 */
public class HiveJdbcClientSecure {

    final static Logger logger = Logger.getLogger(HiveJdbcClientSecure.class);
    private static Connection con;
    private static Statement stmt;
    private static ResultSet res;

    public static void main(String[] args) {
        logger.info("starting main");
        try {
            con = getConnection();
            stmt = con.createStatement();
            logger.info("Running: show databases; ");
            res = stmt.executeQuery("show databases");
            while (res.next()) {
                System.out.println(res.getString(1));
                logger.info("res.next() "+res.getString(1));
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        logger.info("end main");
    }

    public static Connection getConnection(){
        try {
            org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("ambari-qa-transformers@HDP.LOCAL", "/etc/security/keytabs/smokeuser.headless.keytab");            Class.forName("org.apache.hive.jdbc.HiveDriver");
            System.out.println("getting connection");
            con = DriverManager.getConnection("jdbc:hive2://optimus.hdp.local:10000/;principal=hive/optimus.hdp.local@HDP.LOCAL");
            System.out.println("got connection");

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

}
