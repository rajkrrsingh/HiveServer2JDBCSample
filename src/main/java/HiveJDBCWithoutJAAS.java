import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by rasingh on 6/29/16.
 */
public class HiveJDBCWithoutJAAS {
    public static void main (String args[]) {
        try {
            org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hive/rks2.hwxblr.com@HDP.COM", "/tmp/hive.service.keytab");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            System.out.println("getting connection");
            Connection con = DriverManager.getConnection("jdbc:hive2://10.0.1.91:10000/;principal=hive/rks2.hwxblr.com@HDP.COM");
            System.out.println("Connected");
            con.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
