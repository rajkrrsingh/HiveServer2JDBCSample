import org.apache.hadoop.security.UserGroupInformation;

import java.sql.DriverManager;
import java.sql.Connection;

/**
 * Created by rasingh on 6/29/16.
 *
 * the program expect /etc/krb5.conf (sample file is available in resources)
 * */
public class HiveJDBCOverHTTP {
    public static void main (String args[]) {
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hive/rks2.hwxblr.com@HDP.COM", "/tmp/hive.service.keytab");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            System.out.println("getting connection");
            Connection con = DriverManager.getConnection("jdbc:hive2://10.0.1.91:10001/;principal=hive/rks2.hwxblr.com@HDP.COM;transportMode=http;httpPath=cliservice");
            System.out.println("Connected");
            con.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
